#!/usr/bin/env python3
"""
cleanup-linker v3
- DB SQLite persistée : inode ↔ paths ↔ torrent_hash
- Sync automatique depuis les catégories qBit toutes les 12h
- Webhook Sonarr/Radarr : déplace les torrents liés vers cleanuparr-unlinked

Améliorations v3 :
- Sécurité : plus de credentials en dur, comparaison timing-safe des tokens
- Fiabilité : threading.Lock pour SQLite, connexions fermées, sync atomique
- Robustesse : retry réseau, validation payloads, sessions fermées
- Production : gunicorn-ready, type hints, code refactoré
"""

from __future__ import annotations

import hmac
import logging
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Flask, jsonify, request

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────────────


def _require_env(name: str) -> str:
    """Exige qu'une variable d'environnement soit définie (pas de valeur par défaut sensible)."""
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(
            f"Variable d'environnement requise manquante : {name}"
        )
    return value


# Variables obligatoires (sécurité — pas de valeurs par défaut)
QBIT_URL = _require_env("QBIT_URL")
QBIT_USER = _require_env("QBIT_USER")
QBIT_PASS = _require_env("QBIT_PASS")
WEBHOOK_TOKEN = _require_env("WEBHOOK_TOKEN")

# Variables optionnelles avec valeurs par défaut sûres
TARGET_CAT = os.getenv("TARGET_CATEGORY", "cleanuparr-unlinked")
DB_PATH = os.getenv("DB_PATH", "/config/db.sqlite")
SYNC_INTERVAL = int(os.getenv("SYNC_INTERVAL_HOURS", "12")) * 3600

SONARR_URL = os.getenv("SONARR_URL", "")
SONARR_API_KEY = os.getenv("SONARR_API_KEY", "")
RADARR_URL = os.getenv("RADARR_URL", "")
RADARR_API_KEY = os.getenv("RADARR_API_KEY", "")

CROSSSEED_DB = os.getenv("CROSSSEED_DB", "/config/crossseed/cross-seed.db")

# Extensions pertinentes (vidéo uniquement — .iso retiré)
VIDEO_EXTENSIONS: set[str] = {
    ".mkv",
    ".mp4",
    ".avi",
    ".ts",
    ".m2ts",
    ".mov",
    ".wmv",
    ".flv",
}

# ── Verrou global SQLite ─────────────────────────────────────────────────────
_db_lock = threading.Lock()


# ── Base de données ──────────────────────────────────────────────────────────


@contextmanager
def get_db():
    """Context manager qui ouvre ET ferme proprement la connexion SQLite sous verrou."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    with _db_lock:
        with get_db() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS files (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    inode        INTEGER NOT NULL,
                    path         TEXT    NOT NULL UNIQUE,
                    torrent_hash TEXT,
                    torrent_name TEXT,
                    category     TEXT,
                    updated_at   INTEGER DEFAULT (strftime('%s','now'))
                );
                CREATE INDEX IF NOT EXISTS idx_inode ON files(inode);
                CREATE INDEX IF NOT EXISTS idx_path  ON files(path);
                CREATE INDEX IF NOT EXISTS idx_hash  ON files(torrent_hash);

                CREATE TABLE IF NOT EXISTS arr_managed (
                    torrent_hash TEXT PRIMARY KEY,
                    marked_at    INTEGER DEFAULT (strftime('%s','now'))
                );

                CREATE TABLE IF NOT EXISTS cleanup_log (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    torrent_hash  TEXT    NOT NULL,
                    torrent_name  TEXT,
                    original_cat  TEXT,
                    source        TEXT,
                    moved_at      INTEGER DEFAULT (strftime('%s','now'))
                );
                CREATE INDEX IF NOT EXISTS idx_log_hash ON cleanup_log(torrent_hash);
                CREATE INDEX IF NOT EXISTS idx_log_moved ON cleanup_log(moved_at);
            """)
    log.info("DB initialisée")


def log_move(torrent_hash: str, torrent_name: str, original_cat: str, source: str) -> None:
    """Enregistre un move dans le cleanup_log (persisté même après resync)."""
    with _db_lock:
        with get_db() as conn:
            conn.execute(
                "INSERT INTO cleanup_log (torrent_hash, torrent_name, original_cat, source) "
                "VALUES (?, ?, ?, ?)",
                (torrent_hash, torrent_name, original_cat, source),
            )


def mark_arr_managed(download_id: str) -> None:
    """Enregistre un hash torrent comme géré par *arr (idempotent)."""
    if not download_id:
        return
    h = download_id.lower().strip()
    with _db_lock:
        with get_db() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO arr_managed (torrent_hash) VALUES (?)", (h,)
            )


# ── Authentification timing-safe ─────────────────────────────────────────────


def verify_token(token: str | None) -> bool:
    """Comparaison timing-safe du token pour éviter les timing attacks."""
    if token is None:
        return False
    return hmac.compare_digest(token.encode(), WEBHOOK_TOKEN.encode())


# ── HTTP avec retry ──────────────────────────────────────────────────────────


def _create_retry_session(retries: int = 3, backoff: float = 0.5) -> requests.Session:
    """Crée une session requests avec retry automatique sur erreurs transitoires."""
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


# ── qBittorrent ──────────────────────────────────────────────────────────────


@contextmanager
def qbit_session():
    """Context manager qui ouvre et ferme proprement la session qBittorrent."""
    s = _create_retry_session()
    try:
        r = s.post(
            f"{QBIT_URL}/api/v2/auth/login",
            data={"username": QBIT_USER, "password": QBIT_PASS},
            timeout=10,
        )
        if r.text != "Ok.":
            raise RuntimeError(f"qBit login failed: {r.text}")
        yield s
    finally:
        s.close()


def qbit_get_categories(s: requests.Session) -> dict[str, str]:
    """Retourne {category_name: save_path} depuis l'API qBit."""
    r = s.get(f"{QBIT_URL}/api/v2/torrents/categories", timeout=10)
    r.raise_for_status()
    cats = r.json()
    result: dict[str, str] = {}
    for name, info in cats.items():
        path = info.get("savePath", "").rstrip("/")
        if path:
            result[name] = path
    log.info("Catégories qBit trouvées : %s", list(result.keys()))
    return result


def qbit_get_torrents(s: requests.Session) -> list[dict[str, Any]]:
    """Retourne la liste complète des torrents avec hash, save_path, name, category."""
    r = s.get(f"{QBIT_URL}/api/v2/torrents/info", timeout=30)
    r.raise_for_status()
    return r.json()


# ── Scan filesystem (refactoré — une seule fonction réutilisable) ─────────────


def scan_video_files(directory: str) -> dict[int, list[str]]:
    """
    Scanne récursivement un dossier.
    Retourne {inode: [path1, path2, ...]} pour les fichiers vidéo uniquement.
    """
    inode_map: dict[int, list[str]] = {}
    try:
        for root, _dirs, files in os.walk(directory):
            for fname in files:
                ext = os.path.splitext(fname)[1].lower()
                if ext not in VIDEO_EXTENSIONS:
                    continue
                fpath = os.path.join(root, fname)
                try:
                    st = os.stat(fpath)
                    inode_map.setdefault(st.st_ino, []).append(fpath)
                except OSError:
                    pass
    except OSError as e:
        log.warning("Impossible de scanner %s: %s", directory, e)
    return inode_map


def scan_torrent_files(
    root_path: str, hash_: str, name: str, category: str
) -> tuple[dict[int, list[dict[str, str]]], int]:
    """
    Scanne les fichiers vidéo d'un torrent et retourne les mappings inode → torrent info.
    Retourne (inode_to_torrent_updates, files_count).
    """
    updates: dict[int, list[dict[str, str]]] = {}
    count = 0
    torrent_info = {"hash": hash_, "name": name, "category": category}

    try:
        if os.path.isfile(root_path):
            st = os.stat(root_path)
            updates.setdefault(st.st_ino, []).append(torrent_info)
            count = 1
        elif os.path.isdir(root_path):
            for dirpath, _, filenames in os.walk(root_path):
                for fname in filenames:
                    ext = os.path.splitext(fname)[1].lower()
                    if ext not in VIDEO_EXTENSIONS:
                        continue
                    fpath = os.path.join(dirpath, fname)
                    try:
                        st = os.stat(fpath)
                        updates.setdefault(st.st_ino, []).append(torrent_info)
                        count += 1
                    except OSError:
                        pass
    except OSError:
        pass

    return updates, count



# ── Sync principale ──────────────────────────────────────────────────────────


def sync_db() -> None:
    """
    Sync complète (atomique) :
    1. Récupère les catégories qBit → dossiers à scanner
    2. Récupère tous les torrents qBit
    3. Pour chaque torrent → stat() les fichiers → inode
    4. Scanne /data/Media pour trouver les hardlinks côté *arr
    5. Met à jour la DB dans une transaction unique
    """
    log.info("=== Début sync DB ===")
    start = time.time()

    try:
        with qbit_session() as s:
            categories = qbit_get_categories(s)
            torrents = qbit_get_torrents(s)
    except Exception as e:
        log.error("Erreur connexion qBit: %s", e)
        return

    # ── Étape 1 : construire inode → torrent depuis qBit ─────────────────────
    inode_to_torrent: dict[int, list[dict[str, str]]] = {}
    files_scanned = 0

    for t in torrents:
        save_path = t.get("save_path", "").rstrip("/")
        name = t.get("name", "")
        hash_ = t.get("hash", "")
        category = t.get("category", "")
        root_path = f"{save_path}/{name}"

        updates, count = scan_torrent_files(root_path, hash_, name, category)
        for inode, infos in updates.items():
            inode_to_torrent.setdefault(inode, []).extend(infos)
        files_scanned += count

    log.info("qBit : %d torrents, %d fichiers indexés", len(torrents), files_scanned)

    # ── Étape 2 : scanner /data/Media pour les hardlinks *arr ─────────────────
    media_inode_map = scan_video_files("/data/Media")
    log.info("/data/Media : %d inodes trouvés", len(media_inode_map))

    # ── Étape 3 : construire les rows ─────────────────────────────────────────
    rows: list[tuple[int, str, str | None, str | None, str | None]] = []

    def _add_inode_rows(inode: int, paths: list[str]) -> None:
        """Ajoute les rows pour un inode donné."""
        torrents_for_inode = inode_to_torrent.get(inode, [])
        for path in paths:
            if torrents_for_inode:
                for torrent in torrents_for_inode:
                    rows.append((inode, path, torrent["hash"], torrent["name"], torrent["category"]))
            else:
                rows.append((inode, path, None, None, None))

    # Côté Media
    for inode, paths in media_inode_map.items():
        _add_inode_rows(inode, paths)

    # Scan explicite du dossier cross-seeds
    crossseeds_path = "/data/Multimedia/cross-seeds"
    if os.path.isdir(crossseeds_path):
        cs_inode_map = scan_video_files(crossseeds_path)
        for inode, paths in cs_inode_map.items():
            _add_inode_rows(inode, paths)
        log.info("/data/Multimedia/cross-seeds : %d inodes trouvés", len(cs_inode_map))

    # Côté Multimedia — scan les dossiers des catégories qBit
    for cat_name, cat_path in categories.items():
        if not os.path.isdir(cat_path):
            continue
        cat_inode_map = scan_video_files(cat_path)
        for inode, paths in cat_inode_map.items():
            _add_inode_rows(inode, paths)

    # ── Étape 4 : écriture atomique en DB (transaction) ───────────────────────
    with _db_lock:
        with get_db() as conn:
            # Transaction atomique : si le INSERT échoue, le DELETE est annulé
            conn.execute("DELETE FROM files")
            conn.executemany(
                "INSERT OR REPLACE INTO files (inode, path, torrent_hash, torrent_name, category) "
                "VALUES (?, ?, ?, ?, ?)",
                rows,
            )

    elapsed = time.time() - start
    log.info("=== Sync terminée : %d entrées en %.1fs ===", len(rows), elapsed)


def sync_loop() -> None:
    """Lance la sync au démarrage puis toutes les SYNC_INTERVAL secondes."""
    time.sleep(5)
    while True:
        try:
            sync_db()
        except Exception as e:
            log.error("Erreur sync: %s", e)
        log.info("Prochaine sync dans %dh", SYNC_INTERVAL // 3600)
        time.sleep(SYNC_INTERVAL)


# ── Cross-seed DB ────────────────────────────────────────────────────────────


def find_all_torrents_for_hash(original_hash: str) -> list[dict[str, Any]]:
    """
    Cherche dans client_searchee TOUS les torrents qui partagent
    le même nom que le torrent original (original + tous les cross-seeds).
    """
    try:
        conn = sqlite3.connect(CROSSSEED_DB, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            name_row = conn.execute(
                "SELECT name FROM client_searchee WHERE info_hash = ?", (original_hash,)
            ).fetchone()
            if not name_row:
                return []
            torrent_name = name_row[0]
            rows = conn.execute(
                "SELECT DISTINCT info_hash, name, category FROM client_searchee WHERE name = ?",
                (torrent_name,),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()
    except Exception as e:
        log.warning("Erreur lecture DB cross-seed: %s", e)
        return []


def find_crossseeds_for_hash(original_hash: str) -> list[dict[str, Any]]:
    return find_all_torrents_for_hash(original_hash)



# ── Nettoyage ────────────────────────────────────────────────────────────────


def move_torrents_for_path(file_path: str, check_links: bool = True) -> int:
    """
    1. Cherche le path dans la DB locale → inode → hash original
    2. Cherche dans la DB cross-seed tous les cross-seeds liés
    3. Déplace tout (original + cross-seeds) vers cleanuparr-unlinked
    """
    with _db_lock:
        with get_db() as conn:
            row = conn.execute(
                "SELECT inode FROM files WHERE path = ?", (file_path,)
            ).fetchone()

            if row:
                inodes = [row["inode"]]
            else:
                rows = conn.execute(
                    "SELECT DISTINCT inode FROM files WHERE path LIKE ?",
                    (f"{file_path.rstrip('/')}/%",),
                ).fetchall()
                inodes = [r["inode"] for r in rows]

            if not inodes:
                log.warning("Chemin non trouvé en DB: %s", file_path)
                return 0

            placeholders = ",".join("?" * len(inodes))
            originals = conn.execute(
                f"SELECT DISTINCT torrent_hash, torrent_name, category "
                f"FROM files WHERE inode IN ({placeholders}) AND torrent_hash IS NOT NULL",
                inodes,
            ).fetchall()

    if not originals:
        log.warning("Aucun torrent original trouvé pour %s", file_path)
        return 0

    # Collecte tous les hashes à déplacer (originaux + cross-seeds)
    all_torrents: list[dict[str, Any]] = []
    seen_hashes: set[str] = set()
    for orig in originals:
        h = orig["torrent_hash"]
        if h not in seen_hashes:
            seen_hashes.add(h)
            all_torrents.append({
                "info_hash": h,
                "name": orig["torrent_name"],
                "category": orig["category"],
            })
        crossseeds = find_crossseeds_for_hash(h)
        for cs in crossseeds:
            if cs["info_hash"] not in seen_hashes:
                seen_hashes.add(cs["info_hash"])
                all_torrents.append(cs)

    if not all_torrents:
        log.warning("Aucun torrent trouvé pour %s", file_path)
        return 0

    try:
        with qbit_session() as s:
            safe_to_move: list[dict[str, Any]] = []

            for t in all_torrents:
                torrent_info = s.get(
                    f"{QBIT_URL}/api/v2/torrents/info",
                    params={"hashes": t["info_hash"]},
                    timeout=10,
                ).json()
                if not torrent_info:
                    safe_to_move.append(t)
                    continue

                save_path = torrent_info[0].get("save_path", "").rstrip("/")
                name = torrent_info[0].get("name", "")
                root = f"{save_path}/{name}"
                still_linked = False

                if check_links:
                    still_linked = _check_hardlinks_active(root, inodes)

                if still_linked:
                    log.info("  ⏭ [%s] %s ignoré (hardlinks encore actifs)", t.get("category", "?"), t["name"])
                else:
                    safe_to_move.append(t)

            if not safe_to_move:
                log.info("  → Aucun torrent à déplacer (tous ont des hardlinks actifs)")
                return 0

            hashes = "|".join(t["info_hash"] for t in safe_to_move)
            s.post(
                f"{QBIT_URL}/api/v2/torrents/setCategory",
                data={"hashes": hashes, "category": TARGET_CAT},
                timeout=10,
            )

            for t in safe_to_move:
                log.info("  ✓ [%s] %s → %s", t.get("category", "?"), t["name"], TARGET_CAT)
                log_move(t["info_hash"], t["name"], t.get("category", ""), "webhook")

            # Nettoie la DB locale
            with _db_lock:
                with get_db() as conn:
                    placeholders = ",".join("?" * len(inodes))
                    conn.execute(f"DELETE FROM files WHERE inode IN ({placeholders})", inodes)

            log.info("  → %d torrent(s) déplacés vers '%s'", len(safe_to_move), TARGET_CAT)
            return len(safe_to_move)

    except Exception as e:
        log.error("Erreur qBit lors du move: %s", e)
        return 0


def _check_hardlinks_active(root: str, inodes_to_ignore: list[int]) -> bool:
    """Vérifie si un torrent a encore des hardlinks actifs dans /data/Media."""
    try:
        files_to_check: list[str] = []
        if os.path.isfile(root):
            files_to_check.append(root)
        elif os.path.isdir(root):
            for dirpath, _, filenames in os.walk(root):
                for fname in filenames:
                    ext = os.path.splitext(fname)[1].lower()
                    if ext not in VIDEO_EXTENSIONS:
                        continue
                    files_to_check.append(os.path.join(dirpath, fname))

        with _db_lock:
            with get_db() as db_conn:
                for fpath in files_to_check:
                    try:
                        inode = os.stat(fpath).st_ino
                        media_links = db_conn.execute(
                            "SELECT path FROM files WHERE inode = ? AND path LIKE '/data/Media/%'",
                            (inode,),
                        ).fetchall()
                        for ml in media_links:
                            try:
                                if os.stat(ml[0]).st_ino == inode:
                                    log.info("    Hardlink Jellyfin trouvé (confirmé): %s", ml[0])
                                    return True
                            except OSError:
                                pass
                    except OSError:
                        pass
    except OSError:
        pass
    return False



# ── Validation des payloads webhook ──────────────────────────────────────────


def _validate_webhook_payload(data: Any) -> tuple[bool, str]:
    """Valide la structure minimale d'un payload webhook Sonarr/Radarr."""
    if not isinstance(data, dict):
        return False, "Payload doit être un objet JSON"
    if "eventType" not in data:
        return False, "Champ 'eventType' manquant"
    event = data["eventType"]
    if not isinstance(event, str) or not event:
        return False, "'eventType' doit être une chaîne non vide"
    return True, ""


# ── Webhook ──────────────────────────────────────────────────────────────────


@app.route("/webhook", methods=["POST"])
def webhook():
    token = request.args.get("token") or request.headers.get("X-Token")
    if not verify_token(token):
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json(silent=True)
    if data is None:
        return jsonify({"error": "Invalid or missing JSON body"}), 400

    valid, error_msg = _validate_webhook_payload(data)
    if not valid:
        return jsonify({"error": error_msg}), 400

    event: str = data["eventType"]
    is_upgrade: bool = bool(data.get("isUpgrade", False))
    log.info("Reçu event: %s | isUpgrade: %s", event, is_upgrade)

    # Marque toujours le downloadId comme arr_managed dès qu'il est présent
    download_id = data.get("downloadId", "")
    if download_id:
        mark_arr_managed(download_id)

    # Test ping
    if event == "Test":
        log.info("Test webhook reçu ✓")
        return jsonify({"status": "ok"})

    # ── SONARR ───────────────────────────────────────────────────────────────
    if event in ("EpisodeFileDelete", "EpisodeFileDeleteForUpgrade"):
        path = data.get("episodeFile", {}).get("path")
        if path:
            log.info("Sonarr %s: %s", event, path)
            n = move_torrents_for_path(path)
            return jsonify({"moved": n, "path": path})

    elif event == "SeriesDelete":
        path = data.get("series", {}).get("path")
        if path:
            log.info("Sonarr SeriesDelete: %s", path)
            n = move_torrents_for_path(path, check_links=False)
            return jsonify({"moved": n, "path": path})

    # ── RADARR ───────────────────────────────────────────────────────────────
    elif event in ("MovieFileDelete", "MovieFileDeleteForUpgrade"):
        movie = data.get("movie", {})
        mfile = data.get("movieFile", {})
        path = mfile.get("path")
        if not path:
            folder = movie.get("folderPath", "").rstrip("/")
            rel = mfile.get("relativePath", "")
            path = f"{folder}/{rel}" if folder and rel else folder
        if path:
            log.info("Radarr %s: %s", event, path)
            n = move_torrents_for_path(path)
            return jsonify({"moved": n, "path": path})

    elif event == "MovieDelete":
        path = data.get("movie", {}).get("folderPath")
        if path:
            log.info("Radarr MovieDelete: %s", path)
            n = move_torrents_for_path(path, check_links=False)
            return jsonify({"moved": n, "path": path})

    elif event == "Download":
        if not is_upgrade:
            log.info("Import simple ignoré")
            return jsonify({"status": "ignored", "reason": "not an upgrade"})

        log.info("Upgrade détecté (event Download)")
        moved = 0
        deleted_files = data.get("deletedFiles", [])
        for df in deleted_files:
            if isinstance(df, dict):
                path = df.get("path")
                if path:
                    log.info("Fichier remplacé: %s", path)
                    moved += move_torrents_for_path(path)

        return jsonify({"moved": moved, "event": "DownloadUpgrade"})

    return jsonify({"status": "ignored", "event": event})


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/sync", methods=["POST"])
def trigger_sync():
    """Endpoint pour forcer une sync manuelle."""
    token = request.args.get("token") or request.headers.get("X-Token")
    if not verify_token(token):
        return jsonify({"error": "Unauthorized"}), 401
    threading.Thread(target=sync_db, daemon=True).start()
    return jsonify({"status": "sync started"})



@app.route("/cleanup", methods=["POST"])
def cleanup_orphans():
    """Recherche et déplace les torrents orphelins (upgrades ratés passés)."""
    token = request.args.get("token") or request.headers.get("X-Token")
    if not verify_token(token):
        return jsonify({"error": "Unauthorized"}), 401

    dry_run = request.args.get("dry_run", "false").lower() in ("1", "true", "yes")

    def run_cleanup():
        prefix = "[DRY RUN] " if dry_run else ""
        log.info("=== %sDébut du nettoyage des orphelins ===", prefix)
        sync_db()

        with _db_lock:
            with get_db() as conn:
                torrents = conn.execute(
                    """
                    SELECT DISTINCT f.torrent_hash, f.torrent_name, f.category
                    FROM files f
                    INNER JOIN arr_managed am ON am.torrent_hash = f.torrent_hash
                    WHERE f.torrent_hash IS NOT NULL
                      AND f.category != ?
                      AND NOT EXISTS (
                          SELECT 1 FROM files m
                          WHERE m.torrent_hash = f.torrent_hash
                            AND m.path LIKE '/data/Media/%'
                      )
                    """,
                    (TARGET_CAT,),
                ).fetchall()

        if not torrents:
            log.info("%sAucun torrent orphelin trouvé.", prefix)
            return

        for t in torrents:
            log.info("  %sOrphan [%s] %s → %s",
                     "[DRY RUN] " if dry_run else "✓ ",
                     t["category"], t["torrent_name"], TARGET_CAT)

        if dry_run:
            log.info("=== [DRY RUN] %d torrents seraient déplacés ===", len(torrents))
            return

        try:
            with qbit_session() as s:
                hashes = "|".join(t["torrent_hash"] for t in torrents)
                s.post(
                    f"{QBIT_URL}/api/v2/torrents/setCategory",
                    data={"hashes": hashes, "category": TARGET_CAT},
                    timeout=10,
                )
                for t in torrents:
                    log_move(t["torrent_hash"], t["torrent_name"], t["category"], "cleanup")
                log.info("=== Nettoyage terminé : %d torrents déplacés ===", len(torrents))
        except Exception as e:
            log.error("Erreur qBit lors du nettoyage: %s", e)

    threading.Thread(target=run_cleanup, daemon=True).start()
    return jsonify({"status": "cleanup started"})


@app.route("/restore", methods=["POST"])
def restore_last_cleanup():
    """Remet les torrents déplacés par le dernier /cleanup dans leur catégorie d'origine."""
    token = request.args.get("token") or request.headers.get("X-Token")
    if not verify_token(token):
        return jsonify({"error": "Unauthorized"}), 401

    def run_restore():
        log.info("=== Début de la restauration ===")
        try:
            with qbit_session() as s:
                all_torrents = qbit_get_torrents(s)
                in_target = {
                    t["hash"]: t for t in all_torrents if t.get("category") == TARGET_CAT
                }

                if not in_target:
                    log.info("Aucun torrent dans la catégorie cible, rien à restaurer.")
                    return

                # Source 1 : cleanup_log
                with _db_lock:
                    with get_db() as conn:
                        log_rows = conn.execute(
                            "SELECT torrent_hash, torrent_name, original_cat FROM cleanup_log "
                            "WHERE original_cat != ? ORDER BY moved_at DESC",
                            (TARGET_CAT,),
                        ).fetchall()

                hash_to_cat: dict[str, tuple[str, str]] = {}
                for r in log_rows:
                    if r["torrent_hash"] not in hash_to_cat:
                        hash_to_cat[r["torrent_hash"]] = (r["torrent_name"], r["original_cat"])

                # Source 2 : fallback sur files table
                if not hash_to_cat:
                    log.info("cleanup_log vide, fallback sur la table files...")
                    with _db_lock:
                        with get_db() as conn:
                            file_rows = conn.execute(
                                "SELECT DISTINCT torrent_hash, torrent_name, category "
                                "FROM files WHERE torrent_hash IS NOT NULL AND category != ?",
                                (TARGET_CAT,),
                            ).fetchall()
                    for r in file_rows:
                        hash_to_cat[r["torrent_hash"]] = (r["torrent_name"], r["category"])

                # Regroupe par catégorie d'origine
                by_cat: dict[str, list[dict[str, str]]] = {}
                for h, (name, cat) in hash_to_cat.items():
                    if h not in in_target:
                        continue
                    by_cat.setdefault(cat, []).append({"hash": h, "name": name})

                if not by_cat:
                    log.info("Aucun torrent à restaurer.")
                    return

                restored: list[dict[str, str]] = []
                for cat, cat_torrents in by_cat.items():
                    hashes = "|".join(t["hash"] for t in cat_torrents)
                    s.post(
                        f"{QBIT_URL}/api/v2/torrents/setCategory",
                        data={"hashes": hashes, "category": cat},
                        timeout=10,
                    )
                    for t in cat_torrents:
                        log.info("  ↩ Restauré [%s] %s → %s", TARGET_CAT, t["name"], cat)
                        restored.append(t)

                log.info("=== Restauration terminée : %d torrents remis en place ===", len(restored))
        except Exception as e:
            log.error("Erreur lors de la restauration: %s", e)

    threading.Thread(target=run_restore, daemon=True).start()
    return jsonify({"status": "restore started"})



@app.route("/bootstrap", methods=["POST"])
def bootstrap_arr_managed():
    """
    Peuple arr_managed depuis trois sources complémentaires :
    1. Historique *arr (downloadId)
    2. Historique *arr (droppedPath)
    3. DB locale hardlinks actifs
    """
    token = request.args.get("token") or request.headers.get("X-Token")
    if not verify_token(token):
        return jsonify({"error": "Unauthorized"}), 401

    dry_run = request.args.get("dry_run", "false").lower() in ("1", "true", "yes")

    def run_bootstrap():
        prefix = "[DRY RUN] " if dry_run else ""
        log.info("=== %sDébut bootstrap arr_managed ===", prefix)

        marked_hashes: set[str] = set()

        # ── Sources 1 & 2 : historique *arr ──────────────────────────────────
        for label, base_url, api_key in [
            ("Sonarr", SONARR_URL, SONARR_API_KEY),
            ("Radarr", RADARR_URL, RADARR_API_KEY),
        ]:
            if not api_key:
                log.warning("%s: API key manquante, ignoré", label)
                continue
            try:
                page, page_size = 1, 500
                by_dlid = 0
                by_path = 0

                with _db_lock:
                    with get_db() as conn:
                        path_rows = conn.execute(
                            "SELECT path, torrent_hash FROM files WHERE torrent_hash IS NOT NULL"
                        ).fetchall()
                path_to_hash = {r["path"]: r["torrent_hash"] for r in path_rows}

                session = _create_retry_session()
                try:
                    while True:
                        r = session.get(
                            f"{base_url}/api/v3/history",
                            params={"pageSize": page_size, "page": page, "eventType": 3},
                            headers={"X-Api-Key": api_key},
                            timeout=30,
                        )
                        r.raise_for_status()
                        data = r.json()
                        records = data.get("records", [])
                        if not records:
                            break

                        for rec in records:
                            did = rec.get("downloadId", "")
                            if did:
                                h = did.lower().strip()
                                if h not in marked_hashes:
                                    marked_hashes.add(h)
                                    by_dlid += 1

                            dropped = rec.get("data", {}).get("droppedPath", "")
                            if dropped and dropped in path_to_hash:
                                h = path_to_hash[dropped]
                                if h not in marked_hashes:
                                    marked_hashes.add(h)
                                    by_path += 1

                        if len(records) < page_size:
                            break
                        page += 1
                finally:
                    session.close()

                log.info(
                    "%s: %d via downloadId + %d via droppedPath (%s)",
                    label, by_dlid, by_path, "dry run" if dry_run else "marqués"
                )
            except Exception as e:
                log.error("Erreur bootstrap %s: %s", label, e)

        # ── Source 3 : hardlinks actifs dans /data/Media/ ────────────────────
        try:
            with _db_lock:
                with get_db() as conn:
                    rows = conn.execute(
                        "SELECT DISTINCT torrent_hash FROM files "
                        "WHERE torrent_hash IS NOT NULL AND path LIKE '/data/Media/%'"
                    ).fetchall()
            new_from_links = sum(1 for r in rows if r["torrent_hash"] not in marked_hashes)
            for r in rows:
                marked_hashes.add(r["torrent_hash"])
            log.info(
                "DB locale: %d nouveaux via hardlinks /data/Media/ (%s)",
                new_from_links, "dry run" if dry_run else "marqués"
            )
        except Exception as e:
            log.error("Erreur bootstrap DB locale: %s", e)

        # ── Écriture en DB (sauf dry run) ─────────────────────────────────────
        if not dry_run:
            for h in marked_hashes:
                mark_arr_managed(h)

        log.info("=== %sBootstrap terminé : %d hashes uniques ===", prefix, len(marked_hashes))

    threading.Thread(target=run_bootstrap, daemon=True).start()
    return jsonify({"status": "bootstrap started", "dry_run": dry_run})


@app.route("/history")
def history():
    """Historique des derniers moves effectués par cleanup-linker."""
    token = request.args.get("token") or request.headers.get("X-Token")
    if not verify_token(token):
        return jsonify({"error": "Unauthorized"}), 401
    try:
        limit = min(int(request.args.get("limit", 50)), 500)
    except (ValueError, TypeError):
        limit = 50
    with _db_lock:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT id, torrent_hash, torrent_name, original_cat, source, "
                "datetime(moved_at, 'unixepoch') as moved_at "
                "FROM cleanup_log ORDER BY moved_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/stats")
def stats():
    """Statistiques de la DB."""
    token = request.args.get("token") or request.headers.get("X-Token")
    if not verify_token(token):
        return jsonify({"error": "Unauthorized"}), 401
    with _db_lock:
        with get_db() as conn:
            total = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
            linked = conn.execute(
                "SELECT COUNT(*) FROM files WHERE torrent_hash IS NOT NULL"
            ).fetchone()[0]
            inodes = conn.execute("SELECT COUNT(DISTINCT inode) FROM files").fetchone()[0]
            arr_managed_count = conn.execute("SELECT COUNT(*) FROM arr_managed").fetchone()[0]
            log_count = conn.execute("SELECT COUNT(*) FROM cleanup_log").fetchone()[0]
    return jsonify({
        "total_paths": total,
        "linked_to_torrent": linked,
        "unique_inodes": inodes,
        "arr_managed_torrents": arr_managed_count,
        "cleanup_log_entries": log_count,
    })


# ── Main ─────────────────────────────────────────────────────────────────────

def create_app() -> Flask:
    """Factory pour gunicorn : `gunicorn 'app:create_app()'`."""
    init_db()
    threading.Thread(target=sync_loop, daemon=True).start()
    return app


if __name__ == "__main__":
    init_db()
    threading.Thread(target=sync_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=5000)
