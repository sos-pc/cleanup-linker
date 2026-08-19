#!/usr/bin/env python3
"""
cleanup-linker v4
- DB SQLite persistée : inode ↔ paths ↔ torrent_hash (multi-hash par fichier)
- Sync automatique depuis les catégories qBit toutes les 12h
- Webhook Sonarr/Radarr : déplace les torrents liés vers cleanuparr-unlinked

Améliorations v4 :
- Fix faux positifs : UNIQUE(path, torrent_hash) permet plusieurs hash par fichier
- Protection DB : backup rotatif avant chaque sync, table sync_log
- Soft delete : flag stale au lieu de DELETE brutal (rollback possible)
- Garde-fous : refuse le cleanup si sync trop ancienne ou rows en chute
- Fix double-sync : un seul sync_loop même avec plusieurs workers gunicorn
"""

from __future__ import annotations

import hmac
import logging
import os
import shutil
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
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
BACKUP_RETENTION = int(os.getenv("BACKUP_RETENTION", "5"))
BACKUP_DIR = os.getenv("BACKUP_DIR", "/config/backups")

SONARR_URL = os.getenv("SONARR_URL", "")
SONARR_API_KEY = os.getenv("SONARR_API_KEY", "")
RADARR_URL = os.getenv("RADARR_URL", "")
RADARR_API_KEY = os.getenv("RADARR_API_KEY", "")

CROSSSEED_DB = os.getenv("CROSSSEED_DB", "/config/crossseed/cross-seed.db")

# ── Jellyfin ─────────────────────────────────────────────────────────────────
# Optionnel : si JELLYFIN_URL/JELLYFIN_API_KEY sont absents, la route /jellyfin
# répond 503 et l'indexation est simplement sautée à chaque sync.
JELLYFIN_URL = os.getenv("JELLYFIN_URL", "").rstrip("/")
JELLYFIN_API_KEY = os.getenv("JELLYFIN_API_KEY", "")

# Jellyfin voit les fichiers via ses propres montages : le container monte
# /data/Multimedia sur /Multimedia, donc il annonce "/Multimedia/Films/x.mkv"
# là où la DB stocke "/data/Multimedia/Films/x.mkv". Format : "préfixe_jf:préfixe_local"
# séparés par des virgules. Les préfixes les plus longs sont appliqués en premier.
JELLYFIN_PATH_MAP = os.getenv("JELLYFIN_PATH_MAP", "/Multimedia:/data/Multimedia")

# Filet quand l'item n'a jamais été indexé (média ajouté puis supprimé entre
# deux syncs) : on restreint les candidats par métadonnées et on n'agit que sur
# les fichiers réellement absents du disque.
JELLYFIN_FALLBACK_MATCH = os.getenv("JELLYFIN_FALLBACK_MATCH", "true").lower() in (
    "1",
    "true",
    "yes",
)
# Borne le rayon d'action du fallback : au-delà, le motif est jugé trop large
# (titre générique) et on préfère ne rien faire plutôt que déplacer à l'aveugle.
JELLYFIN_FALLBACK_MAX = int(os.getenv("JELLYFIN_FALLBACK_MAX", "10"))

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
_sync_started = threading.Event()  # Empêche les doubles sync_loop (multi-workers)


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
                    path         TEXT    NOT NULL,
                    torrent_hash TEXT,
                    torrent_name TEXT,
                    category     TEXT,
                    stale        INTEGER DEFAULT 0,
                    updated_at   INTEGER DEFAULT (strftime('%s','now')),
                    UNIQUE(path, torrent_hash)
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

                CREATE TABLE IF NOT EXISTS sync_log (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at     INTEGER NOT NULL,
                    finished_at    INTEGER,
                    status         TEXT DEFAULT 'running',
                    rows_before    INTEGER,
                    rows_after     INTEGER,
                    torrents_count INTEGER,
                    duration_s     REAL
                );
            """)
            # Migration : si l'ancienne table files a UNIQUE(path), on la recrée
            # Détecte si la contrainte unique est sur path seul
            table_info = conn.execute("PRAGMA table_info(files)").fetchall()
            index_info = conn.execute("PRAGMA index_list(files)").fetchall()
            needs_migration = False
            for idx in index_info:
                idx_name = idx[1]
                cols = conn.execute(f"PRAGMA index_info({idx_name})").fetchall()
                # Si un index unique ne porte que sur 'path' (colonne 2 = path)
                if idx[2] == 1 and len(cols) == 1:  # unique=1, single column
                    col_name_row = conn.execute(
                        f"PRAGMA index_info({idx_name})"
                    ).fetchone()
                    if col_name_row and col_name_row[2] == "path":
                        needs_migration = True
                        break
            if needs_migration:
                log.info("Migration DB : UNIQUE(path) → UNIQUE(path, torrent_hash)")
                conn.executescript("""
                    ALTER TABLE files RENAME TO files_old;
                    CREATE TABLE files (
                        id           INTEGER PRIMARY KEY AUTOINCREMENT,
                        inode        INTEGER NOT NULL,
                        path         TEXT    NOT NULL,
                        torrent_hash TEXT,
                        torrent_name TEXT,
                        category     TEXT,
                        stale        INTEGER DEFAULT 0,
                        updated_at   INTEGER DEFAULT (strftime('%s','now')),
                        UNIQUE(path, torrent_hash)
                    );
                    INSERT OR IGNORE INTO files (inode, path, torrent_hash, torrent_name, category, updated_at)
                        SELECT inode, path, torrent_hash, torrent_name, category, updated_at FROM files_old;
                    DROP TABLE files_old;
                    CREATE INDEX IF NOT EXISTS idx_inode ON files(inode);
                    CREATE INDEX IF NOT EXISTS idx_path  ON files(path);
                    CREATE INDEX IF NOT EXISTS idx_hash  ON files(torrent_hash);
                    CREATE INDEX IF NOT EXISTS idx_stale ON files(stale);
                """)
                log.info("Migration terminée")
            else:
                # Ajoute la colonne stale si elle n'existe pas encore
                cols = [row[1] for row in conn.execute("PRAGMA table_info(files)").fetchall()]
                if "stale" not in cols:
                    conn.execute("ALTER TABLE files ADD COLUMN stale INTEGER DEFAULT 0")
                    log.info("Colonne 'stale' ajoutée à la table files")
            # Crée l'index stale (après migration si nécessaire)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_stale ON files(stale)")
            # Correspondance item Jellyfin → ligne fichier. Un même chemin porte
            # plusieurs lignes (une par torrent_hash) : elles sont toutes taguées,
            # la lecture se fait en DISTINCT.
            cols = [row[1] for row in conn.execute("PRAGMA table_info(files)").fetchall()]
            if "jellyfin_item_id" not in cols:
                conn.execute("ALTER TABLE files ADD COLUMN jellyfin_item_id TEXT")
                log.info("Colonne 'jellyfin_item_id' ajoutée à la table files")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_jellyfin_item "
                "ON files(jellyfin_item_id) WHERE jellyfin_item_id IS NOT NULL"
            )
            # Index partiel sur les paths Media — accélère le cleanup
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_media_hash "
                "ON files(torrent_hash) WHERE path LIKE '/data/Media/%'"
            )
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
        # qBittorrent < 5.x répond 200 "Ok." ; 5.x répond 204 sans corps et
        # ne fournit que le cookie SID. On accepte les deux formes.
        body_ok = r.text.strip() == "Ok."
        has_sid = any(c.startswith("QBT_SID") for c in s.cookies.keys())
        if not (body_ok or (r.ok and has_sid)):
            raise RuntimeError(
                f"qBit login failed: HTTP {r.status_code} body={r.text.strip()!r}"
            )
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


# ── Backup DB ─────────────────────────────────────────────────────────────────


def backup_db() -> str | None:
    """Crée un backup SQLite avant chaque sync. Retourne le chemin du backup ou None."""
    try:
        os.makedirs(BACKUP_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d_%Hh%M")
        backup_path = os.path.join(BACKUP_DIR, f"db_{timestamp}.sqlite")

        # Utilise l'API SQLite backup (safe même pendant des écritures)
        source = sqlite3.connect(DB_PATH, timeout=10)
        dest = sqlite3.connect(backup_path)
        source.backup(dest)
        dest.close()
        source.close()

        log.info("Backup DB créé : %s", backup_path)

        # Rotation : supprime les anciens backups au-delà de BACKUP_RETENTION
        backups = sorted(Path(BACKUP_DIR).glob("db_*.sqlite"))
        while len(backups) > BACKUP_RETENTION:
            old = backups.pop(0)
            old.unlink()
            log.info("Backup supprimé (rotation) : %s", old.name)

        return backup_path
    except Exception as e:
        log.error("Erreur backup DB : %s", e)
        return None


# ── Sync log ─────────────────────────────────────────────────────────────────


def _log_sync_start(rows_before: int) -> int:
    """Enregistre le début d'une sync. Retourne l'ID du sync_log."""
    with _db_lock:
        with get_db() as conn:
            cursor = conn.execute(
                "INSERT INTO sync_log (started_at, rows_before) VALUES (?, ?)",
                (int(time.time()), rows_before),
            )
            return cursor.lastrowid


def _log_sync_end(sync_id: int, status: str, rows_after: int,
                  torrents_count: int, duration: float) -> None:
    """Met à jour le sync_log avec les résultats."""
    with _db_lock:
        with get_db() as conn:
            conn.execute(
                "UPDATE sync_log SET finished_at=?, status=?, rows_after=?, "
                "torrents_count=?, duration_s=? WHERE id=?",
                (int(time.time()), status, rows_after, torrents_count, duration, sync_id),
            )


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
    Sync complète (atomique avec soft delete) :
    1. Backup la DB
    2. Récupère les catégories qBit → dossiers à scanner
    3. Récupère tous les torrents qBit
    4. Pour chaque torrent → stat() les fichiers → inode
    5. Scanne /data/Media pour trouver les hardlinks côté *arr
    6. Soft delete (stale=1) + insert nouvelles rows + purge stale
    """
    log.info("=== Début sync DB ===")
    start = time.time()

    # Backup avant modification
    backup_db()

    # Compte les rows avant
    with _db_lock:
        with get_db() as conn:
            rows_before = conn.execute("SELECT COUNT(*) FROM files WHERE stale = 0").fetchone()[0]

    sync_id = _log_sync_start(rows_before)

    try:
        with qbit_session() as s:
            categories = qbit_get_categories(s)
            torrents = qbit_get_torrents(s)
    except Exception as e:
        log.error("Erreur connexion qBit: %s", e)
        _log_sync_end(sync_id, "failed", rows_before, 0, time.time() - start)
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

    # ── Étape 4 : soft delete + insert (atomique) ─────────────────────────────
    with _db_lock:
        with get_db() as conn:
            # Marque toutes les rows existantes comme stale
            conn.execute("UPDATE files SET stale = 1")

            # Insère les nouvelles rows (ou met à jour si (path, torrent_hash) existe)
            conn.executemany(
                """INSERT INTO files (inode, path, torrent_hash, torrent_name, category, stale, updated_at)
                   VALUES (?, ?, ?, ?, ?, 0, strftime('%s','now'))
                   ON CONFLICT(path, torrent_hash) DO UPDATE SET
                       inode = excluded.inode,
                       torrent_name = excluded.torrent_name,
                       category = excluded.category,
                       stale = 0,
                       updated_at = strftime('%s','now')
                """,
                rows,
            )

            # Purge les rows stale (plus présentes sur le filesystem)
            stale_count = conn.execute("SELECT COUNT(*) FROM files WHERE stale = 1").fetchone()[0]
            conn.execute("DELETE FROM files WHERE stale = 1")

    rows_after = len(rows)
    elapsed = time.time() - start

    # Vérification d'intégrité : alerte si chute > 30%
    if rows_before > 0 and rows_after < rows_before * 0.7:
        log.warning(
            "⚠️ ALERTE : rows en chute de %d → %d (%.0f%% de perte)",
            rows_before, rows_after, (1 - rows_after / rows_before) * 100
        )

    _log_sync_end(sync_id, "success", rows_after, len(torrents), elapsed)

    # A3 : met à jour les stats pour l'optimiseur SQLite
    with _db_lock:
        with get_db() as conn:
            conn.execute("ANALYZE files")

    log.info(
        "=== Sync terminée : %d entrées (%d stale purgées) en %.1fs ===",
        rows_after, stale_count, elapsed
    )

    # Les tags Jellyfin portent sur des chemins qui viennent d'être reconstruits :
    # on réindexe dans la foulée. Isolé pour qu'un Jellyfin injoignable ne fasse
    # jamais échouer la sync elle-même.
    if jellyfin_enabled():
        try:
            sync_jellyfin_ids()
        except Exception as e:
            log.error("Erreur indexation Jellyfin: %s", e)


def sync_loop() -> None:
    """Lance la sync au démarrage puis toutes les SYNC_INTERVAL secondes.
    Utilise _sync_started pour éviter les doubles sync_loop (multi-workers gunicorn)."""
    if _sync_started.is_set():
        log.info("sync_loop déjà active dans un autre worker, skip")
        return
    _sync_started.set()

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


# ── Jellyfin ─────────────────────────────────────────────────────────────────


def jellyfin_enabled() -> bool:
    return bool(JELLYFIN_URL and JELLYFIN_API_KEY)


def _parse_path_map(raw: str) -> list[tuple[str, str]]:
    """Parse "préfixe_jf:préfixe_local,..." → liste triée du plus long au plus court."""
    pairs: list[tuple[str, str]] = []
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry or ":" not in entry:
            continue
        jf, local = entry.split(":", 1)
        jf, local = jf.strip().rstrip("/"), local.strip().rstrip("/")
        if jf and local:
            pairs.append((jf, local))
    # Le préfixe le plus spécifique gagne (/Multimedia/Films avant /Multimedia)
    pairs.sort(key=lambda p: len(p[0]), reverse=True)
    return pairs


_PATH_MAP = _parse_path_map(JELLYFIN_PATH_MAP)


def map_jellyfin_path(path: str) -> str:
    """Traduit un chemin vu par Jellyfin en chemin tel que stocké dans la DB."""
    if not path:
        return path
    for jf_prefix, local_prefix in _PATH_MAP:
        if path == jf_prefix or path.startswith(jf_prefix + "/"):
            return local_prefix + path[len(jf_prefix):]
    return path


def jellyfin_fetch_items() -> list[dict[str, Any]]:
    """
    Récupère tous les médias Jellyfin avec leur chemin fichier.
    Un seul type d'appel, paginé — aucun scan disque.
    """
    items: list[dict[str, Any]] = []
    start, page_size = 0, 1000
    session = _create_retry_session()
    try:
        while True:
            r = session.get(
                f"{JELLYFIN_URL}/Items",
                params={
                    "Recursive": "true",
                    "IncludeItemTypes": "Movie,Episode,Video",
                    "Fields": "Path",
                    "EnableTotalRecordCount": "false",
                    "StartIndex": start,
                    "Limit": page_size,
                },
                headers={"Authorization": f'MediaBrowser Token="{JELLYFIN_API_KEY}"'},
                timeout=60,
            )
            r.raise_for_status()
            batch = r.json().get("Items", [])
            if not batch:
                break
            items.extend(batch)
            if len(batch) < page_size:
                break
            start += page_size
    finally:
        session.close()
    return items


def sync_jellyfin_ids() -> int:
    """
    Tague les lignes de `files` avec l'id de l'item Jellyfin correspondant.

    C'est la seule façon de relier un event ItemDeleted à un fichier : le payload
    du webhook ne transporte que l'ItemId, et l'item n'existe plus côté Jellyfin
    au moment où l'event arrive. La correspondance doit donc être établie avant.
    """
    if not jellyfin_enabled():
        return 0

    log.info("=== Indexation Jellyfin ===")
    start = time.time()
    try:
        items = jellyfin_fetch_items()
    except Exception as e:
        log.error("Erreur API Jellyfin: %s", e)
        return 0

    updates: list[tuple[str, str]] = []
    sans_path = 0
    for item in items:
        item_id, path = item.get("Id"), item.get("Path")
        if not item_id or not path:
            sans_path += 1
            continue
        updates.append((item_id, map_jellyfin_path(path)))

    if not updates:
        log.warning("Jellyfin : aucun item exploitable (%d items reçus)", len(items))
        return 0

    with _db_lock:
        with get_db() as conn:
            # Repart de zéro : un item réidentifié change d'id, l'ancien tag
            # ne doit pas survivre. Atomique — rollback si l'UPDATE échoue.
            conn.execute(
                "UPDATE files SET jellyfin_item_id = NULL WHERE jellyfin_item_id IS NOT NULL"
            )
            conn.executemany(
                "UPDATE files SET jellyfin_item_id = ? WHERE path = ?", updates
            )
            tagged = conn.execute(
                "SELECT COUNT(DISTINCT path) FROM files WHERE jellyfin_item_id IS NOT NULL"
            ).fetchone()[0]

    log.info(
        "=== Indexation Jellyfin terminée : %d/%d items rattachés à un fichier "
        "indexé en %.1fs (%d sans chemin) ===",
        tagged, len(updates), time.time() - start, sans_path,
    )
    if tagged == 0 and updates:
        log.warning(
            "⚠️ Aucun item Jellyfin rattaché : vérifie JELLYFIN_PATH_MAP (actuel: %s)",
            JELLYFIN_PATH_MAP,
        )
    return tagged


def _jellyfin_paths_for_item(item_id: str) -> list[str]:
    """Chemins DB taggés avec cet item Jellyfin."""
    with _db_lock:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT DISTINCT path FROM files WHERE jellyfin_item_id = ?", (item_id,)
            ).fetchall()
    return [r["path"] for r in rows]


def _jellyfin_paths_by_metadata(data: dict[str, Any]) -> list[str]:
    """
    Filet quand l'item n'a jamais été indexé : on restreint par métadonnées puis
    on ne garde que les chemins réellement absents du disque. Un fichier encore
    présent n'est jamais retenu, ce qui rend un faux positif très improbable.
    """
    item_type = data.get("ItemType", "")
    patterns: list[str] = []

    if item_type == "Episode":
        series = data.get("SeriesName")
        season = data.get("SeasonNumber")
        if series and season is not None:
            patterns.append(f"%/{series}/Season {int(season):02d}/%")
            patterns.append(f"%/{series} (%/Season {int(season):02d}/%")
    elif item_type in ("Movie", "Video"):
        name, year = data.get("Name"), data.get("Year")
        if name and year:
            patterns.append(f"%/{name} ({year})%")
        elif name:
            patterns.append(f"%/{name}%")

    if not patterns:
        return []

    candidates: set[str] = set()
    with _db_lock:
        with get_db() as conn:
            for pat in patterns:
                for row in conn.execute(
                    "SELECT DISTINCT path FROM files WHERE path LIKE ?", (pat,)
                ).fetchall():
                    candidates.add(row["path"])

    disparus = [p for p in candidates if not os.path.exists(p)]
    if candidates:
        log.info(
            "  [fallback] %d candidat(s) par métadonnées, %d réellement absent(s) du disque",
            len(candidates), len(disparus),
        )
    if len(disparus) > JELLYFIN_FALLBACK_MAX:
        log.warning(
            "  [fallback] %d chemins pour un seul item — motif jugé trop large, "
            "abandon (JELLYFIN_FALLBACK_MAX=%d)",
            len(disparus), JELLYFIN_FALLBACK_MAX,
        )
        return []
    return disparus


# ── Nettoyage ────────────────────────────────────────────────────────────────


def move_torrents_for_path(
    file_path: str,
    check_links: bool = True,
    source: str = "webhook",
    dry_run: bool = False,
) -> int:
    """
    1. Cherche le path dans la DB locale → inode → hash original
    2. Cherche dans la DB cross-seed tous les cross-seeds liés
    3. Déplace tout (original + cross-seeds) vers cleanuparr-unlinked

    `source` est tracé dans cleanup_log (webhook / jellyfin / cleanup).
    `dry_run` journalise ce qui serait déplacé sans toucher à qBit ni à la DB.
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

            if dry_run:
                for t in safe_to_move:
                    log.info(
                        "  [DRY RUN] [%s] %s → %s", t.get("category", "?"), t["name"], TARGET_CAT
                    )
                log.info(
                    "  → [DRY RUN] %d torrent(s) seraient déplacés (aucune action)",
                    len(safe_to_move),
                )
                return len(safe_to_move)

            hashes = "|".join(t["info_hash"] for t in safe_to_move)
            s.post(
                f"{QBIT_URL}/api/v2/torrents/setCategory",
                data={"hashes": hashes, "category": TARGET_CAT},
                timeout=10,
            )

            for t in safe_to_move:
                log.info("  ✓ [%s] %s → %s", t.get("category", "?"), t["name"], TARGET_CAT)
                log_move(t["info_hash"], t["name"], t.get("category", ""), source)

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


# ── Webhook Jellyfin ─────────────────────────────────────────────────────────


@app.route("/jellyfin", methods=["POST"])
def jellyfin_webhook():
    """
    Reçoit les events du plugin Webhook de Jellyfin (destination "Generic",
    option "Send All Properties" cochée pour recevoir le JSON brut).

    Le payload ne contient pas le chemin du fichier, seulement un ItemId — d'où
    la résolution via la colonne `files.jellyfin_item_id` remplie à chaque sync.
    """
    token = request.args.get("token") or request.headers.get("X-Token")
    if not verify_token(token):
        return jsonify({"error": "Unauthorized"}), 401

    if not jellyfin_enabled():
        return jsonify({"error": "JELLYFIN_URL / JELLYFIN_API_KEY non configurés"}), 503

    # force=True : le plugin Webhook ne garantit pas l'en-tête Content-Type
    data = request.get_json(silent=True, force=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Invalid or missing JSON body"}), 400

    dry_run = request.args.get("dry_run", "false").lower() in ("1", "true", "yes")
    notif = data.get("NotificationType", "")
    item_type = data.get("ItemType", "")
    item_id = data.get("ItemId", "")
    label = data.get("SeriesName") or data.get("Name") or item_id

    log.info(
        "Reçu event Jellyfin: %s | type=%s | %s%s",
        notif, item_type, label, " [DRY RUN]" if dry_run else "",
    )

    if notif != "ItemDeleted":
        return jsonify({"status": "ignored", "notification": notif})

    # Series/Season sont des dossiers, pas des fichiers : rien à taguer. Jellyfin
    # émet un ItemDeleted par épisode, ce sont eux qui portent l'action.
    if item_type in ("Series", "Season"):
        log.info("  Conteneur %s ignoré (les épisodes sont traités un par un)", item_type)
        return jsonify({"status": "ignored", "reason": f"{item_type} container"})

    if not item_id:
        return jsonify({"error": "Champ 'ItemId' manquant"}), 400

    paths = _jellyfin_paths_for_item(item_id)
    resolution = "index"

    if not paths and JELLYFIN_FALLBACK_MATCH:
        log.info("  Item %s absent de l'index, tentative par métadonnées", item_id)
        paths = _jellyfin_paths_by_metadata(data)
        resolution = "fallback"

    if not paths:
        log.warning(
            "  Item Jellyfin non résolu: %s (%s). Média ajouté depuis la dernière "
            "sync, ou hors périmètre de l'index.",
            label, item_id,
        )
        return jsonify({"status": "unresolved", "item_id": item_id}), 200

    # Garde-fou : ItemDeleted part aussi quand un item quitte simplement la base
    # Jellyfin (refresh, réidentification, retrait d'une bibliothèque) alors que
    # le fichier est toujours là. On n'agit que sur un hardlink réellement supprimé.
    encore_presents = [p for p in paths if os.path.exists(p)]
    if encore_presents:
        log.info(
            "  ⏭ Ignoré : %d/%d chemin(s) encore présent(s) sur le disque "
            "(retrait de bibliothèque ou refresh, pas une suppression)",
            len(encore_presents), len(paths),
        )
        return jsonify(
            {"status": "ignored", "reason": "file still on disk", "paths": encore_presents}
        )

    moved = 0
    for path in paths:
        log.info("  Jellyfin ItemDeleted (%s): %s", resolution, path)
        moved += move_torrents_for_path(path, source="jellyfin", dry_run=dry_run)

    return jsonify(
        {"moved": moved, "paths": paths, "resolution": resolution, "dry_run": dry_run}
    )


@app.route("/jellyfin/index", methods=["POST"])
def jellyfin_reindex():
    """Force une réindexation Jellyfin sans attendre la prochaine sync complète."""
    token = request.args.get("token") or request.headers.get("X-Token")
    if not verify_token(token):
        return jsonify({"error": "Unauthorized"}), 401
    if not jellyfin_enabled():
        return jsonify({"error": "JELLYFIN_URL / JELLYFIN_API_KEY non configurés"}), 503
    threading.Thread(target=sync_jellyfin_ids, daemon=True).start()
    return jsonify({"status": "jellyfin index started"})


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

        # ── Garde-fou : vérifier que la dernière sync est récente et OK ───────
        with _db_lock:
            with get_db() as conn:
                last_sync = conn.execute(
                    "SELECT finished_at, status, rows_before, rows_after "
                    "FROM sync_log WHERE status = 'success' ORDER BY id DESC LIMIT 1"
                ).fetchone()

        if last_sync:
            sync_age = int(time.time()) - (last_sync["finished_at"] or 0)
            max_age = SYNC_INTERVAL * 2  # 2x l'intervalle = trop vieux
            if sync_age > max_age:
                log.warning(
                    "⚠️ Cleanup refusé : dernière sync réussie il y a %dh (max %dh). "
                    "Lancez /sync d'abord.",
                    sync_age // 3600, max_age // 3600
                )
                return

            # Vérifier que les rows n'ont pas chuté anormalement
            if last_sync["rows_before"] and last_sync["rows_after"]:
                ratio = last_sync["rows_after"] / max(last_sync["rows_before"], 1)
                if ratio < 0.5:
                    log.warning(
                        "⚠️ Cleanup refusé : la dernière sync a perdu %.0f%% des rows "
                        "(%d → %d). Vérifiez la DB.",
                        (1 - ratio) * 100,
                        last_sync["rows_before"], last_sync["rows_after"]
                    )
                    return

        # B1 : sync fraîche seulement si > 2 min depuis la dernière
        with _db_lock:
            with get_db() as conn:
                last_ok = conn.execute(
                    "SELECT finished_at FROM sync_log WHERE status='success' ORDER BY id DESC LIMIT 1"
                ).fetchone()
        cooldown = 120  # secondes
        if last_ok and (int(time.time()) - (last_ok["finished_at"] or 0)) < cooldown:
            log.info("Sync récente (<2 min), skip re-sync avant cleanup")
        else:
            sync_db()

        # A1 : requête en 2 étapes pour éviter le NOT EXISTS corrélé (lent sur 124k rows)
        t_query = time.time()
        with _db_lock:
            with get_db() as conn:
                # Étape 1 : hash qui ont AU MOINS un fichier dans /data/Media/ — 1 seule passe
                media_hashes = set(
                    r[0] for r in conn.execute(
                        "SELECT DISTINCT torrent_hash FROM files "
                        "WHERE path LIKE '/data/Media/%' AND stale = 0 AND torrent_hash IS NOT NULL"
                    ).fetchall()
                )
                log.info("Étape 1 (hashes Media) : %d hashes en %.2fs",
                         len(media_hashes), time.time() - t_query)

                # Étape 2 : orphelins = arr_managed ET hash PAS dans media_hashes
                t2 = time.time()
                if media_hashes:
                    placeholders = ",".join("?" * len(media_hashes))
                    torrents = conn.execute(
                        f"""
                        SELECT DISTINCT f.torrent_hash, f.torrent_name, f.category
                        FROM files f
                        INNER JOIN arr_managed am ON am.torrent_hash = f.torrent_hash
                        WHERE f.torrent_hash IS NOT NULL
                          AND f.category != ?
                          AND f.stale = 0
                          AND f.torrent_hash NOT IN ({placeholders})
                        """,
                        (TARGET_CAT, *media_hashes),
                    ).fetchall()
                else:
                    torrents = conn.execute(
                        """
                        SELECT DISTINCT f.torrent_hash, f.torrent_name, f.category
                        FROM files f
                        INNER JOIN arr_managed am ON am.torrent_hash = f.torrent_hash
                        WHERE f.torrent_hash IS NOT NULL
                          AND f.category != ?
                          AND f.stale = 0
                        """,
                        (TARGET_CAT,),
                    ).fetchall()
                log.info("Étape 2 (orphelins) : %d trouvés en %.2fs",
                         len(torrents), time.time() - t2)

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
            total = conn.execute("SELECT COUNT(*) FROM files WHERE stale = 0").fetchone()[0]
            total_paths = conn.execute("SELECT COUNT(DISTINCT path) FROM files WHERE stale = 0").fetchone()[0]
            linked = conn.execute(
                "SELECT COUNT(*) FROM files WHERE torrent_hash IS NOT NULL AND stale = 0"
            ).fetchone()[0]
            inodes = conn.execute("SELECT COUNT(DISTINCT inode) FROM files WHERE stale = 0").fetchone()[0]
            arr_managed_count = conn.execute("SELECT COUNT(*) FROM arr_managed").fetchone()[0]
            log_count = conn.execute("SELECT COUNT(*) FROM cleanup_log").fetchone()[0]
            jellyfin_tagged = conn.execute(
                "SELECT COUNT(DISTINCT path) FROM files WHERE jellyfin_item_id IS NOT NULL"
            ).fetchone()[0]
            last_sync = conn.execute(
                "SELECT finished_at, status, rows_after, duration_s "
                "FROM sync_log WHERE status = 'success' ORDER BY id DESC LIMIT 1"
            ).fetchone()
    last_sync_info = None
    if last_sync:
        last_sync_info = {
            "finished_at": last_sync["finished_at"],
            "status": last_sync["status"],
            "rows": last_sync["rows_after"],
            "duration_s": last_sync["duration_s"],
        }
    return jsonify({
        "total_rows": total,
        "total_paths": total_paths,
        "linked_to_torrent": linked,
        "unique_inodes": inodes,
        "arr_managed_torrents": arr_managed_count,
        "cleanup_log_entries": log_count,
        "jellyfin_enabled": jellyfin_enabled(),
        "jellyfin_indexed_paths": jellyfin_tagged,
        "last_sync": last_sync_info,
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
