#!/usr/bin/env python3
"""
cleanup-linker v2
- DB SQLite persistée : inode ↔ paths ↔ torrent_hash
- Sync automatique depuis les catégories qBit toutes les 12h
- Webhook Sonarr/Radarr : déplace les torrents liés vers cleanuparr-unlinked
"""

import logging
import os
import sqlite3
import stat
import threading
import time

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────────────
QBIT_URL = os.getenv("QBIT_URL", "http://192.168.1.111:8080")
QBIT_USER = os.getenv("QBIT_USER", "admin")
QBIT_PASS = os.getenv("QBIT_PASS", "Marsrpz@qbit666")
TARGET_CAT = os.getenv("TARGET_CATEGORY", "cleanuparr-unlinked")
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN", "cleanup-token-bobynas")
DB_PATH = os.getenv("DB_PATH", "/config/db.sqlite")
SYNC_INTERVAL = int(os.getenv("SYNC_INTERVAL_HOURS", "12")) * 3600

# Extensions pertinentes (vidéo uniquement)
VIDEO_EXTENSIONS = {
    ".mkv",
    ".mp4",
    ".avi",
    ".ts",
    ".m2ts",
    ".mov",
    ".wmv",
    ".flv",
    ".iso",
}


# ── Base de données ──────────────────────────────────────────────────────────


def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
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
        """)
    log.info("DB initialisée")


# ── qBittorrent ──────────────────────────────────────────────────────────────


def qbit_session():
    s = requests.Session()
    r = s.post(
        f"{QBIT_URL}/api/v2/auth/login",
        data={"username": QBIT_USER, "password": QBIT_PASS},
        timeout=10,
    )
    if r.text != "Ok.":
        raise RuntimeError(f"qBit login failed: {r.text}")
    return s


def qbit_get_categories(s):
    """Retourne {category_name: save_path} depuis l'API qBit."""
    r = s.get(f"{QBIT_URL}/api/v2/torrents/categories", timeout=10)
    cats = r.json()
    result = {}
    for name, info in cats.items():
        path = info.get("savePath", "").rstrip("/")
        if path and path != "":
            result[name] = path
    log.info(f"Catégories qBit trouvées : {list(result.keys())}")
    return result


def qbit_get_torrents(s):
    """Retourne la liste complète des torrents avec hash, save_path, name, category."""
    r = s.get(f"{QBIT_URL}/api/v2/torrents/info", timeout=30)
    return r.json()


# ── Scan filesystem ──────────────────────────────────────────────────────────


def scan_directory(directory):
    """
    Scanne récursivement un dossier.
    Retourne {inode: [path1, path2, ...]} pour les fichiers vidéo uniquement.
    """
    inode_map = {}
    try:
        for root, dirs, files in os.walk(directory):
            # Ignore les dossiers cross-seeds
            pass  # on scanne tous les dossiers y compris cross-seeds
            for fname in files:
                ext = os.path.splitext(fname)[1].lower()
                if ext not in VIDEO_EXTENSIONS:
                    continue
                fpath = os.path.join(root, fname)
                try:
                    st = os.stat(fpath)
                    inode = st.st_ino
                    if inode not in inode_map:
                        inode_map[inode] = []
                    inode_map[inode].append(fpath)
                except OSError:
                    pass
    except OSError as e:
        log.warning(f"Impossible de scanner {directory}: {e}")
    return inode_map


# ── Sync principale ──────────────────────────────────────────────────────────


def sync_db():
    """
    Sync complète :
    1. Récupère les catégories qBit → dossiers à scanner
    2. Récupère tous les torrents qBit
    3. Pour chaque torrent → stat() les fichiers → inode
    4. Scanne /data/Media pour trouver les hardlinks côté *arr
    5. Met à jour la DB
    """
    log.info("=== Début sync DB ===")
    start = time.time()

    try:
        s = qbit_session()
        categories = qbit_get_categories(s)
        torrents = qbit_get_torrents(s)
    except Exception as e:
        log.error(f"Erreur connexion qBit: {e}")
        return

    # ── Étape 1 : construire inode → torrent depuis qBit ─────────────────────
    inode_to_torrent = {}  # {inode: [{hash, name, category}, ...]}
    files_scanned = 0

    for t in torrents:
        save_path = t.get("save_path", "").rstrip("/")
        name = t.get("name", "")
        hash_ = t.get("hash", "")
        category = t.get("category", "")
        root_path = f"{save_path}/{name}"

        # Stat le dossier/fichier racine du torrent
        try:
            if os.path.isfile(root_path):
                st = os.stat(root_path)
                inode = st.st_ino
                if inode not in inode_to_torrent:
                    inode_to_torrent[inode] = []
                inode_to_torrent[inode].append(
                    {"hash": hash_, "name": name, "category": category}
                )
                files_scanned += 1
            elif os.path.isdir(root_path):
                for dirpath, _, filenames in os.walk(root_path):
                    for fname in filenames:
                        ext = os.path.splitext(fname)[1].lower()
                        if ext not in VIDEO_EXTENSIONS:
                            continue
                        fpath = os.path.join(dirpath, fname)
                        try:
                            st = os.stat(fpath)
                            inode = st.st_ino
                            if inode not in inode_to_torrent:
                                inode_to_torrent[inode] = []
                            inode_to_torrent[inode].append(
                                {"hash": hash_, "name": name, "category": category}
                            )
                            files_scanned += 1
                        except OSError:
                            pass
        except OSError:
            pass

    log.info(f"qBit : {len(torrents)} torrents, {files_scanned} fichiers indexés")

    # ── Étape 2 : scanner /data/Media pour les hardlinks *arr ─────────────────
    media_inode_map = scan_directory("/data/Media")
    log.info(f"/data/Media : {len(media_inode_map)} inodes trouvés")

    # ── Étape 3 : mettre à jour la DB ─────────────────────────────────────────
    inserted = updated = 0
    with get_db() as conn:
        # Vide la DB et reconstruit (plus simple que diff incrémental)
        conn.execute("DELETE FROM files")

        rows = []
        # Côté qBit (Multimedia)
        for inode, torrent_info in inode_to_torrent.items():
            # Retrouve les paths depuis l'inode via le scan des catégories
            pass  # géré dans la boucle media ci-dessous

        # Côté Media — pour chaque inode trouvé dans /data/Media
        for inode, paths in media_inode_map.items():
            torrents_for_inode = inode_to_torrent.get(inode, [])
            for path in paths:
                if torrents_for_inode:
                    for torrent in torrents_for_inode:
                        rows.append(
                            (
                                inode,
                                path,
                                torrent["hash"],
                                torrent["name"],
                                torrent["category"],
                            )
                        )
                else:
                    rows.append((inode, path, None, None, None))

        # Scan explicite du dossier cross-seeds (catégorie cross-seed-link)
        crossseeds_path = "/data/Multimedia/cross-seeds"
        if os.path.isdir(crossseeds_path):
            cs_inode_map = scan_directory(crossseeds_path)
            for inode, paths in cs_inode_map.items():
                torrents_for_inode = inode_to_torrent.get(inode, [])
                for path in paths:
                    if torrents_for_inode:
                        for torrent in torrents_for_inode:
                            rows.append(
                                (
                                    inode,
                                    path,
                                    torrent["hash"],
                                    torrent["name"],
                                    torrent["category"],
                                )
                            )
                    else:
                        rows.append((inode, path, None, None, None))
            log.info(
                f"/data/Multimedia/cross-seeds : {len(cs_inode_map)} inodes trouvés"
            )

        # Côté Multimedia — scan les dossiers des catégories qBit
        for cat_name, cat_path in categories.items():
            if not os.path.isdir(cat_path):
                continue
            cat_inode_map = scan_directory(cat_path)
            for inode, paths in cat_inode_map.items():
                torrents_for_inode = inode_to_torrent.get(inode, [])
                for path in paths:
                    if torrents_for_inode:
                        for torrent in torrents_for_inode:
                            rows.append(
                                (
                                    inode,
                                    path,
                                    torrent["hash"],
                                    torrent["name"],
                                    torrent["category"],
                                )
                            )
                    else:
                        rows.append((inode, path, None, None, None))

        conn.executemany(
            """
            INSERT OR REPLACE INTO files (inode, path, torrent_hash, torrent_name, category)
            VALUES (?, ?, ?, ?, ?)
        """,
            rows,
        )
        inserted = len(rows)

    elapsed = time.time() - start
    log.info(f"=== Sync terminée : {inserted} entrées en {elapsed:.1f}s ===")


def sync_loop():
    """Lance la sync au démarrage puis toutes les SYNC_INTERVAL secondes."""
    # Petite pause pour laisser Flask démarrer
    time.sleep(5)
    while True:
        try:
            sync_db()
        except Exception as e:
            log.error(f"Erreur sync: {e}")
        log.info(f"Prochaine sync dans {SYNC_INTERVAL // 3600}h")
        time.sleep(SYNC_INTERVAL)


# ── Nettoyage ────────────────────────────────────────────────────────────────

CROSSSEED_DB = os.getenv("CROSSSEED_DB", "/config/crossseed/cross-seed.db")


def get_crossseed_db():
    conn = sqlite3.connect(CROSSSEED_DB, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def find_all_torrents_for_hash(original_hash: str) -> list:
    """
    Cherche dans client_searchee TOUS les torrents qui partagent
    le même nom que le torrent original (original + tous les cross-seeds).
    """
    try:
        cs = get_crossseed_db()
        # Trouve le nom du torrent original
        name_row = cs.execute(
            "SELECT name FROM client_searchee WHERE info_hash = ?", (original_hash,)
        ).fetchone()
        if not name_row:
            return []
        torrent_name = name_row[0]
        # Trouve tous les torrents avec ce nom
        rows = cs.execute(
            """
            SELECT DISTINCT info_hash, name, category
            FROM client_searchee
            WHERE name = ?
        """,
            (torrent_name,),
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        log.warning(f"Erreur lecture DB cross-seed: {e}")
        return []


def find_crossseeds_for_hash(original_hash: str) -> list:
    return find_all_torrents_for_hash(original_hash)


def move_torrents_for_path(file_path: str, check_links: bool = True):
    """
    1. Cherche le path dans la DB locale → inode → hash original
    2. Cherche dans la DB cross-seed tous les cross-seeds liés
    3. Déplace tout (original + cross-seeds) vers cleanuparr-unlinked
    """
    with get_db() as conn:
        # Cherche l'inode exact du fichier supprimé
        row = conn.execute(
            "SELECT inode FROM files WHERE path = ?", (file_path,)
        ).fetchone()

        if row:
            # Fichier unique → on cherche les torrents de cet inode uniquement
            inodes = [row["inode"]]
        else:
            # Recherche par préfixe (dossier) → on récupère TOUS les inodes sous ce dossier
            rows = conn.execute(
                "SELECT DISTINCT inode FROM files WHERE path LIKE ?",
                (f"{file_path.rstrip('/')}/%",),
            ).fetchall()
            inodes = [r["inode"] for r in rows]

        if not inodes:
            log.warning(f"Chemin non trouvé en DB: {file_path}")
            return 0

        # Trouve tous les torrents liés à tous les inodes trouvés
        placeholders = ",".join("?" * len(inodes))
        originals = conn.execute(
            f"""
            SELECT DISTINCT torrent_hash, torrent_name, category
            FROM files
            WHERE inode IN ({placeholders}) AND torrent_hash IS NOT NULL
        """,
            inodes,
        ).fetchall()

    inode = inodes[0]  # gardé pour la suppression DB en fin de fonction

    if not originals:
        log.warning(f"Aucun torrent original trouvé pour {file_path}")
        return 0

    # Collecte tous les hashes à déplacer (originaux + cross-seeds)
    all_torrents = []
    for orig in originals:
        all_torrents.append(
            {
                "info_hash": orig["torrent_hash"],
                "name": orig["torrent_name"],
                "category": orig["category"],
            }
        )
        # Cherche les cross-seeds liés via la DB cross-seed
        crossseeds = find_crossseeds_for_hash(orig["torrent_hash"])
        for cs in crossseeds:
            if cs["info_hash"] not in [t["info_hash"] for t in all_torrents]:
                all_torrents.append(cs)

    if not all_torrents:
        log.warning(f"Aucun torrent trouvé pour {file_path}")
        return 0

    try:
        s = qbit_session()

        # Vérifie pour chaque torrent si ses fichiers ont encore des hardlinks actifs
        # (nlink > 1 = un autre dossier pointe encore dessus, ex: autre saison dans /data/Media)
        safe_to_move = []
        for t in all_torrents:
            torrent_info = s.get(
                f"{QBIT_URL}/api/v2/torrents/info?hashes={t['info_hash']}"
            ).json()
            if not torrent_info:
                safe_to_move.append(t)
                continue
            save_path = torrent_info[0].get("save_path", "").rstrip("/")
            name = torrent_info[0].get("name", "")
            root = f"{save_path}/{name}"
            still_linked = False
            try:
                files_to_check = []
                if os.path.isfile(root):
                    files_to_check.append(root)
                elif os.path.isdir(root):
                    for dirpath, _, filenames in os.walk(root):
                        for fname in filenames:
                            ext = os.path.splitext(fname)[1].lower()
                            if ext not in VIDEO_EXTENSIONS:
                                continue
                            files_to_check.append(os.path.join(dirpath, fname))

                # Vérifie si l'un des fichiers a encore un hardlink vers /data/Media
                # (= encore importé dans la bibliothèque Jellyfin)
                # Skippé pour SeriesDelete/MovieDelete (check_links=False)
                if check_links:
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
                                            still_linked = True
                                            log.info(
                                                f"    Hardlink Jellyfin trouvé (confirmé): {ml[0]}"
                                            )
                                            break
                                        else:
                                            log.debug(
                                                f"    Hardlink remplacé ignoré (inode diff): {ml[0]}"
                                            )
                                    except OSError:
                                        log.debug(
                                            f"    Hardlink obsolète ignoré: {ml[0]}"
                                        )
                                if still_linked:
                                    break
                            except OSError:
                                pass
            except OSError:
                pass
            if still_linked:
                log.info(
                    f"  ⏭ [{t['category']}] {t['name']} ignoré (hardlinks encore actifs)"
                )
            else:
                safe_to_move.append(t)

        if not safe_to_move:
            log.info(f"  → Aucun torrent à déplacer (tous ont des hardlinks actifs)")
            return 0

        hashes = "|".join(t["info_hash"] for t in safe_to_move)
        s.post(
            f"{QBIT_URL}/api/v2/torrents/setCategory",
            data={"hashes": hashes, "category": TARGET_CAT},
        )

        for t in safe_to_move:
            log.info(f"  ✓ [{t['category']}] {t['name']} → {TARGET_CAT}")

        # Nettoie la DB locale
        with get_db() as conn:
            placeholders = ",".join("?" * len(inodes))
            conn.execute(f"DELETE FROM files WHERE inode IN ({placeholders})", inodes)

        log.info(f"  → {len(safe_to_move)} torrent(s) déplacés vers '{TARGET_CAT}'")
        return len(safe_to_move)

    except Exception as e:
        log.error(f"Erreur qBit lors du move: {e}")
        return 0


# ── Webhook ──────────────────────────────────────────────────────────────────


@app.route("/webhook", methods=["POST"])
def webhook():
    token = request.args.get("token") or request.headers.get("X-Token")
    if token != WEBHOOK_TOKEN:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    if not data:
        return jsonify({"error": "No JSON"}), 400

    event = data.get("eventType", "")
    is_upgrade = data.get("isUpgrade", False)
    log.info(f"Reçu event: {event} | isUpgrade: {is_upgrade}")

    # Test ping
    if event == "Test":
        log.info("Test webhook reçu ✓")
        return jsonify({"status": "ok"})

    # ── SONARR ───────────────────────────────────────────────────────────────
    if event in ("EpisodeFileDelete", "EpisodeFileDeleteForUpgrade"):
        path = data.get("episodeFile", {}).get("path")
        if path:
            log.info(f"Sonarr {event}: {path}")
            n = move_torrents_for_path(path)
            return jsonify({"moved": n, "path": path})

    elif event == "SeriesDelete":
        path = data.get("series", {}).get("path")
        if path:
            log.info(f"Sonarr SeriesDelete: {path}")
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
            log.info(f"Radarr {event}: {path}")
            n = move_torrents_for_path(path)
            return jsonify({"moved": n, "path": path})

    elif event == "MovieDelete":
        path = data.get("movie", {}).get("folderPath")
        if path:
            log.info(f"Radarr MovieDelete: {path}")
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
            path = df.get("path")
            if path:
                log.info(f"Fichier remplacé: {path}")
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
    if token != WEBHOOK_TOKEN:
        return jsonify({"error": "Unauthorized"}), 401
    threading.Thread(target=sync_db, daemon=True).start()
    return jsonify({"status": "sync started"})


@app.route("/stats")
def stats():
    """Statistiques de la DB."""
    token = request.args.get("token") or request.headers.get("X-Token")
    if token != WEBHOOK_TOKEN:
        return jsonify({"error": "Unauthorized"}), 401
    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
        linked = conn.execute(
            "SELECT COUNT(*) FROM files WHERE torrent_hash IS NOT NULL"
        ).fetchone()[0]
        inodes = conn.execute("SELECT COUNT(DISTINCT inode) FROM files").fetchone()[0]
    return jsonify(
        {"total_paths": total, "linked_to_torrent": linked, "unique_inodes": inodes}
    )


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    threading.Thread(target=sync_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=5000)
