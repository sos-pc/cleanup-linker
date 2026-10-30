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

import hashlib
import hmac
import json
import logging
import os
import re
import shutil
import sqlite3
import threading
import time
import unicodedata
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

# Délai de grâce du /cleanup. Un torrent tout juste terminé est déjà marqué
# arr_managed par le webhook de grab, mais n'a pas encore de hardlink tant que
# l'*arr ne l'a pas importé : il ressemble donc à un orphelin sans en être un.
# Passé ce délai, un torrent toujours sans hardlink l'est réellement.
# 0 désactive la protection.
CLEANUP_GRACE_HOURS = int(os.getenv("CLEANUP_GRACE_HOURS", "24"))
BACKUP_DIR = os.getenv("BACKUP_DIR", "/config/backups")

SONARR_URL = os.getenv("SONARR_URL", "")
SONARR_API_KEY = os.getenv("SONARR_API_KEY", "")
RADARR_URL = os.getenv("RADARR_URL", "")
RADARR_API_KEY = os.getenv("RADARR_API_KEY", "")

# Assistant d'import : traite les téléchargements bloqués des files Sonarr/Radarr,
# dès l'event « Manual Interaction Required ». Chaque règle a sa propre
# simulation, active par défaut : elle journalise ce qu'elle ferait, sans rien
# changer. Celle-ci est la simulation de la règle 1 (débloquer la file). Le
# passage régulier n'est qu'un renfort : l'*arr n'émet l'event qu'une fois par
# téléchargement, et l'oublie à son redémarrage. 0 minute le désactive (/assist
# reste utilisable). Seuls 0/false/no font agir : une valeur mal saisie garde la
# simulation.
IMPORT_ASSIST_DRY_RUN = os.getenv("IMPORT_ASSIST_DRY_RUN", "true").strip().lower() not in ("0", "false", "no")
IMPORT_ASSIST_INTERVAL = int(os.getenv("IMPORT_ASSIST_INTERVAL_MINUTES", "360")) * 60

# Règle 4 de l'assistant (pack incomplet) : un pack de saison partiel est
# remplacé par un pack complet, cherché dans Prowlarr. Sa propre simulation,
# dans les mêmes conditions. Le filet après import remplace la recherche
# d'épisodes de la règle 1 : il n'agit que si la règle 1 agit aussi.
# Sans Prowlarr, la règle se contente de signaler les packs partiels.
IMPORT_ASSIST_PACK_DRY_RUN = os.getenv("IMPORT_ASSIST_PACK_DRY_RUN", "true").strip().lower() not in ("0", "false", "no")
PROWLARR_URL = os.getenv("PROWLARR_URL", "").rstrip("/")
PROWLARR_API_KEY = os.getenv("PROWLARR_API_KEY", "")

# Règle 5 de l'assistant (bonne saison) : accorde chaque saison demandée dans
# Seerr (numérotée comme TMDB) avec la saison TVDB que Sonarr surveille.
# Simulation propre, dans les mêmes conditions. Sans Seerr, la règle ne fait rien.
IMPORT_ASSIST_ACCORD_DRY_RUN = os.getenv("IMPORT_ASSIST_ACCORD_DRY_RUN", "true").strip().lower() not in ("0", "false", "no")
SEERR_URL = os.getenv("SEERR_URL", "").rstrip("/")
SEERR_API_KEY = os.getenv("SEERR_API_KEY", "")

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
# Une série entière compte légitimement beaucoup de fichiers : plafond distinct
# pour les suppressions de conteneur (Series/Season).
JELLYFIN_FALLBACK_MAX_CONTAINER = int(os.getenv("JELLYFIN_FALLBACK_MAX_CONTAINER", "500"))

# Traduit une suppression Jellyfin en action Sonarr/Radarr (dé-monitorer puis
# supprimer l'enregistrement du fichier) au lieu de déplacer les torrents nous-mêmes.
# L'*arr émet alors son propre webhook, qui repasse par le chemin de code existant.
# Sans ça, l'*arr croit toujours détenir le fichier et le retélécharge.
# Opt-in : déclenche des actions destructrices côté *arr.
JELLYFIN_DELEGATE_TO_ARR = os.getenv("JELLYFIN_DELEGATE_TO_ARR", "false").lower() in (
    "1",
    "true",
    "yes",
)

# Supprimer une série entière ou un film depuis Jellyfin retire aussi l'entrée
# de Sonarr/Radarr, au lieu de seulement la dé-monitorer. Sans ça la fiche reste
# indéfiniment dans la liste, à 0 fichier, en affichant ses épisodes manquants.
# Ne concerne QUE les conteneurs Series et Movie : supprimer une saison ou un
# épisode d'une série qu'on garde ne doit évidemment pas retirer la série.
# Désactivé par défaut — c'est plus fort qu'un dé-monitorage, et irréversible :
# on perd l'historique, le profil de qualité, les tags et le monitoring.
JELLYFIN_ARR_DELETE_CONTAINERS = os.getenv(
    "JELLYFIN_ARR_DELETE_CONTAINERS", "false"
).lower() in ("1", "true", "yes")

# Extensions indexées. Un fichier dont l'extension manque ici est invisible pour
# tout le service : ni webhook ni /cleanup ne pourront déplacer son torrent.
# Configurable pour ne plus dépendre d'un rebuild — la casse et le point initial
# sont normalisés, "mkv" et ".MKV" sont acceptés.
_DEFAULT_VIDEO_EXTENSIONS = ".mkv,.mp4,.avi,.ts,.m2ts,.mov,.wmv,.flv,.iso,.m4v"


def _parse_extensions(raw: str) -> set[str]:
    exts: set[str] = set()
    for entry in raw.split(","):
        entry = entry.strip().lower()
        if not entry:
            continue
        exts.add(entry if entry.startswith(".") else f".{entry}")
    return exts


VIDEO_EXTENSIONS: set[str] = _parse_extensions(
    os.getenv("VIDEO_EXTENSIONS", _DEFAULT_VIDEO_EXTENSIONS)
)

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

                CREATE TABLE IF NOT EXISTS assist_log (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    download_id TEXT    NOT NULL,
                    arr         TEXT,
                    title       TEXT,
                    decision    TEXT,
                    details     TEXT,
                    dry_run     INTEGER,
                    at          INTEGER DEFAULT (strftime('%s','now')),
                    rule        INTEGER
                );
                CREATE INDEX IF NOT EXISTS idx_assist_dl ON assist_log(download_id);

                -- Remplacements réels d'un pack partiel par un pack complet (règle 4).
                CREATE TABLE IF NOT EXISTS pack_replace (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    series_id  INTEGER NOT NULL,
                    season     INTEGER NOT NULL,
                    old_hash   TEXT    NOT NULL,
                    new_hash   TEXT    NOT NULL,
                    new_title  TEXT,
                    stage      TEXT,
                    episodes   TEXT,
                    at         INTEGER DEFAULT (strftime('%s','now'))
                );
                CREATE INDEX IF NOT EXISTS idx_pack_new ON pack_replace(new_hash);
                CREATE INDEX IF NOT EXISTS idx_pack_old ON pack_replace(old_hash);
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
            for col in ("jellyfin_item_id", "jellyfin_series_id", "jellyfin_season_id"):
                if col not in cols:
                    conn.execute(f"ALTER TABLE files ADD COLUMN {col} TEXT")
                    log.info("Colonne '%s' ajoutée à la table files", col)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_jellyfin_item "
                "ON files(jellyfin_item_id) WHERE jellyfin_item_id IS NOT NULL"
            )
            # Une suppression de série/saison dans Jellyfin n'émet qu'un seul event,
            # sur le conteneur — jamais un par épisode. Ces index permettent de
            # remonter du conteneur à ses fichiers.
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_jellyfin_series "
                "ON files(jellyfin_series_id) WHERE jellyfin_series_id IS NOT NULL"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_jellyfin_season "
                "ON files(jellyfin_season_id) WHERE jellyfin_season_id IS NOT NULL"
            )
            # Index partiel sur les paths Media — accélère le cleanup
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_media_hash "
                "ON files(torrent_hash) WHERE path LIKE '/data/Media/%'"
            )
            # Règle de l'assistant qui a rendu chaque verdict. Pour les verdicts
            # d'avant, elle se déduit de la décision.
            cols = [row[1] for row in conn.execute("PRAGMA table_info(assist_log)").fetchall()]
            if "rule" not in cols:
                conn.execute("ALTER TABLE assist_log ADD COLUMN rule INTEGER")
                conn.execute(
                    "UPDATE assist_log SET rule = CASE WHEN decision LIKE 'accord-%' THEN 5 "
                    "WHEN decision LIKE 'pack-%' THEN 4 ELSE 1 END"
                )
                log.info("Colonne 'rule' ajoutée à la table assist_log")
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


def refresh_arr_managed_from_links() -> int:
    """
    Marque arr_managed tout torrent actuellement hardlinké dans /data/Media.

    Filet contre les webhooks ratés — service arrêté pendant un import, type
    d'event non coché dans un *arr, livraison échouée. Un hardlink dans une
    racine *arr est la preuve de l'import, indépendamment de tout event reçu.

    Le critère ne porte que sur /data/Media : un torrent vivant uniquement sous
    /data/Multimedia (ajout manuel) n'est jamais marqué, donc jamais candidat au
    cleanup. C'est ce qui distingue le contenu géré des ajouts manuels.
    """
    with _db_lock:
        with get_db() as conn:
            cur = conn.execute(
                "INSERT OR IGNORE INTO arr_managed (torrent_hash) "
                "SELECT DISTINCT torrent_hash FROM files "
                "WHERE torrent_hash IS NOT NULL AND path LIKE '/data/Media/%' AND stale = 0"
            )
            return cur.rowcount


def purge_arr_managed_absents(qbit_hashes: set[str]) -> int:
    """
    Retire d'arr_managed les hashes dont le torrent n'est plus dans qBittorrent.

    Le critère est la liste qBit, pas la table `files` : un torrent bien présent
    dans le client mais dont les fichiers ont disparu du disque n'a aucune ligne
    dans `files` et serait purgé à tort (209 cas sur l'installation de référence).

    Sans ça, une release re-téléchargée plus tard reprend le même hash et serait
    considérée gérée par un *arr d'emblée, même ajoutée à la main.
    """
    if not qbit_hashes:
        log.warning("arr_managed : purge annulée, liste qBit vide")
        return 0

    with _db_lock:
        with get_db() as conn:
            total = conn.execute("SELECT COUNT(*) FROM arr_managed").fetchone()[0]
            if not total:
                return 0
            connus = set(
                r[0] for r in conn.execute("SELECT torrent_hash FROM arr_managed")
            )
            absents = [h for h in connus if h.lower() not in qbit_hashes]

            # Même garde-fou que ses voisins : une liste qBit tronquée ferait
            # disparaître un marquage encore valide.
            if len(absents) > total * 0.3:
                log.warning(
                    "⚠️ arr_managed : purge de %d/%d lignes (%.0f%%) refusée — "
                    "liste qBittorrent suspecte",
                    len(absents), total, 100 * len(absents) / total,
                )
                return 0

            if absents:
                ph = ",".join("?" * len(absents))
                conn.execute(f"DELETE FROM arr_managed WHERE torrent_hash IN ({ph})", absents)
            return len(absents)


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


def torrent_root(t: dict[str, Any]) -> str:
    """
    Emplacement réel des données d'un torrent.

    `content_path` fait foi : le champ `name` est le nom d'affichage du torrent,
    qui diffère du dossier sur le disque dès que le .torrent porte un titre
    différent de son contenu. Sur l'installation de référence, 308 torrents sur
    3269 (9,4 %) sont dans ce cas — reconstruire le chemin avec save_path + name
    les rendait introuvables, donc non indexés et faussement orphelins.
    """
    cp = (t.get("content_path") or "").rstrip("/")
    if cp:
        return cp
    return f"{t.get('save_path', '').rstrip('/')}/{t.get('name', '')}"


def qbit_get_torrents(s: requests.Session) -> list[dict[str, Any]]:
    """Retourne la liste complète des torrents avec hash, save_path, name, category."""
    r = s.get(f"{QBIT_URL}/api/v2/torrents/info", timeout=30)
    r.raise_for_status()
    return r.json()


def _shared_content(
    hashes: set[str], torrents: list[dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    """
    Torrents du lot `hashes` dont les données sont aussi celles d'un torrent hors du
    lot, associés à ce torrent.

    Une même release prise sur deux trackers atterrit au même endroit : le second
    torrent réutilise les fichiers du premier. Changer la catégorie de l'un fait
    déplacer ces fichiers par ATM — la corbeille a son propre dossier — et l'autre
    reste en seed sur du vide. C'est arrivé sur un upgrade Radarr vers la même
    release : l'ancien torrent est parti en corbeille avec le fichier du nouveau.
    Le jumeau peut dater d'après la dernière sync, d'où la lecture de qBit plutôt
    que de l'index.
    """
    lot = {h.lower() for h in hashes}
    par_racine: dict[str, list[dict[str, Any]]] = {}
    for t in torrents:
        par_racine.setdefault(torrent_root(t), []).append(t)

    partages: dict[str, dict[str, Any]] = {}
    for t in torrents:
        h = t.get("hash", "").lower()
        if h not in lot:
            continue
        autre = next(
            (o for o in par_racine[torrent_root(t)] if o.get("hash", "").lower() not in lot),
            None,
        )
        if autre is not None:
            partages[h] = autre
    return partages


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

# Partagée avec index_import : un fichier indexé à l'import doit porter exactement
# la même ligne que celle que la sync suivante écrira.
_UPSERT_FILES = """
    INSERT INTO files (inode, path, torrent_hash, torrent_name, category, stale, updated_at)
    VALUES (?, ?, ?, ?, ?, 0, strftime('%s','now'))
    ON CONFLICT(path, torrent_hash) DO UPDATE SET
        inode = excluded.inode,
        torrent_name = excluded.torrent_name,
        category = excluded.category,
        stale = 0,
        updated_at = strftime('%s','now')
"""


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
        name = t.get("name", "")
        hash_ = t.get("hash", "")
        category = t.get("category", "")
        root_path = torrent_root(t)

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
            conn.executemany(_UPSERT_FILES, rows)

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

    # files vient d'être reconstruite : c'est le moment où l'état des hardlinks
    # est le plus fiable. Isolé pour ne jamais faire échouer la sync.
    try:
        n = refresh_arr_managed_from_links()
        if n:
            log.info("arr_managed : +%d torrent(s) via hardlinks /data/Media", n)
        # La liste qBit vient d'être récupérée pour cette sync : c'est la source
        # d'autorité sur ce qui existe encore.
        p = purge_arr_managed_absents({t.get("hash", "").lower() for t in torrents})
        if p:
            log.info("arr_managed : -%d torrent(s) absents de qBittorrent", p)
    except Exception as e:
        log.error("Erreur rafraîchissement arr_managed: %s", e)

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


class _IndexDropTooLarge(Exception):
    """Levée pour annuler (rollback) une réindexation qui perdrait trop de tags."""


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
                    # Par défaut, Jellyfin remplace les films rangés dans une collection
                    # par la collection elle-même : 189 films sur 1654 manquaient à l'index
                    # sur l'installation de référence, et leur suppression restait sans effet.
                    "CollapseBoxSetItems": "false",
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

    updates: list[tuple[str, str | None, str | None, str]] = []
    sans_path = 0
    for item in items:
        item_id, path = item.get("Id"), item.get("Path")
        if not item_id or not path:
            sans_path += 1
            continue
        # SeriesId/SeasonId sont renvoyés nativement pour les épisodes : c'est ce
        # qui permet de traiter une suppression de série ou de saison, Jellyfin
        # n'émettant qu'un seul event sur le conteneur.
        updates.append(
            (item_id, item.get("SeriesId"), item.get("SeasonId"), map_jellyfin_path(path))
        )

    if not updates:
        log.warning("Jellyfin : aucun item exploitable (%d items reçus)", len(items))
        return 0

    try:
        with _db_lock:
            with get_db() as conn:
                avant = conn.execute(
                    "SELECT COUNT(DISTINCT path) FROM files WHERE jellyfin_item_id IS NOT NULL"
                ).fetchone()[0]

                # Repart de zéro : un item réidentifié change d'id, l'ancien tag
                # ne doit pas survivre. Atomique — rollback si l'UPDATE échoue.
                conn.execute(
                    "UPDATE files SET jellyfin_item_id = NULL, jellyfin_series_id = NULL, "
                    "jellyfin_season_id = NULL WHERE jellyfin_item_id IS NOT NULL"
                )
                conn.executemany(
                    "UPDATE files SET jellyfin_item_id = ?, jellyfin_series_id = ?, "
                    "jellyfin_season_id = ? WHERE path = ?",
                    updates,
                )
                tagged = conn.execute(
                    "SELECT COUNT(DISTINCT path) FROM files WHERE jellyfin_item_id IS NOT NULL"
                ).fetchone()[0]

                # Pendant une analyse de médiathèque, Jellyfin peut renvoyer des items
                # sans chemin : le reset les effacerait de l'index. On annule plutôt
                # que de dégrader — l'exception fait rollback via get_db().
                if avant > 0 and tagged < avant * 0.7:
                    raise _IndexDropTooLarge(f"{avant} → {tagged}")
    except _IndexDropTooLarge as e:
        log.warning(
            "⚠️ Indexation Jellyfin annulée : les tags chuteraient de %s "
            "(analyse de médiathèque en cours ?). Index précédent conservé.", e
        )
        return 0

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


def _jellyfin_paths_for_item(item_id: str, item_type: str = "") -> list[str]:
    """
    Chemins DB rattachés à cet item Jellyfin.

    Un Movie/Episode est tagué directement ; une Series/Season est un dossier, on
    passe donc par les épisodes qu'elle contient (colonnes jellyfin_series_id /
    jellyfin_season_id).
    """
    column = {
        "Series": "jellyfin_series_id",
        "Season": "jellyfin_season_id",
    }.get(item_type, "jellyfin_item_id")
    with _db_lock:
        with get_db() as conn:
            rows = conn.execute(
                f"SELECT DISTINCT path FROM files WHERE {column} = ?", (item_id,)
            ).fetchall()
    return [r["path"] for r in rows]


# Profondeur minimale d'un dossier commun exploitable : ['', 'data', 'Media', 'Séries', 'Titre'].
# Au-dessus, le préfixe engloberait une bibliothèque entière — jamais légitime pour
# une seule série, et le LIKE ratisserait des torrents sans rapport. Ça arrive quand
# Jellyfin réunit deux dossiers d'une même bibliothèque sous une seule série.
_MIN_ROOT_DEPTH = 5


def _common_root(paths: list[str]) -> str:
    """
    Ramène une liste de chemins à un point d'entrée unique pour move_torrents_for_path.

    Un seul chemin → lui-même (branche correspondance exacte). Plusieurs → leur
    dossier commun, que la branche préfixe traite en une seule passe qBit au lieu
    d'une session par épisode.

    Découpage sur "/" plutôt que os.path.commonpath : les chemins de la DB sont
    toujours POSIX, et on garantit ainsi une coupure sur une frontière de segment.
    Retourne "" si le dossier commun est trop haut pour être sûr.
    """
    if len(paths) == 1:
        return paths[0]

    common: list[str] = []
    for segments in zip(*(p.split("/") for p in paths)):
        if len(set(segments)) != 1:
            break
        common.append(segments[0])

    if len(common) < _MIN_ROOT_DEPTH:
        return ""
    return "/".join(common)


def _jellyfin_paths_by_metadata(data: dict[str, Any], max_paths: int) -> list[str]:
    """
    Filet quand l'item n'a jamais été indexé : on restreint par métadonnées puis
    on ne garde que les chemins réellement absents du disque. Un fichier encore
    présent n'est jamais retenu, ce qui rend un faux positif très improbable.
    """
    item_type = data.get("ItemType", "")
    patterns: list[str] = []

    if item_type in ("Series", "Season"):
        series = data.get("SeriesName") or data.get("Name")
        if series:
            season = data.get("IndexNumber") if item_type == "Season" else None
            if season is not None:
                patterns.append(f"%/{series}%/Season {int(season):02d}/%")
            else:
                patterns.append(f"%/{series}/%")
                patterns.append(f"%/{series} (%/%")
    elif item_type == "Episode":
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
    if len(disparus) > max_paths:
        log.warning(
            "  [fallback] %d chemins pour un seul item — motif jugé trop large, abandon "
            "(plafond %d)",
            len(disparus), max_paths,
        )
        return []
    return disparus


# ── Délégation à Sonarr / Radarr ─────────────────────────────────────────────


def _arr_request(
    method: str, base_url: str, api_key: str, endpoint: str, timeout: float = 30, **kwargs: Any
) -> Any:
    """Appel à l'API v3 d'un *arr. Retourne le JSON, ou None si corps vide."""
    session = _create_retry_session()
    try:
        r = session.request(
            method,
            f"{base_url.rstrip('/')}/api/v3/{endpoint}",
            headers={"X-Api-Key": api_key},
            timeout=timeout,
            **kwargs,
        )
        r.raise_for_status()
        return r.json() if r.content else None
    finally:
        session.close()


def _match_by_path(entries: list[dict[str, Any]], paths: list[str]) -> dict[str, Any] | None:
    """
    Retrouve l'entrée *arr dont le dossier contient les chemins supprimés.

    Correspondance par préfixe de chemin, pas par titre : le dossier d'une série
    Sonarr est toujours un ancêtre du chemin de ses épisodes, ce qui donne une
    identification exacte là où un rapprochement de titres serait approximatif.
    Le préfixe le plus long gagne, au cas où des racines seraient imbriquées.
    """
    best: tuple[int, dict[str, Any]] | None = None
    for entry in entries:
        folder = (entry.get("path") or "").rstrip("/")
        if not folder:
            continue
        if any(p == folder or p.startswith(folder + "/") for p in paths):
            if best is None or len(folder) > best[0]:
                best = (len(folder), entry)
    return best[1] if best else None


def _delegate_sonarr(paths: list[str], item_type: str, dry_run: bool) -> dict[str, Any] | None:
    """Dé-monitore puis supprime les fichiers côté Sonarr. None si série inconnue."""
    if not (SONARR_URL and SONARR_API_KEY):
        return None

    series = _match_by_path(_arr_request("GET", SONARR_URL, SONARR_API_KEY, "series") or [], paths)
    if not series:
        return None

    sid, titre = series["id"], series.get("title", "?")

    # Série entière : on retire la fiche plutôt que de la vider. Sonarr émet
    # alors un seul SeriesDelete, que /webhook résout par préfixe de dossier —
    # au lieu d'un EpisodeFileDelete par épisode, tous sur le même torrent.
    if item_type == "Series" and JELLYFIN_ARR_DELETE_CONTAINERS:
        action = f"Sonarr : série '{titre}' supprimée de Sonarr avec ses fichiers"
        if dry_run:
            log.info("  [DRY RUN] %s", action)
            return {"arr": "sonarr", "series": titre, "supprimee": True, "dry_run": True}
        _arr_request("DELETE", SONARR_URL, SONARR_API_KEY,
                     f"series/{sid}", params={"deleteFiles": "true"})
        log.info("  ✓ %s", action)
        return {"arr": "sonarr", "series": titre, "supprimee": True}

    targets = [
        f
        for f in (
            _arr_request("GET", SONARR_URL, SONARR_API_KEY, "episodefile", params={"seriesId": sid})
            or []
        )
        if f.get("path") in set(paths)
    ]
    if not targets:
        log.info("  Sonarr : série '%s' trouvée mais aucun episodefile correspondant", titre)
        return None

    file_ids = {f["id"] for f in targets}
    episodes = _arr_request(
        "GET", SONARR_URL, SONARR_API_KEY, "episode", params={"seriesId": sid}
    ) or []
    episode_ids = [e["id"] for e in episodes if e.get("episodeFileId") in file_ids]

    action = f"Sonarr '{titre}' : {len(episode_ids)} épisode(s) dé-monitoré(s), {len(targets)} fichier(s) supprimé(s)"
    if item_type == "Series":
        action += " + série dé-monitorée"

    if dry_run:
        log.info("  [DRY RUN] %s", action)
        return {"arr": "sonarr", "series": titre, "episodes": len(episode_ids),
                "files": len(targets), "dry_run": True}

    # Dé-monitorer d'abord : sinon Sonarr peut relancer une recherche entre les
    # deux appels et retélécharger ce qu'on vient de retirer.
    if episode_ids:
        _arr_request("PUT", SONARR_URL, SONARR_API_KEY, "episode/monitor",
                     json={"episodeIds": episode_ids, "monitored": False})
    if item_type == "Series":
        # Endpoint "editor" plutôt que PUT series/{id} : il prend une charge
        # minimale au lieu d'exiger le renvoi de l'objet série complet, qu'un
        # champ mal repris suffirait à abîmer.
        _arr_request(
            "PUT", SONARR_URL, SONARR_API_KEY, "series/editor",
            json={"seriesIds": [sid], "monitored": False},
        )

    # La suppression déclenche le webhook EpisodeFileDelete de Sonarr, qui
    # repasse par /webhook et déplace les torrents.
    for f in targets:
        _arr_request("DELETE", SONARR_URL, SONARR_API_KEY, f"episodefile/{f['id']}")

    log.info("  ✓ %s", action)
    return {"arr": "sonarr", "series": titre, "episodes": len(episode_ids), "files": len(targets)}


def _delegate_radarr(paths: list[str], dry_run: bool) -> dict[str, Any] | None:
    """Dé-monitore puis supprime le fichier côté Radarr. None si film inconnu."""
    if not (RADARR_URL and RADARR_API_KEY):
        return None

    movie = _match_by_path(_arr_request("GET", RADARR_URL, RADARR_API_KEY, "movie") or [], paths)
    if not movie:
        return None

    mid, titre = movie["id"], movie.get("title", "?")
    file_id = (movie.get("movieFile") or {}).get("id")

    # Un film est un conteneur à lui seul : le supprimer de Jellyfin veut dire
    # qu'on n'en veut plus, pas qu'on attend une meilleure version.
    if JELLYFIN_ARR_DELETE_CONTAINERS:
        action = f"Radarr : film '{titre}' supprimé de Radarr avec ses fichiers"
        if dry_run:
            log.info("  [DRY RUN] %s", action)
            return {"arr": "radarr", "movie": titre, "supprime": True, "dry_run": True}
        _arr_request("DELETE", RADARR_URL, RADARR_API_KEY,
                     f"movie/{mid}", params={"deleteFiles": "true"})
        log.info("  ✓ %s", action)
        return {"arr": "radarr", "movie": titre, "supprime": True}

    action = f"Radarr '{titre}' : dé-monitoré" + (
        f", fichier {file_id} supprimé" if file_id else ", aucun fichier référencé"
    )
    if dry_run:
        log.info("  [DRY RUN] %s", action)
        return {"arr": "radarr", "movie": titre, "file_id": file_id, "dry_run": True}

    # Endpoint "editor" : charge minimale, aucun renvoi de l'objet film complet.
    _arr_request(
        "PUT", RADARR_URL, RADARR_API_KEY, "movie/editor",
        json={"movieIds": [mid], "monitored": False},
    )
    if file_id:
        _arr_request("DELETE", RADARR_URL, RADARR_API_KEY, f"moviefile/{file_id}")

    log.info("  ✓ %s", action)
    return {"arr": "radarr", "movie": titre, "file_id": file_id}


# Taille des lots de paramètres SQL : sous la limite historique de SQLite (999).
_SQL_CHUNK = 500


def _arr_candidate_paths(paths: list[str]) -> list[str]:
    """
    Chemins sous lesquels Sonarr/Radarr peuvent connaître les fichiers supprimés.

    Jellyfin et l'*arr désignent parfois le même fichier par deux chemins. Une
    bibliothèque posée sur le dossier de téléchargement (/data/Multimedia/Films)
    voit la copie seedée ; Radarr, lui, connaît le hardlink qu'il a importé
    (/data/Media/Films/...). Comparer les seuls chemins Jellyfin aux dossiers *arr
    échoue alors à coup sûr, et l'*arr garde un fichier devenu invisible.

    On y ajoute donc les autres chemins du même inode connus de la base. Ils y
    sont encore au moment de l'event : la sync ne purge un chemin disparu qu'au
    passage suivant.

    Un lien n'est retenu que s'il désigne toujours cet inode sur le disque. Un
    fichier remplacé depuis la dernière sync — upgrade *arr — est un autre
    fichier, qu'une suppression Jellyfin ne doit pas entraîner.
    """
    connus = set(paths)
    trouves: dict[str, int] = {}
    with _db_lock:
        with get_db() as conn:
            for i in range(0, len(paths), _SQL_CHUNK):
                lot = paths[i:i + _SQL_CHUNK]
                ph = ",".join("?" * len(lot))
                for r in conn.execute(
                    f"SELECT DISTINCT f2.path, f2.inode FROM files f1 "
                    f"JOIN files f2 ON f2.inode = f1.inode "
                    f"WHERE f1.path IN ({ph}) AND f1.stale = 0 AND f2.stale = 0",
                    lot,
                ):
                    if r["path"] not in connus:
                        trouves[r["path"]] = r["inode"]

    liens = []
    for chemin, inode in trouves.items():
        try:
            if os.stat(chemin).st_ino == inode:
                liens.append(chemin)
        except OSError:
            continue
    return list(paths) + sorted(liens)


def _locate_inode(inode: int, hashes: set[str]) -> str | None:
    """
    Cherche un lien vers l'inode dans les torrents qui le portaient, là où ils
    sont maintenant : un changement de catégorie qBittorrent déplace leurs données,
    et l'index ne le saura qu'à la sync suivante.

    Lève une exception si qBittorrent ne répond pas : l'appelant doit pouvoir
    distinguer « introuvable » de « impossible à vérifier ».
    """
    if not hashes:
        return None
    with qbit_session() as s:
        r = s.get(
            f"{QBIT_URL}/api/v2/torrents/info",
            params={"hashes": "|".join(sorted(hashes))},
            timeout=10,
        )
        r.raise_for_status()
        torrents = r.json()

    for t in torrents:
        racine = torrent_root(t)
        if os.path.isfile(racine):
            fichiers: list[str] = [racine]
        else:
            fichiers = [
                os.path.join(dossier, nom)
                for dossier, _, noms in os.walk(racine)
                for nom in noms
                if os.path.splitext(nom)[1].lower() in VIDEO_EXTENSIONS
            ]
        for f in fichiers:
            try:
                if os.lstat(f).st_ino == inode:
                    return f
            except OSError:
                continue
    return None


def _moved_paths(paths: list[str]) -> set[str]:
    """
    Parmi des chemins disparus du disque, ceux dont le fichier existe encore ailleurs.

    Changer un titre de bibliothèque Jellyfin fait disparaître l'ancien chemin
    exactement comme une suppression, que le dossier ait été déplacé par
    Sonarr/Radarr ou les données par un changement de catégorie qBittorrent.
    Jellyfin émet le même ItemDeleted, et l'index ne connaîtra le nouveau chemin
    qu'à la sync suivante. Traité comme une suppression, l'event retirerait le
    titre de l'*arr ou enverrait ses torrents à la corbeille.

    Seul le compteur de liens de l'inode distingue les deux cas : une suppression
    le fait baisser, un déplacement le laisse intact. Si st_nlink dépasse le nombre
    de liens connus encore présents, un lien est apparu ailleurs depuis la sync :
    le fichier vit sous un autre nom.

    Quand aucun lien connu ne subsiste — torrent et hardlink *arr déplacés tous les
    deux —, on cherche l'inode à l'emplacement actuel des torrents. Si qBittorrent
    ne répond pas, le doute profite au fichier : le chemin est rendu comme déplacé
    et rien n'est fait.
    """
    disparus_par_inode: dict[int, list[str]] = {}
    connus: dict[int, set[str]] = {}
    hashes: dict[int, set[str]] = {}
    with _db_lock:
        with get_db() as conn:
            for i in range(0, len(paths), _SQL_CHUNK):
                lot = paths[i:i + _SQL_CHUNK]
                ph = ",".join("?" * len(lot))
                for r in conn.execute(
                    f"SELECT f1.path AS disparu, f2.inode, f2.path, f2.torrent_hash "
                    f"FROM files f1 JOIN files f2 ON f2.inode = f1.inode "
                    f"WHERE f1.path IN ({ph}) AND f1.stale = 0 AND f2.stale = 0",
                    lot,
                ):
                    inode = r["inode"]
                    disparus = disparus_par_inode.setdefault(inode, [])
                    if r["disparu"] not in disparus:
                        disparus.append(r["disparu"])
                    connus.setdefault(inode, set()).add(r["path"])
                    if r["torrent_hash"]:
                        hashes.setdefault(inode, set()).add(r["torrent_hash"])

    # lstat : on compte des hardlinks, un lien symbolique n'en est pas un.
    deplaces: set[str] = set()
    for inode, chemins in connus.items():
        presents = []
        for chemin in chemins:
            try:
                if os.lstat(chemin).st_ino == inode:
                    presents.append(chemin)
            except OSError:
                continue

        sonde = presents[0] if presents else None
        if sonde is None:
            try:
                sonde = _locate_inode(inode, hashes.get(inode, set()))
            except Exception as e:
                log.warning("  qBit injoignable, inode %d présumé déplacé : %s", inode, e)
                deplaces.update(disparus_par_inode[inode])
                continue
            if sonde is None:
                continue  # plus aucun lien nulle part : vraie suppression

        try:
            liens = os.lstat(sonde).st_nlink
        except OSError:
            continue
        if liens > len(presents):
            deplaces.update(disparus_par_inode[inode])
    return deplaces


def delegate_to_arr(paths: list[str], item_type: str, dry_run: bool) -> dict[str, Any] | None:
    """
    Traduit une suppression Jellyfin en action Sonarr/Radarr.

    Retourne None si le média n'est géré par aucun *arr — l'appelant retombe
    alors sur le déplacement direct des torrents.
    """
    try:
        candidats = _arr_candidate_paths(paths)
    except Exception as e:
        log.error("  Résolution des hardlinks impossible, chemins Jellyfin seuls : %s", e)
        candidats = list(paths)
    if len(candidats) > len(paths):
        log.info("  %d autre(s) chemin(s) du même fichier, via l'inode : %s",
                 len(candidats) - len(paths), candidats[len(paths)])

    try:
        if item_type in ("Series", "Season", "Episode"):
            return _delegate_sonarr(candidats, item_type, dry_run)
        if item_type in ("Movie", "Video"):
            return _delegate_radarr(candidats, dry_run)
    except Exception as e:
        log.error("  Erreur délégation *arr : %s", e)
    return None


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
            qbit_torrents = qbit_get_torrents(s)
            infos = {x["hash"].lower(): x for x in qbit_torrents}
            safe_to_move: list[dict[str, Any]] = []

            for t in all_torrents:
                torrent_info = infos.get(t["info_hash"].lower())
                if not torrent_info:
                    safe_to_move.append(t)
                    continue

                root = torrent_root(torrent_info)
                still_linked = False

                if check_links:
                    still_linked = _check_hardlinks_active(root, inodes)

                if still_linked:
                    log.info("  ⏭ [%s] %s ignoré (hardlinks encore actifs)", t.get("category", "?"), t["name"])
                else:
                    safe_to_move.append(t)

            partages = _shared_content({t["info_hash"] for t in safe_to_move}, qbit_torrents)
            for t in safe_to_move:
                autre = partages.get(t["info_hash"].lower())
                if autre:
                    log.info("  ⏭ [%s] %s ignoré (fichiers partagés avec [%s] %s)",
                             t.get("category", "?"), t["name"], autre.get("category", "?"), autre.get("name", "?"))
            safe_to_move = [t for t in safe_to_move if t["info_hash"].lower() not in partages]

            if not safe_to_move:
                log.info("  → Aucun torrent à déplacer (hardlinks actifs ou fichiers partagés)")
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
            # Un refus de qBit (catégorie inconnue, session expirée) ne lève rien de
            # lui-même : sans ce contrôle, on tracerait des moves jamais faits et on
            # purgerait leurs lignes de l'index.
            r = s.post(
                f"{QBIT_URL}/api/v2/torrents/setCategory",
                data={"hashes": hashes, "category": TARGET_CAT},
                timeout=10,
            )
            r.raise_for_status()

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


# ── Indexation à l'import ────────────────────────────────────────────────────


def _imported_paths(data: dict[str, Any]) -> list[str]:
    """Chemins des fichiers qu'un event Download de Sonarr/Radarr vient d'importer."""
    files = [data[k] for k in ("episodeFile", "movieFile") if isinstance(data.get(k), dict)]
    files += [f for f in data.get("episodeFiles") or [] if isinstance(f, dict)]
    folder = ((data.get("movie") or {}).get("folderPath") or "").rstrip("/")

    paths: list[str] = []
    for f in files:
        path = f.get("path")
        if not path and folder and f.get("relativePath"):
            path = f"{folder}/{f['relativePath']}"
        if path and path not in paths:
            paths.append(path)
    return paths


def index_import(data: dict[str, Any]) -> int:
    """
    Indexe tout de suite les fichiers d'un import *arr, sans attendre la sync.

    Un fichier importé puis supprimé avant la sync suivante n'avait aucune ligne
    dans `files` : la suppression tombait sur « Chemin non trouvé en DB » et le
    torrent restait dans sa catégorie (5 cas en trois jours sur l'installation de
    référence). L'event donne le chemin importé et le hash du torrent ; l'inode
    relie l'un à l'autre.

    On ne retient que ce que la sync aurait trouvé : un hardlink sous /data/Media
    d'un fichier du torrent. Une copie, qui a son propre inode, n'est pas rattachée.

    Retourne le nombre de lignes écrites. Ne lève jamais : un échec ici ne doit pas
    faire échouer le webhook, et la sync suivante rattrapera.
    """
    download_id = (data.get("downloadId") or "").lower().strip()
    paths = [p for p in _imported_paths(data) if p.startswith("/data/Media/")]
    if not download_id or not paths:
        return 0

    try:
        with qbit_session() as s:
            r = s.get(
                f"{QBIT_URL}/api/v2/torrents/info",
                params={"hashes": download_id},
                timeout=10,
            )
            r.raise_for_status()
            infos = r.json()
        if not infos:
            log.info("  Import non indexé : torrent %s absent de qBittorrent", download_id[:8])
            return 0
        t = infos[0]

        racine = torrent_root(t)
        if os.path.isfile(racine):
            fichiers = [racine]
        else:
            fichiers = [
                os.path.join(dossier, nom)
                for dossier, _, noms in os.walk(racine)
                for nom in noms
                if os.path.splitext(nom)[1].lower() in VIDEO_EXTENSIONS
            ]
        seed_par_inode: dict[int, str] = {}
        for f in fichiers:
            try:
                seed_par_inode.setdefault(os.lstat(f).st_ino, f)
            except OSError:
                continue

        rows: list[tuple[int, str, str, str, str]] = []
        for path in paths:
            try:
                inode = os.lstat(path).st_ino
            except OSError:
                continue
            seed = seed_par_inode.get(inode)
            if not seed:
                log.info("  Import non indexé : %s n'est pas un hardlink du torrent", path)
                continue
            for p in (path, seed):
                rows.append((inode, p, t["hash"], t.get("name", ""), t.get("category", "")))

        if rows:
            with _db_lock:
                with get_db() as conn:
                    conn.executemany(_UPSERT_FILES, rows)
        return len(rows)
    except Exception as e:
        log.error("  Indexation de l'import impossible, la sync rattrapera : %s", e)
        return 0


# ── Assistant d'import ───────────────────────────────────────────────────────

# Un téléchargement dans cet état n'avancera plus seul : l'*arr a tenté l'import
# et attend une intervention. Ceux en cours de téléchargement ou d'import ne
# sont jamais touchés.
_ASSIST_STATES = {"importBlocked"}
# Un même verdict n'est journalisé qu'une fois par jour, et une action réelle
# n'est jamais rejouée dans ce délai.
_ASSIST_REPEAT = 24 * 3600
# L'*arr émet l'event juste avant de rafraîchir sa file : le passage attend
# qu'elle affiche l'état « importBlocked ». Les events reçus entre-temps se
# regroupent dans ce même passage.
_ASSIST_EVENT_DELAY = 120
# Attente maximale du verrou quand un autre passage tourne déjà.
_ASSIST_LOCK_WAIT = 30
# Sonarr lance une recherche d'indexeur par épisode (EpisodeSearchCommand) :
# au-delà de ce nombre, la recherche est laissée à la main pour ne pas inonder
# les trackers — une saison d'anime TVDB peut compter des centaines d'épisodes.
_ASSIST_MAX_SEARCH = 25
# Sonarr renvoie une ligne de file par épisode : la file est lue par pages.
_ASSIST_QUEUE_PAGE = 1000
# Un échantillon porte « sample » dans son nom ET reste petit : un épisode
# intitulé « Free Sample » compte bien parmi les vidéos à retrouver.
_SAMPLE_RE = re.compile(r"(^|[\W_])samples?([\W_]|$)", re.IGNORECASE)
_SAMPLE_MAX_BYTES = 300 * 1024 * 1024
_assist_started = threading.Event()
_assist_timer_lock = threading.Lock()
_assist_timer: threading.Timer | None = None
# Nom de chaque règle, dans les logs et dans /assist/history : on y lit quelle
# règle a fait quoi.
_REGLES = {1: "débloquer la file", 4: "pack incomplet", 5: "bonne saison"}


def _regle(n: int) -> str:
    return f"règle {n} · {_REGLES[n]}"


def _assist_log(regle: int, signe: str, arr: str, titre: str, details: str, dry_run: bool) -> None:
    """Une ligne de log de l'assistant, au nom de la règle qui l'écrit."""
    log.info("  %s%s [%s] [%s] %s : %s", "[SIMULATION] " if dry_run else "", signe, _regle(regle), arr, titre, details)


def _assist_modes() -> str:
    """Ce que fait chaque règle avec la configuration actuelle : agir, simuler, ou rien."""
    def mode(simulation: bool) -> str:
        return "en simulation" if simulation else "ACTIVE"
    sonarr = bool(SONARR_URL and SONARR_API_KEY)
    r4 = (mode(IMPORT_ASSIST_PACK_DRY_RUN) if sonarr and PROWLARR_URL and PROWLARR_API_KEY
          else "signale seulement (pas de Prowlarr)" if sonarr else "inactive (pas de Sonarr)")
    r5 = mode(IMPORT_ASSIST_ACCORD_DRY_RUN) if sonarr and SEERR_URL and SEERR_API_KEY else "inactive (Seerr ou Sonarr non configuré)"
    return f"{_regle(1)} {mode(IMPORT_ASSIST_DRY_RUN)}, {_regle(4)} {r4}, {_regle(5)} {r5}"


@contextmanager
def _assist_exclusive(attente: float):
    """
    Un seul passage à la fois, entre tous les process : le passage régulier
    tourne dans le maître gunicorn, les events et /assist dans les workers.
    Rend False si le verrou n'est pas libéré dans le délai.
    """
    import fcntl  # POSIX seulement : importé ici pour que le module se charge ailleurs

    fd = os.open(DB_PATH + ".assist.lock", os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fin = time.time() + attente
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.time() >= fin:
                    yield False
                    return
                time.sleep(1)
        try:
            yield True
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def _assist_arrs() -> list[tuple[str, str, str]]:
    """(nom, URL, clé) des *arr configurés."""
    return [
        (nom, url, cle)
        for nom, url, cle in (
            ("sonarr", SONARR_URL, SONARR_API_KEY),
            ("radarr", RADARR_URL, RADARR_API_KEY),
        )
        if url and cle
    ]


def _torrent_videos(t: dict[str, Any]) -> list[str] | None:
    """
    Vidéos d'un torrent, échantillons exclus : aucun *arr ne les importe.
    None si ses données sont introuvables sur le disque.
    """
    racine = torrent_root(t)
    if os.path.isfile(racine):
        fichiers = [racine]
    elif os.path.isdir(racine):
        fichiers = [os.path.join(d, n) for d, _, noms in os.walk(racine) for n in noms]
    else:
        return None
    videos: list[str] = []
    for f in fichiers:
        if os.path.splitext(f)[1].lower() not in VIDEO_EXTENSIONS:
            continue
        nom = os.path.basename(f) if f == racine else os.path.relpath(f, racine)
        try:
            petit = os.path.getsize(f) < _SAMPLE_MAX_BYTES
        except OSError:
            petit = False
        if petit and _SAMPLE_RE.search(nom):
            continue
        videos.append(f)
    return videos


def _client_field(clients: list[dict[str, Any]], nom: str, champs: tuple[str, ...]) -> str:
    """Valeur du premier de ces champs renseigné sur le client *arr nommé."""
    for c in clients:
        if c.get("name") != nom:
            continue
        for f in c.get("fields") or []:
            if f.get("name") in champs and f.get("value"):
                return f["value"]
    return ""


def _imported_category(clients: list[dict[str, Any]], nom: str) -> str:
    """Catégorie après import du client *arr : celle qu'il aurait posée lui-même."""
    return _client_field(clients, nom, ("tvImportedCategory", "movieImportedCategory"))


def _assist_seen(download_id: str, decision: str, dry_run: bool) -> bool:
    with _db_lock:
        with get_db() as conn:
            return conn.execute(
                "SELECT 1 FROM assist_log WHERE download_id = ? AND decision = ? "
                "AND dry_run = ? AND at > ?",
                (download_id, decision, int(dry_run), int(time.time()) - _ASSIST_REPEAT),
            ).fetchone() is not None


def _assist_record(download_id: str, arr: str, title: str, decision: str,
                   details: str, dry_run: bool, regle: int) -> None:
    with _db_lock:
        with get_db() as conn:
            conn.execute(
                "INSERT INTO assist_log (download_id, arr, title, decision, details, dry_run, rule) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (download_id, arr, title, decision, details, int(dry_run), regle),
            )


def run_import_assist(dry_run: bool | None = None, source: str = "manuel") -> list[dict[str, Any]] | None:
    """
    Un passage de l'assistant d'import sur les files Sonarr/Radarr.

    Règle 1 — déjà importé mais coincé : chaque vidéo du torrent a déjà un hardlink
    dans le dossier du média. C'est le cas d'un pack « S01 » qui n'apporte qu'une
    partie d'une saison TVDB (l'*arr attend les épisodes manquants et garde le
    téléchargement), ou d'un import fait à la main sans son téléchargement.
    Le torrent passe dans la catégorie après import de son client, ce qui le sort
    de la file : tant qu'il y restait, l'*arr refusait les releases des épisodes
    manquants, que Sonarr cherche alors.

    La correspondance se fait par inode, sans aucun nom de fichier. Rien n'est
    importé, rien n'est supprimé, rien ne bouge sur le disque.

    La règle 5 passe d'abord, sur les demandes Seerr récentes : une saison
    demandée qui correspond à une autre saison TVDB n'a plus d'épisodes à chercher.
    Retourne un verdict par téléchargement bloqué et par désaccord de saison, ou
    None si un autre passage tournait déjà.

    `dry_run` : None, chaque règle suit son interrupteur ; True simule toutes les
    règles (aperçu) ; False fait agir la règle 1, les autres suivant le leur.
    """
    apercu = dry_run is True
    dry_run = IMPORT_ASSIST_DRY_RUN if dry_run is None else dry_run
    with _assist_exclusive(_ASSIST_LOCK_WAIT) as libre:
        if not libre:
            log.info("Assistant d'import déjà en cours, passage %s ignoré", source)
            return None

        verdicts_accord, ecartees = run_accord(apercu)

        # Les files d'abord : sans téléchargement bloqué, inutile de lister qBit.
        bloques: list[tuple[str, str, str, dict[str, list[dict[str, Any]]]]] = []
        for arr, url, cle in _assist_arrs():
            try:
                par_dl = _assist_blocked(url, cle)
            except Exception as e:
                log.error("Assistant d'import : %s injoignable ou réponse inattendue (%s)", arr, e)
                continue
            if par_dl:
                bloques.append((arr, url, cle, par_dl))
        if not bloques:
            return verdicts_accord

        try:
            with qbit_session() as s:
                torrents = {t["hash"].lower(): t for t in qbit_get_torrents(s)}
                r = s.get(f"{QBIT_URL}/api/v2/torrents/categories", timeout=10)
                r.raise_for_status()
                categories = {
                    nom: (info.get("savePath") or "").rstrip("/") for nom, info in r.json().items()
                }
        except Exception as e:
            log.error("Assistant d'import : qBittorrent injoignable (%s)", e)
            return verdicts_accord

        ctx = {"torrents": torrents, "categories": categories, "dry_run": dry_run, "apercu": apercu,
               "ecartees": ecartees}
        verdicts: list[dict[str, Any]] = list(verdicts_accord)
        nouveaux = 0
        for arr, url, cle, par_dl in bloques:
            try:
                clients = _arr_request("GET", url, cle, "downloadclient") or []
            except Exception as e:
                log.error("Assistant d'import : %s injoignable ou réponse inattendue (%s)", arr, e)
                continue
            inodes_par_media: dict[int, set[int]] = {}
            for dl, records in par_dl.items():
                try:
                    verdict, nouveau = _assist_download(arr, url, cle, dl, records, clients, inodes_par_media, ctx)
                except Exception as e:
                    # Un téléchargement en erreur n'empêche pas de traiter les suivants.
                    titre = records[0].get("title") or dl
                    log.error("  ⚠ [%s] [%s] %s en erreur (%s)", _regle(1), arr, titre, e)
                    verdict = {"arr": arr, "download_id": dl, "title": titre, "decision": "error", "reason": str(e)}
                    nouveau = False
                verdicts.append(verdict)
                nouveaux += nouveau
        if nouveaux:
            # Chaque ligne au-dessus dit sa règle, et si elle n'est qu'une simulation.
            log.info(
                "=== Assistant d'import (%s) : %d téléchargement(s) bloqué(s), %d nouveau(x) verdict(s) ===",
                source, len(verdicts) - len(verdicts_accord), nouveaux,
            )
        return verdicts


def schedule_import_assist(source: str = "webhook") -> bool:
    """
    Programme un passage dans _ASSIST_EVENT_DELAY secondes. Les events suivants
    se regroupent dans ce passage : False s'il était déjà programmé.
    """
    global _assist_timer
    with _assist_timer_lock:
        if _assist_timer is not None and _assist_timer.is_alive():
            return False
        _assist_timer = threading.Timer(_ASSIST_EVENT_DELAY, _run_scheduled_assist, kwargs={"source": source})
        _assist_timer.daemon = True
        _assist_timer.start()
        return True


def _run_scheduled_assist(source: str) -> None:
    global _assist_timer
    # Libéré avant le passage : un event reçu pendant qu'il tourne en programme un autre.
    with _assist_timer_lock:
        _assist_timer = None
    try:
        if run_import_assist(source=source) is None:
            # Un autre passage tenait le verrou — une recherche de pack peut durer
            # plusieurs minutes. Il a pu lire la file avant ce blocage : on repasse.
            schedule_import_assist(source)
    except Exception as e:
        log.error("Erreur assistant d'import : %s", e)


def _assist_blocked(url: str, cle: str) -> dict[str, list[dict[str, Any]]]:
    """Lignes de file des téléchargements bloqués d'un *arr, regroupées par téléchargement."""
    bloques: dict[str, list[dict[str, Any]]] = {}
    page, lues = 1, 0
    while True:
        # Sans les éléments « inconnus » : quand Sonarr et Radarr partagent une
        # catégorie, chacun liste les téléchargements de l'autre, sans média.
        rep = _arr_request("GET", url, cle, "queue",
                           params={"page": page, "pageSize": _ASSIST_QUEUE_PAGE}) or {}
        lot = rep.get("records") or []
        for r in lot:
            if r.get("downloadId") and r.get("trackedDownloadState") in _ASSIST_STATES:
                bloques.setdefault(r["downloadId"].lower(), []).append(r)
        lues += len(lot)
        if not lot or lues >= (rep.get("totalRecords") or 0):
            return bloques
        page += 1


def _assist_download(
    arr: str, url: str, cle: str, dl: str, records: list[dict[str, Any]],
    clients: list[dict[str, Any]], inodes_par_media: dict[int, set[int]],
    ctx: dict[str, Any],
) -> tuple[dict[str, Any], bool]:
    """Applique la règle 1 à un téléchargement. Retourne (verdict, journalisé maintenant)."""
    dry_run = ctx["dry_run"]
    titre = records[0].get("title") or dl
    verdict: dict[str, Any] = {"arr": arr, "download_id": dl, "title": titre}

    def ecarte(raison: str, regle: int = 1, simulation: bool | None = None) -> tuple[dict[str, Any], bool]:
        # La règle 4 finit ses remplacements ici, avec sa propre simulation.
        simulation = dry_run if simulation is None else simulation
        verdict.update(decision="ignored", reason=raison)
        decision = f"ignored:{raison}"
        if _assist_seen(dl, decision, simulation):
            return verdict, False
        _assist_record(dl, arr, titre, decision, raison, simulation, regle)
        _assist_log(regle, "⏭", arr, titre, raison, simulation)
        return verdict, True

    t = ctx["torrents"].get(dl)
    if not t:
        return ecarte("absent de qBittorrent")
    if (t.get("progress") or 0) < 1:
        return ecarte("téléchargement incomplet")

    cle_media, route = ("seriesId", "series") if arr == "sonarr" else ("movieId", "movie")
    medias = {r.get(cle_media) for r in records}
    if len(medias) != 1 or None in medias:
        return ecarte("média inconnu ou multiple")
    media_id = medias.pop()
    if media_id not in inodes_par_media:
        dossier = (_arr_request("GET", url, cle, f"{route}/{media_id}") or {}).get("path") or ""
        inodes_par_media[media_id] = set(scan_video_files(dossier)) if dossier else set()
    inodes = inodes_par_media[media_id]

    # Chaque vidéo doit avoir un hardlink dans le dossier du média du grab — pas
    # ailleurs dans la bibliothèque : un torrent rangé sous une autre série
    # n'est pas « importé » pour celle-ci.
    videos = _torrent_videos(t)
    if videos is None:
        return ecarte("données du torrent introuvables sur le disque")
    if not videos:
        return ecarte("aucune vidéo dans le torrent")
    lies = 0
    for v in videos:
        try:
            lies += os.stat(v).st_ino in inodes
        except OSError:
            continue
    if lies < len(videos):
        # Règle 4 : le pack complet ajouté à la place d'un pack partiel n'importe
        # seul que les épisodes manquants. Le remplacement se termine ici.
        remplacement = _pack_replacement(dl) if arr == "sonarr" else None
        if remplacement:
            return _pack_finish(url, cle, dl, titre, inodes, remplacement, ctx, verdict, ecarte)
        return ecarte(f"{lies}/{len(videos)} vidéo(s) dans la bibliothèque")

    categorie = _imported_category(clients, records[0].get("downloadClient") or "")
    if not categorie:
        return ecarte("pas de catégorie après import sur le client")
    if categorie == t.get("category"):
        # Changer de catégorie ne sortirait pas le torrent de la file : l'action
        # serait rejouée chaque jour, recherche comprise.
        return ecarte(f"déjà dans la catégorie après import {categorie}")
    if categorie not in ctx["categories"]:
        return ecarte(f"catégorie {categorie} absente de qBittorrent")
    if t.get("auto_tmm") and ctx["categories"][categorie] != (t.get("save_path") or "").rstrip("/"):
        # Garde-fou : la règle ne doit rien déplacer sur le disque.
        return ecarte(f"la catégorie {categorie} a un autre dossier")
    if not dry_run and _assist_seen(dl, "seed", False):
        return ecarte("déjà traité dans les dernières 24 h, toujours dans la file")
    # Simulation déjà journalisée : la recherche d'un pack complet n'est pas refaite.
    deja_vu = dry_run and _assist_seen(dl, "seed", True)

    # Épisodes que la release devait apporter et qui manquent encore.
    manquants: list[int] = []
    episodes: list[dict[str, Any]] = []
    attendus = {r.get("episodeId") for r in records if r.get("episodeId")}
    note_accord = ""
    if arr == "sonarr" and attendus:
        episodes = _arr_request("GET", url, cle, "episode", params={"seriesId": media_id}) or []
        voulus = _episodes_wanted(episodes)
        # Règle 5 : une saison demandée qui est en réalité une autre saison TVDB n'a
        # plus d'épisodes attendus — chercher ceux de Dahmer pour Lizzie Borden.
        ecartees = (ctx.get("ecartees") or {}).get(media_id) or set()
        hors_demande = {e["id"] for e in episodes if e.get("seasonNumber") in ecartees}
        manquants = [e["id"] for e in episodes if e["id"] in attendus and e["id"] in voulus]
        if hors_demande & set(manquants):
            note_accord = ("règle 5 : la saison " + ", ".join(str(s) for s in sorted(ecartees))
                           + " demandée est une autre saison TVDB, ses épisodes manquants ne sont pas cherchés")
            manquants = [i for i in manquants if i not in hors_demande]

    # Règle 4, filet après import : un pack de saison qui n'a pas tout apporté est
    # remplacé par un pack complet, plutôt que de chercher ses épisodes un à un.
    pack: dict[str, Any] | None = None
    note = ""
    if manquants and not deja_vu:
        try:
            pack, note = _pack_after_import(url, cle, media_id, dl, titre, records, episodes,
                                            manquants, videos, clients)
        except Exception as e:
            note = f"règle 4 : recherche d'un pack complet impossible : {e}"
            log.error("  ⚠ [%s] [%s] %s : %s", _regle(4), arr, titre, note)
    # Un remplacement déjà lancé compte, même si la règle 4 est repassée en simulation.
    remplace = pack is not None and (bool(pack.get("deja")) or not IMPORT_ASSIST_PACK_DRY_RUN)
    if remplace and not dry_run and not pack.get("deja"):
        try:
            _qbit_add(pack, pack["category"], t)
            _pack_register(media_id, pack["season"], dl, pack, "import", pack["episodes"])
        except Exception as e:
            remplace, note = False, f"règle 4 : ajout du pack complet {pack['title']} impossible : {e}"
            log.error("  ⚠ [%s] [%s] %s : %s", _regle(4), arr, titre, note)

    recherche = manquants if len(manquants) <= _ASSIST_MAX_SEARCH and not remplace else []
    resume = f"{len(videos)}/{len(videos)} vidéo(s) déjà dans la bibliothèque → {categorie}"
    if remplace:
        resume += f", règle 4 : pack complet {pack['title']} à la place des {len(manquants)} épisode(s) manquant(s)"
    elif recherche:
        resume += f", recherche de {len(recherche)} épisode(s) manquant(s)"
    elif manquants:
        resume += (f", {len(manquants)} épisodes manquants : trop pour une recherche "
                   "automatique, à lancer depuis Sonarr")
    if note:
        resume += f" ({note})"
    elif pack is not None and not remplace:
        resume += f" (règle 4 en simulation : remplacerait par le pack complet {pack['title']})"
    if note_accord:
        resume += f" ({note_accord})"
    verdict.update(decision="seed", category=categorie, missing_episodes=len(manquants),
                   search=bool(recherche), pack=pack["title"] if remplace else None)

    if dry_run:
        if deja_vu:
            return verdict, False
        _assist_record(dl, arr, titre, "seed", resume, True, 1)
        _assist_log(1, "✓", arr, titre, resume, True)
        return verdict, True

    with qbit_session() as s:
        r = s.post(f"{QBIT_URL}/api/v2/torrents/setCategory",
                   data={"hashes": dl, "category": categorie}, timeout=10)
        r.raise_for_status()
    # Tracé dès que le torrent a changé de catégorie : il a quitté la file, le
    # prochain passage ne le reverra pas.
    _assist_record(dl, arr, titre, "seed", resume, False, 1)
    _assist_log(1, "✓", arr, titre, resume, False)
    if recherche:
        try:
            _arr_request("POST", url, cle, "command", json={"name": "EpisodeSearch", "episodeIds": recherche})
        except Exception as e:
            verdict["search"] = False
            _assist_record(dl, arr, titre, "search-failed",
                           f"recherche de {len(recherche)} épisode(s) non lancée : {e}", False, 1)
            log.error("  ⚠ [%s] [%s] %s : recherche des épisodes manquants non lancée (%s), "
                      "à lancer depuis Sonarr", _regle(1), arr, titre, e)
    return verdict, True


def import_assist_loop() -> None:
    """
    Passage de renfort de l'assistant d'import, pour les events manqués : service
    arrêté, webhook non coché, *arr redémarré. Un seul par process (cf. sync_loop).
    Le premier passage, deux minutes après le démarrage, rattrape les blocages
    survenus pendant un arrêt.
    """
    if IMPORT_ASSIST_INTERVAL <= 0 or not _assist_arrs() or _assist_started.is_set():
        return
    _assist_started.set()
    log.info("Assistant d'import : %s", _assist_modes())
    # Laisse la première sync démarrer avant de solliciter qBit et les *arr.
    time.sleep(120)
    while True:
        try:
            if SONARR_URL and SONARR_API_KEY:
                _pack_recover()
            run_import_assist(source="passage de renfort")
        except Exception as e:
            log.error("Erreur assistant d'import : %s", e)
        time.sleep(IMPORT_ASSIST_INTERVAL)


# ── Assistant d'import, règle 4 : packs de saison partiels ──────────────────
#
# Sonarr prend tout « S01 » pour la saison entière. Un pack qui n'en contient
# qu'une partie (une moitié de saison TVDB, une « Part 1 ») passe donc pour
# complet, et un vrai pack complet est ensuite refusé : il n'améliore pas les
# épisodes déjà là. La règle lit la liste des fichiers dès le grab ; s'il manque
# des épisodes, le torrent est arrêté avant d'avoir téléchargé et remplacé par un
# pack complet trouvé dans Prowlarr. Après import, elle sert de filet.

# Un torrent privé déjà entamé n'est jamais arrêté (hit-and-run) : au-delà de
# cette part téléchargée, on le laisse finir et la règle reprend après l'import.
_PACK_STOP_MAX_PROGRESS = 0.02
# Un magnet n'a pas encore sa liste de fichiers au grab : délai d'attente.
_PACK_METADATA_WAIT = 120
# Sources minimum d'un pack candidat.
_PACK_MIN_SEEDERS = 3
# .torrent lus au plus par recherche : chaque lecture passe par un tracker.
_PACK_MAX_TORRENTS = 4
# Remplacements réels au plus par saison sur la période : au-delà, la saison
# tourne en rond (numérotation, releases mal nommées) et revient à la main.
_PACK_MAX_ATTEMPTS = 2
_PACK_ATTEMPTS_WINDOW = 7 * 24 * 3600
# Au-delà, un pack n'est pas analysé : chaque vidéo coûte un /parse à Sonarr.
_PACK_MAX_FILES = 300
_PACK_TORRENT_MAX_BYTES = 20 * 1024 * 1024
# Un torrent arrêté par une vérification restée sans verdict (service redémarré
# pendant la recherche) est relancé par le passage de renfort, passé ce délai.
_PACK_RECOVER_AFTER = 1800
# Recherches Prowlarr simultanées par process : l'arrêt du torrent, lui, n'attend pas.
_pack_slots = threading.BoundedSemaphore(2)


def schedule_pack_check(data: dict[str, Any]) -> bool:
    """
    Lance la vérification d'un grab Sonarr de pack de saison, en arrière-plan.
    False si le grab n'en est pas un : film, épisode seul, plusieurs saisons.
    """
    serie = data.get("series") if isinstance(data.get("series"), dict) else {}
    release = data.get("release") if isinstance(data.get("release"), dict) else {}
    episodes = [e for e in data.get("episodes") or [] if isinstance(e, dict)]
    saisons = {e.get("seasonNumber") for e in episodes}
    titre = release.get("releaseTitle") or ""
    dl = (data.get("downloadId") or "").lower().strip()
    if not (SONARR_URL and SONARR_API_KEY) or not serie.get("id") or not dl or not titre:
        return False
    if len(episodes) < 2 or len(saisons) != 1 or None in saisons:
        return False
    if (data.get("downloadClientType") or "").lower() != "qbittorrent":
        return False
    threading.Thread(target=_run_pack_check, args=(serie["id"], saisons.pop(), dl, titre), daemon=True).start()
    return True


def _run_pack_check(serie_id: int, saison: int, dl: str, titre: str) -> None:
    try:
        check_pack_grab(serie_id, saison, dl, titre)
    except Exception as e:
        log.error("  ⚠ [%s] [sonarr] %s : vérification en erreur (%s)", _regle(4), titre, e)
        _assist_record(dl, "sonarr", titre, "pack-error", str(e), IMPORT_ASSIST_PACK_DRY_RUN, 4)


def check_pack_grab(serie_id: int, saison: int, dl: str, titre: str,
                    dry_run: bool | None = None) -> dict[str, Any]:
    """
    Règle 4, au grab : le pack apporte-t-il tous les épisodes attendus de la saison ?

    La liste des fichiers vient de qBittorrent, qui a le .torrent dès le grab ;
    chaque vidéo passe par le parseur de Sonarr, celui qui servira à l'import.
    S'il manque des épisodes, le torrent est arrêté tout de suite, puis remplacé
    par un pack complet trouvé dans Prowlarr : le complet est ajouté d'abord, puis
    le partiel est blacklisté (sans suppression ni nouvelle recherche) et part en
    corbeille, arrêté. Sans pack complet, le partiel repart : mieux vaut une
    demi-saison que rien.
    """
    dry_run = IMPORT_ASSIST_PACK_DRY_RUN if dry_run is None else dry_run
    dl = dl.lower()
    url, cle = SONARR_URL, SONARR_API_KEY
    verdict: dict[str, Any] = {"arr": "sonarr", "download_id": dl, "title": titre, "stage": "grab"}

    def fin(decision: str, details: str, signe: str = "⏭") -> dict[str, Any]:
        verdict.update(decision=decision, reason=details)
        _assist_record(dl, "sonarr", titre, decision, details, dry_run, 4)
        _assist_log(4, signe, "sonarr", titre, details, dry_run)
        return verdict

    if saison == 0:
        return fin("pack-skipped", "saison 0 (épisodes spéciaux) : non traitée")
    serie = _arr_request("GET", url, cle, f"series/{serie_id}") or {}
    profil = _quality_profile(url, cle, serie.get("qualityProfileId"))
    ref = _release_ref(url, cle, titre, profil)
    if not ref["season_pack"]:
        return fin("pack-skipped", "pas un pack de saison")
    t, fichiers = _qbit_torrent_files(dl, _PACK_METADATA_WAIT)
    if t is None:
        return fin("pack-skipped", "absent de qBittorrent")
    if fichiers is None:
        return fin("pack-skipped", "liste des fichiers inconnue (magnet sans métadonnées) : "
                                   "la règle reprendra après l'import")
    noms = _video_names(fichiers)
    if len(noms) > _PACK_MAX_FILES:
        return fin("pack-skipped", f"{len(noms)} vidéos : trop pour être analysées")
    episodes = [e for e in _arr_request("GET", url, cle, "episode", params={"seriesId": serie_id}) or []
                if e.get("seasonNumber") == saison]
    voulus = _episodes_wanted(episodes)
    if not voulus:
        return fin("pack-ok", "aucun épisode manquant dans la saison", "✓")
    couverts = _pack_coverage(url, cle, serie_id, saison, episodes, noms) & voulus
    if couverts == voulus:
        return fin("pack-ok", f"le pack apporte les {len(voulus)} épisodes attendus", "✓")
    resume = f"pack partiel : {len(couverts)}/{len(voulus)} épisodes attendus de la saison {saison}"
    if _pack_attempts(serie_id, saison) >= _PACK_MAX_ATTEMPTS:
        return fin("pack-partial", f"{resume}, limite de remplacements atteinte pour cette saison : "
                                   "à voir à la main", "⚠")
    if not (PROWLARR_URL and PROWLARR_API_KEY):
        # Sans recherche possible, l'arrêter ne servirait à rien.
        return fin("pack-partial", f"{resume}, Prowlarr non configuré : rien à remplacer", "⚠")

    arrete = False
    if not dry_run:
        # Arrêté d'abord, mesuré ensuite : la part téléchargée n'augmente plus
        # pendant qu'on la lit.
        _qbit_post("stop", {"hashes": dl})
        progres = (_qbit_info(dl) or {}).get("progress") or 0
        if t.get("private") and progres > _PACK_STOP_MAX_PROGRESS:
            _qbit_post("start", {"hashes": dl})
            return fin("pack-partial", f"{resume}, déjà téléchargé à {progres:.0%} sur un tracker privé : "
                                       "on le laisse finir, la règle reprendra après l'import", "⚠")
        arrete = True
        # Tracé tout de suite : si le service redémarre pendant la recherche, le
        # passage de renfort relance le torrent.
        _assist_record(dl, "sonarr", titre, "pack-stopped", resume, False, 4)
    try:
        with _pack_slots:
            cand, ecartes = _pack_candidate(url, cle, serie, saison, episodes, voulus, ref, profil, {dl})
        if cand is None:
            if arrete:
                _qbit_post("start", {"hashes": dl})
            return fin("pack-partial", f"{resume}, aucun pack complet trouvé{_ecarts(ecartes)} : on le garde", "⚠")
        details = (f"{resume} → remplacé par {cand['title']} ({cand['indexer']}, "
                   f"{cand['seeders']} sources, score {cand['score']})")
        verdict["replacement"] = cand["title"]
        if dry_run:
            return fin("pack-replace", details, "✓")
        _qbit_add(cand, t.get("category") or "", t)
    except Exception:
        if arrete:
            _qbit_restart(dl)
        raise

    # Le pack complet est en place : le partiel peut partir.
    _pack_register(serie_id, saison, dl, cand, "grab", voulus)
    try:
        if not _sonarr_blocklist(url, cle, serie_id, dl):
            details += " (pas blacklisté : absent de la file Sonarr)"
    except Exception as e:
        details += f" (blacklist Sonarr impossible : {e})"
        log.error("  ⚠ [%s] [sonarr] %s : blacklist impossible (%s)", _regle(4), titre, e)
    if not _pack_trash(t):
        details += " (laissé arrêté dans sa catégorie : son dossier est aussi celui d'un autre torrent)"
    return fin("pack-replace", details, "✓")


def _pack_after_import(
    url: str, cle: str, serie_id: int, dl: str, titre: str, records: list[dict[str, Any]],
    episodes: list[dict[str, Any]], manquants: list[int], videos: list[str],
    clients: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, str]:
    """
    Règle 4, filet après import : le pack de saison est importé mais n'a pas tout
    apporté — pris pendant un arrêt du service, magnet sans métadonnées au grab,
    torrent privé trop entamé pour être arrêté. Cherche un pack complet qui
    remplacera ses épisodes et apportera les manquants.
    Retourne (pack à ajouter, ou None ; note pour le verdict).
    """
    deja = _pack_replacement(dl, "old_hash")
    if deja:
        # Ajouté par un passage précédent, qui n'a pas pu sortir le partiel de la file.
        return {"title": deja["new_title"], "deja": True}, ""
    saisons = {e.get("seasonNumber") for e in episodes if e["id"] in manquants}
    if len(saisons) != 1 or 0 in saisons:
        return None, ""
    saison = saisons.pop()
    serie = _arr_request("GET", url, cle, f"series/{serie_id}") or {}
    profil = _quality_profile(url, cle, serie.get("qualityProfileId"))
    ref = _release_ref(url, cle, titre, profil)
    if not ref["season_pack"] or ref["season"] != saison:
        return None, ""
    if _pack_attempts(serie_id, saison) >= _PACK_MAX_ATTEMPTS:
        return None, "règle 4 : limite de remplacements atteinte pour cette saison"
    categorie = _client_field(clients, records[0].get("downloadClient") or "", ("tvCategory",))
    if not categorie:
        return None, "règle 4 : pas de catégorie de téléchargement sur le client"

    de_la_saison = [e for e in episodes if e.get("seasonNumber") == saison]
    # Épisodes que ce pack a apportés : leur fichier est un hardlink d'une de ses vidéos.
    inodes: set[int] = set()
    for v in videos:
        try:
            inodes.add(os.stat(v).st_ino)
        except OSError:
            continue
    chemins = {f["id"]: f.get("path") for f in
               _arr_request("GET", url, cle, "episodefile", params={"seriesId": serie_id}) or []}
    apportes: set[int] = set()
    for e in de_la_saison:
        chemin = chemins.get(e.get("episodeFileId"))
        try:
            if chemin and os.stat(chemin).st_ino in inodes:
                apportes.add(e["id"])
        except OSError:
            continue
    a_couvrir = apportes | set(manquants)
    with _pack_slots:
        cand, ecartes = _pack_candidate(url, cle, serie, saison, de_la_saison, a_couvrir, ref, profil, {dl})
    if cand is None:
        return None, f"règle 4 : aucun pack complet trouvé{_ecarts(ecartes)}"
    cand.update(season=saison, episodes=a_couvrir, category=categorie)
    return cand, ""


def _pack_finish(
    url: str, cle: str, dl: str, titre: str, inodes_media: set[int], remplacement: sqlite3.Row,
    ctx: dict[str, Any], verdict: dict[str, Any], ecarte: Any,
) -> tuple[dict[str, Any], bool]:
    """
    Règle 4, fin du remplacement. Le pack complet est téléchargé : Sonarr a importé
    seul les épisodes manquants, mais refuse de remplacer ceux du pack partiel,
    qu'il n'améliore pas (même score). On les importe à la main, en hardlink, à la
    place des fichiers du pack partiel — de ceux-là seulement : un épisode remplacé
    entre-temps par autre chose n'est pas touché. Le pack partiel, qui n'a plus de
    fichier dans la bibliothèque, part ensuite en corbeille par l'event d'upgrade.
    Suit la simulation de la règle 4 seule, sauf aperçu.
    """
    dry_run = ctx["apercu"] or IMPORT_ASSIST_PACK_DRY_RUN
    if not dry_run and _assist_seen(dl, "pack-reimport", False):
        return ecarte("remplacement déjà lancé dans les dernières 24 h, toujours dans la file", 4, dry_run)
    serie_id = remplacement["series_id"]
    ancien = ctx["torrents"].get(remplacement["old_hash"])
    inodes_ancien: set[int] = set()
    for v in (_torrent_videos(ancien) or []) if ancien else []:
        try:
            inodes_ancien.add(os.stat(v).st_ino)
        except OSError:
            continue
    if not inodes_ancien:
        # Retiré de qBittorrent, ou arrêté au grab avant d'avoir rien téléchargé.
        return ecarte("aucun fichier du pack partiel à remplacer, import à finir à la main", 4, dry_run)

    prevus = set(json.loads(remplacement["episodes"] or "[]"))
    chemins = {f["id"]: f.get("path") for f in
               _arr_request("GET", url, cle, "episodefile", params={"seriesId": serie_id}) or []}
    fichier_de = {e["id"]: chemins.get(e.get("episodeFileId"))
                  for e in _arr_request("GET", url, cle, "episode", params={"seriesId": serie_id}) or []}

    def du_partiel(episode_id: int) -> bool:
        chemin = fichier_de.get(episode_id)
        if not chemin:
            return True  # sans fichier : rien à écraser
        try:
            return os.stat(chemin).st_ino in inodes_ancien
        except OSError:
            return False

    entrees: list[dict[str, Any]] = []
    propositions = _arr_request("GET", url, cle, "manualimport", timeout=300,
                                params={"downloadId": dl.upper(), "filterExistingFiles": "false"}) or []
    for it in propositions:
        ids = [e["id"] for e in it.get("episodes") or []]
        if (not ids or not it.get("quality") or (it.get("series") or {}).get("id") != serie_id
                or not set(ids) <= prevus or not all(du_partiel(i) for i in ids)):
            continue
        try:
            if os.stat(it["path"]).st_ino in inodes_media:
                continue  # déjà importé
        except OSError:
            continue
        entrees.append({
            "path": it["path"], "seriesId": serie_id, "episodeIds": ids, "quality": it["quality"],
            "languages": it.get("languages") or [], "releaseGroup": it.get("releaseGroup") or "",
            "indexerFlags": it.get("indexerFlags") or 0, "releaseType": it.get("releaseType") or "singleEpisode",
            "downloadId": dl.upper(),
        })
    if not entrees:
        return ecarte("rien à remplacer, les épisodes restants ne viennent pas du pack partiel", 4, dry_run)

    resume = f"pack complet de remplacement : {len(entrees)} vidéo(s) importée(s) à la place du pack partiel"
    verdict.update(decision="pack-reimport", files=len(entrees))
    if dry_run:
        if _assist_seen(dl, "pack-reimport", True):
            return verdict, False
        _assist_record(dl, "sonarr", titre, "pack-reimport", resume, True, 4)
        _assist_log(4, "✓", "sonarr", titre, resume, True)
        return verdict, True
    # « copy » : hardlink depuis le torrent, qui continue de seeder.
    _arr_request("POST", url, cle, "command", json={"name": "ManualImport", "importMode": "copy", "files": entrees})
    _assist_record(dl, "sonarr", titre, "pack-reimport", resume, False, 4)
    _assist_log(4, "✓", "sonarr", titre, resume, False)
    return verdict, True


def _pack_recover() -> int:
    """
    Relance les packs arrêtés par une vérification restée sans verdict — service
    redémarré pendant la recherche. Sans ça, ils resteraient arrêtés dans la file
    de Sonarr. Retourne le nombre de torrents relancés.
    """
    maintenant = int(time.time())
    with _db_lock:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT a.download_id, a.title FROM assist_log a "
                "WHERE a.decision = 'pack-stopped' AND a.dry_run = 0 AND a.at BETWEEN ? AND ? "
                "AND NOT EXISTS (SELECT 1 FROM assist_log b WHERE b.download_id = a.download_id "
                "AND b.id > a.id AND b.decision LIKE 'pack-%')",
                (maintenant - 2 * 86400, maintenant - _PACK_RECOVER_AFTER),
            ).fetchall()
    relances = 0
    for row in rows:
        dl, titre = row["download_id"], row["title"]
        try:
            t = _qbit_info(dl)
            if t and t.get("category") != TARGET_CAT and t.get("state") in ("stoppedDL", "pausedDL"):
                _qbit_post("start", {"hashes": dl})
                relances += 1
                details = "vérification interrompue : torrent relancé"
            else:
                details = "vérification interrompue : torrent déjà reparti ou retiré"
            _assist_record(dl, "sonarr", titre, "pack-resumed", details, False, 4)
            _assist_log(4, "↻", "sonarr", titre, details, False)
        except Exception as e:
            log.error("  ⚠ [%s] [sonarr] %s : relance impossible (%s)", _regle(4), titre, e)
    return relances


def _episodes_wanted(episodes: list[dict[str, Any]]) -> set[int]:
    """Épisodes diffusés, surveillés et sans fichier : ceux que Sonarr attend."""
    maintenant = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    return {e["id"] for e in episodes if e.get("monitored") and not e.get("hasFile")
            and (e.get("airDateUtc") or "9999") <= maintenant}


def _video_names(fichiers: list[tuple[str, int]]) -> list[str]:
    """Vidéos d'une liste (chemin, taille), échantillons exclus — comme _torrent_videos, sans le disque."""
    return [nom for nom, taille in fichiers
            if os.path.splitext(nom)[1].lower() in VIDEO_EXTENSIONS
            and not (taille < _SAMPLE_MAX_BYTES and _SAMPLE_RE.search(nom))]


def _pack_coverage(url: str, cle: str, serie_id: int, saison: int,
                   episodes: list[dict[str, Any]], noms: list[str]) -> set[int]:
    """
    Épisodes de la saison qu'apportent ces vidéos, d'après le parseur de Sonarr.
    Seul le nom du fichier compte, comme à l'import : une vidéo qui ne porte pas
    son numéro ne couvre rien, même rangée dans un dossier « S01 ».
    """
    par_numero = {e["episodeNumber"]: e["id"] for e in episodes if e.get("seasonNumber") == saison}
    par_absolu = {e["absoluteEpisodeNumber"]: e["id"] for e in episodes
                  if e.get("seasonNumber") == saison and e.get("absoluteEpisodeNumber")}
    couverts: set[int] = set()
    for nom in noms:
        p = _arr_request("GET", url, cle, "parse", params={"title": os.path.basename(nom)}) or {}
        info = p.get("parsedEpisodeInfo") or {}
        if not info or info.get("fullSeason"):
            continue
        serie = p.get("series") or {}
        if serie:
            # Série reconnue : la correspondance de Sonarr fait foi, numérotation
            # de scène comprise. Une autre série ne couvre rien.
            if serie.get("id") == serie_id:
                couverts |= {e["id"] for e in p.get("episodes") or [] if e.get("seasonNumber") == saison}
        elif info.get("episodeNumbers"):
            if info.get("seasonNumber") == saison:
                couverts |= {par_numero[n] for n in info["episodeNumbers"] if n in par_numero}
        else:
            couverts |= {par_absolu[n] for n in info.get("absoluteEpisodeNumbers") or [] if n in par_absolu}
    return couverts


def _quality_profile(url: str, cle: str, profil_id: int | None) -> dict[str, Any]:
    """Qualités autorisées par le profil, et leur rang : l'ordre du profil, de la moins bonne à la meilleure."""
    profil = (_arr_request("GET", url, cle, f"qualityprofile/{profil_id}") or {}) if profil_id else {}
    autorisees: set[int] = set()
    rangs: dict[int, int] = {}
    for rang, item in enumerate(profil.get("items") or []):
        for membre in [item] if item.get("quality") else item.get("items") or []:
            qid = (membre.get("quality") or {}).get("id")
            if qid is None:
                continue
            rangs[qid] = rang
            if item.get("allowed"):
                autorisees.add(qid)
    return {"autorisees": autorisees, "rangs": rangs}


def _series_title_key(titre: str) -> str:
    """Titre de série tel que lu dans une release, comparable : sans casse, accents, ponctuation ni année."""
    t = unicodedata.normalize("NFKD", titre).encode("ascii", "ignore").decode().lower()
    t = re.sub(r"\b(19|20)\d{2}\b", " ", t)
    return re.sub(r"[^a-z0-9]+", "", t)


def _release_ref(url: str, cle: str, titre: str, profil: dict[str, Any]) -> dict[str, Any]:
    """Titre de série, saison, score de formats et rang de qualité d'une release, d'après le parseur de Sonarr."""
    p = _arr_request("GET", url, cle, "parse", params={"title": titre}) or {}
    info = p.get("parsedEpisodeInfo") or {}
    qid = ((info.get("quality") or {}).get("quality") or {}).get("id")
    return {
        "series_title": _series_title_key(info.get("seriesTitle") or ""),
        "season": info.get("seasonNumber"),
        "season_pack": bool(info.get("fullSeason") or info.get("isPartialSeason")) and not info.get("isMultiSeason"),
        "score": p.get("customFormatScore") or 0,
        "rang": profil["rangs"].get(qid, -1),
    }


def _pack_queries(serie: dict[str, Any], saison: int) -> list[str]:
    """Requêtes « Titre S01 » : titre de la série et titres alternatifs, deux au plus."""
    titres = [serie.get("title") or ""] + [
        a.get("title") or "" for a in serie.get("alternateTitles") or []
        if a.get("seasonNumber") in (None, -1, saison)
    ]
    requetes: list[str] = []
    for titre in titres:
        titre = re.sub(r"\(\d{4}\)", " ", titre)
        titre = re.sub(r"['’]", "", titre)
        titre = re.sub(r"[^\w]+", " ", titre).strip()
        requete = f"{titre} S{saison:02d}"
        if titre and requete.lower() not in {r.lower() for r in requetes}:
            requetes.append(requete)
    return requetes[:2]


def _pack_candidate(
    url: str, cle: str, serie: dict[str, Any], saison: int, episodes: list[dict[str, Any]],
    a_couvrir: set[int], ref: dict[str, Any], profil: dict[str, Any], exclus: set[str],
) -> tuple[dict[str, Any] | None, list[str]]:
    """
    Cherche dans Prowlarr un pack de la saison qui apporte tous les épisodes à
    couvrir, sans rien perdre : qualité autorisée par le profil et au moins aussi
    bonne, score de formats au moins égal (le français reste le français).

    Il doit aussi porter le même titre de série que le pack partiel. Une série
    d'anthologie rassemble sous une fiche TVDB des histoires que leurs releases
    nomment chacune « S01 » : « Monster: The Lizzie Borden Story » est la saison 4
    de « Monster » pour TVDB, mais Sonarr la range en saison 1, celle de Dahmer.
    Le « S01 complet » serait alors une autre histoire que celle demandée.

    Les mieux notés d'abord ; seuls leurs .torrent sont lus, la liste des
    fichiers faisant foi. Retourne (candidat, motifs d'écart).
    """
    if not (PROWLARR_URL and PROWLARR_API_KEY):
        return None, ["Prowlarr non configuré"]
    resultats: list[dict[str, Any]] = []
    for requete in _pack_queries(serie, saison):
        resultats += _prowlarr_search(requete)
    # Déjà dans qBittorrent, partiel compris : rien à ajouter.
    with qbit_session() as s:
        deja = exclus | {t["hash"].lower() for t in qbit_get_torrents(s)}

    candidats: list[tuple[int, int, int, dict[str, Any]]] = []
    vus: set[tuple[str, Any]] = set()
    autres_titres: set[str] = set()
    for r in resultats:
        titre = r.get("title") or ""
        cle_r = (titre.lower(), r.get("size"))
        if cle_r in vus:
            continue
        vus.add(cle_r)
        # Un magnet n'a pas de liste de fichiers à lire avant d'être ajouté.
        if r.get("protocol") != "torrent" or not r.get("downloadUrl"):
            continue
        if (r.get("seeders") or 0) < _PACK_MIN_SEEDERS:
            continue
        if (r.get("infoHash") or "").lower() in deja:
            continue
        if r.get("files") and r["files"] < len(a_couvrir):
            continue  # Prowlarr connaît le nombre de fichiers : trop peu
        p = _arr_request("GET", url, cle, "parse", params={"title": titre}) or {}
        info = p.get("parsedEpisodeInfo") or {}
        if ((p.get("series") or {}).get("id") != serie.get("id") or info.get("seasonNumber") != saison
                or not info.get("fullSeason") or info.get("isPartialSeason") or info.get("isMultiSeason")
                or info.get("isSeasonExtra") or info.get("special")):
            continue
        titre_serie = info.get("seriesTitle") or ""
        if ref["series_title"] and _series_title_key(titre_serie) != ref["series_title"]:
            autres_titres.add(titre_serie)
            continue
        qid = ((info.get("quality") or {}).get("quality") or {}).get("id")
        score, rang = p.get("customFormatScore") or 0, profil["rangs"].get(qid, -1)
        if qid not in profil["autorisees"] or score < ref["score"] or rang < ref["rang"]:
            continue
        candidats.append((score, rang, r.get("seeders") or 0, r))
    ecartes: list[str] = []
    if autres_titres:
        ecartes.append("sous un autre titre : " + ", ".join(sorted(autres_titres)[:3]))
    if not candidats:
        return None, ["aucun pack de la saison à la hauteur dans Prowlarr"] + ecartes

    candidats.sort(key=lambda c: (-c[0], -c[1], -c[2]))
    for score, _, seeders, r in candidats[:_PACK_MAX_TORRENTS]:
        titre = r.get("title") or ""
        try:
            contenu = _fetch_torrent(r["downloadUrl"])
            meta = _torrent_meta(contenu)
        except Exception as e:
            ecartes.append(f"{titre} : .torrent illisible ({e})")
            continue
        if meta["hash"] in deja:
            ecartes.append(f"{titre} : déjà dans qBittorrent")
            continue
        noms = _video_names(meta["files"])
        if len(noms) > _PACK_MAX_FILES:
            ecartes.append(f"{titre} : {len(noms)} vidéos, trop pour être analysées")
            continue
        couverts = _pack_coverage(url, cle, serie["id"], saison, episodes, noms) & a_couvrir
        if couverts == a_couvrir:
            return {"title": titre, "indexer": r.get("indexer") or "?", "seeders": seeders, "score": score,
                    "hash": meta["hash"], "torrent": contenu}, ecartes
        ecartes.append(f"{titre} : {len(couverts)}/{len(a_couvrir)} épisodes")
    return None, ecartes


def _ecarts(ecartes: list[str]) -> str:
    if not ecartes:
        return ""
    return " (" + " ; ".join(ecartes[:3]) + (" ; …" if len(ecartes) > 3 else "") + ")"


def _prowlarr_search(requete: str) -> list[dict[str, Any]]:
    """Recherche texte sur tous les indexeurs de Prowlarr. Rien n'est téléchargé."""
    r = requests.get(
        f"{PROWLARR_URL}/api/v1/search",
        params={"query": requete, "type": "search", "limit": 100},
        headers={"X-Api-Key": PROWLARR_API_KEY},
        timeout=300,
    )
    r.raise_for_status()
    return r.json() or []


def _fetch_torrent(lien: str) -> bytes:
    """
    Télécharge un .torrent par son lien Prowlarr. Le lien porte une clé d'API, et
    le tracker vers lequel il redirige souvent une clé personnelle : il n'apparaît
    dans aucun message, l'erreur ne garde que son type.
    """
    try:
        with requests.get(lien, timeout=60, stream=True) as r:
            if r.status_code != 200:
                raise ValueError(f"HTTP {r.status_code}")
            contenu = b""
            for bloc in r.iter_content(65536):
                contenu += bloc
                if len(contenu) > _PACK_TORRENT_MAX_BYTES:
                    raise ValueError("fichier trop gros")
            return contenu
    except requests.RequestException as e:
        # Un lien magnet (InvalidSchema) aboutit aussi ici.
        raise ValueError(type(e).__name__) from None


def _bdecode(data: bytes, i: int = 0) -> tuple[Any, int]:
    """Décode une valeur bencode à partir de la position i. Retourne (valeur, position suivante)."""
    c = data[i:i + 1]
    if c == b"i":
        fin = data.index(b"e", i)
        return int(data[i + 1:fin]), fin + 1
    if c in (b"l", b"d"):
        i += 1
        items: list[Any] = []
        while data[i:i + 1] != b"e":
            if i >= len(data):
                raise ValueError("bencode tronqué")
            v, i = _bdecode(data, i)
            items.append(v)
        return (items if c == b"l" else dict(zip(items[::2], items[1::2]))), i + 1
    if c.isdigit():
        sep = data.index(b":", i)
        n = int(data[i:sep])
        if sep + 1 + n > len(data):
            raise ValueError("bencode tronqué")
        return data[sep + 1:sep + 1 + n], sep + 1 + n
    raise ValueError("bencode invalide")


def _torrent_meta(contenu: bytes) -> dict[str, Any]:
    """Nom, fichiers (chemin, taille) et infohash v1 d'un .torrent. Les trackers n'en sortent jamais."""
    if contenu[:1] != b"d":
        raise ValueError("pas un .torrent")
    i, info, brut = 1, None, b""
    while contenu[i:i + 1] != b"e":
        if i >= len(contenu):
            raise ValueError("bencode tronqué")
        k, i = _bdecode(contenu, i)
        debut = i
        v, i = _bdecode(contenu, i)
        if k == b"info":
            info, brut = v, contenu[debut:i]
    if not isinstance(info, dict):
        raise ValueError("pas de dictionnaire info")
    nom = info.get(b"name", b"").decode("utf-8", "replace")
    if b"files" in info:
        fichiers = [("/".join(p.decode("utf-8", "replace") for p in f[b"path"]), f[b"length"])
                    for f in info[b"files"]]
    elif b"length" in info:
        fichiers = [(nom, info[b"length"])]
    else:
        raise ValueError("torrent v2 seul, non pris en charge")
    return {"name": nom, "files": fichiers, "hash": hashlib.sha1(brut).hexdigest()}


def _qbit_torrent_files(
    dl: str, attente: float
) -> tuple[dict[str, Any] | None, list[tuple[str, int]] | None]:
    """
    Le torrent et ses fichiers (chemin relatif, taille). Les fichiers valent None
    tant que qBittorrent n'a pas les métadonnées : un magnet les tient des pairs.
    """
    fin = time.time() + attente
    with qbit_session() as s:
        while True:
            r = s.get(f"{QBIT_URL}/api/v2/torrents/info", params={"hashes": dl}, timeout=10)
            r.raise_for_status()
            infos = r.json()
            t = infos[0] if infos else None
            if t is not None and t.get("has_metadata", True):
                r = s.get(f"{QBIT_URL}/api/v2/torrents/files", params={"hash": dl}, timeout=10)
                r.raise_for_status()
                fichiers = [(f.get("name") or "", f.get("size") or 0) for f in r.json() or []]
                if fichiers:
                    return t, fichiers
            if time.time() >= fin:
                return t, None
            time.sleep(5)


def _qbit_info(dl: str) -> dict[str, Any] | None:
    with qbit_session() as s:
        r = s.get(f"{QBIT_URL}/api/v2/torrents/info", params={"hashes": dl}, timeout=10)
        r.raise_for_status()
        infos = r.json()
    return infos[0] if infos else None


def _qbit_post(action: str, data: dict[str, str]) -> None:
    with qbit_session() as s:
        r = s.post(f"{QBIT_URL}/api/v2/torrents/{action}", data=data, timeout=10)
        r.raise_for_status()


def _qbit_restart(dl: str) -> None:
    """Relance un torrent arrêté par la règle, sans masquer l'erreur qui a interrompu la vérification."""
    try:
        _qbit_post("start", {"hashes": dl})
    except Exception as e:
        log.error("  ⚠ [%s] torrent %s : relance impossible (%s), à relancer depuis qBittorrent", _regle(4), dl, e)


def _qbit_add(cand: dict[str, Any], categorie: str, ref: dict[str, Any]) -> None:
    """
    Ajoute le pack complet dans la catégorie de téléchargement du client Sonarr,
    rangé comme le torrent qu'il remplace. Il porte le titre de sa release : Sonarr,
    qui ne l'a pas pris lui-même, le reconnaît par ce titre. Vérifie qu'il est là.
    """
    if not categorie:
        raise RuntimeError("catégorie de téléchargement inconnue")
    data = {"category": categorie, "rename": cand["title"], "tags": "cleanup-linker",
            "stopped": "false", "paused": "false"}
    if ref.get("auto_tmm"):
        data["autoTMM"] = "true"
    else:
        data.update(autoTMM="false", savepath=ref.get("save_path") or "")
    with qbit_session() as s:
        r = s.post(f"{QBIT_URL}/api/v2/torrents/add", data=data, timeout=30,
                   files={"torrents": (f"{cand['hash']}.torrent", cand["torrent"], "application/x-bittorrent")})
        r.raise_for_status()
        for _ in range(10):
            r = s.get(f"{QBIT_URL}/api/v2/torrents/info", params={"hashes": cand["hash"]}, timeout=10)
            r.raise_for_status()
            if r.json():
                return
            time.sleep(1)
    raise RuntimeError("qBittorrent n'a pas ajouté le torrent")


def _sonarr_blocklist(url: str, cle: str, serie_id: int, dl: str) -> bool:
    """
    Sort le téléchargement de la file Sonarr en le blacklistant : ni suppression
    dans le client, ni nouvelle recherche — le pack complet est déjà là.
    Retourne False s'il n'apparaît pas dans la file.
    """
    for essai in range(3):
        if essai:
            time.sleep(10)  # Sonarr rafraîchit sa file quelques secondes après le grab
        ids = [q["id"] for q in _arr_request("GET", url, cle, "queue/details", params={"seriesId": serie_id}) or []
               if (q.get("downloadId") or "").lower() == dl]
        if ids:
            _arr_request("DELETE", url, cle, f"queue/{ids[0]}",
                         params={"removeFromClient": "false", "blocklist": "true", "skipRedownload": "true"})
            return True
    return False


def _pack_trash(t: dict[str, Any]) -> bool:
    """
    Le pack partiel part en corbeille, arrêté : à peine entamé, il n'a rien à seeder.
    Sauf s'il partage son dossier avec un autre torrent — un pack complet publié
    sous le même nom : ATM emporterait les fichiers de l'autre dans le dossier de
    la corbeille. Il reste alors arrêté dans sa catégorie. True s'il est parti.
    """
    with qbit_session() as s:
        partage = _shared_content({t["hash"]}, qbit_get_torrents(s))
        actions = [("stop", {"hashes": t["hash"]})]
        if not partage:
            actions.insert(0, ("setCategory", {"hashes": t["hash"], "category": TARGET_CAT}))
        for action, data in actions:
            r = s.post(f"{QBIT_URL}/api/v2/torrents/{action}", data=data, timeout=10)
            r.raise_for_status()
    if partage:
        return False
    log_move(t["hash"], t.get("name", ""), t.get("category", ""), "assist")
    return True


def _pack_register(serie_id: int, saison: int, ancien: str, cand: dict[str, Any],
                   etape: str, episodes: set[int]) -> None:
    with _db_lock:
        with get_db() as conn:
            conn.execute(
                "INSERT INTO pack_replace (series_id, season, old_hash, new_hash, new_title, stage, episodes) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (serie_id, saison, ancien, cand["hash"], cand["title"], etape, json.dumps(sorted(episodes))),
            )


def _pack_attempts(serie_id: int, saison: int) -> int:
    with _db_lock:
        with get_db() as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM pack_replace WHERE series_id = ? AND season = ? AND at > ?",
                (serie_id, saison, int(time.time()) - _PACK_ATTEMPTS_WINDOW),
            ).fetchone()[0]


def _pack_replacement(h: str, colonne: str = "new_hash") -> sqlite3.Row | None:
    """Dernier remplacement où ce torrent est le pack complet (new_hash) ou le partiel (old_hash)."""
    if colonne not in ("new_hash", "old_hash"):
        raise ValueError(colonne)
    with _db_lock:
        with get_db() as conn:
            return conn.execute(
                f"SELECT * FROM pack_replace WHERE {colonne} = ? ORDER BY id DESC LIMIT 1", (h.lower(),)
            ).fetchone()


# ── Assistant d'import, règle 5 : accorder la demande Seerr et la saison Sonarr ─
#
# Seerr demande des saisons numérotées comme TMDB, Sonarr les range comme TVDB,
# et les deux ne s'accordent pas toujours. « Monster: The Lizzie Borden Story »
# est une mini-série d'une saison pour TMDB, mais la saison 4 de « Monster »
# pour TVDB : Seerr a demandé la saison 1, celle de Dahmer, et Sonarr y a rangé
# les 8 épisodes de Lizzie Borden. C'est la demande qui tranche. La saison TVDB
# qui lui correspond se reconnaît à ses épisodes : mêmes dates de diffusion,
# mêmes titres. La règle surveille alors la bonne saison, cesse de surveiller
# l'autre, et réaffecte les fichiers déjà rangés au mauvais endroit — sur place,
# puis par un renommage : rien n'est supprimé, le torrent reste lié.

# Demandes examinées : celles des derniers jours.
_ACCORD_WINDOW_DAYS = 30
_ACCORD_PAGE = 50
# Une saison correspond si toutes ses dates coïncident, ou au moins cette part de
# ses titres : les épisodes à venir n'ont pas encore de date.
_ACCORD_TITLES_MIN = 0.8
# Attente maximale de l'import manuel qui réaffecte les fichiers, avant le renommage.
_ACCORD_COMMAND_WAIT = 120


def _seerr_get(route: str, **params: Any) -> Any:
    r = requests.get(f"{SEERR_URL}/api/v1/{route}", params=params or None,
                     headers={"X-Api-Key": SEERR_API_KEY}, timeout=60)
    r.raise_for_status()
    return r.json()


def _seerr_tv_requests(depuis: float) -> list[dict[str, Any]]:
    """Demandes de séries approuvées ou terminées depuis cette date, les plus récentes d'abord."""
    demandes: list[dict[str, Any]] = []
    skip = 0
    while True:
        page = _seerr_get("request", take=_ACCORD_PAGE, skip=skip, filter="all", sort="added") or {}
        lot = page.get("results") or []
        for d in lot:
            cree = datetime.strptime((d.get("createdAt") or "1970-01-01")[:19], "%Y-%m-%dT%H:%M:%S")
            if (cree - datetime(1970, 1, 1)).total_seconds() < depuis:
                return demandes
            media = d.get("media") or {}
            if d.get("type") == "tv" and d.get("status") in (2, 5) and media.get("tmdbId") and media.get("tvdbId"):
                demandes.append(d)
        skip += len(lot)
        if not lot or skip >= ((page.get("pageInfo") or {}).get("results") or 0):
            return demandes


def _episode_title_key(titre: str) -> str:
    t = unicodedata.normalize("NFKD", titre or "").encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "", t)


def _season_scores(tmdb_eps: list[dict[str, Any]], tvdb_eps: list[dict[str, Any]]) -> tuple[int, int] | None:
    """(dates identiques, titres identiques), épisode par épisode dans l'ordre. None si les nombres diffèrent."""
    if not tmdb_eps or len(tmdb_eps) != len(tvdb_eps):
        return None
    dates = titres = 0
    for a, b in zip(tmdb_eps, tvdb_eps):
        dates += bool(a.get("airDate")) and a.get("airDate") == b.get("airDate")
        cle = _episode_title_key(a.get("name") or "")
        titres += bool(cle) and cle == _episode_title_key(b.get("title") or "")
    return dates, titres


def _season_for_request(tmdb_eps: list[dict[str, Any]], par_saison: dict[int, list[dict[str, Any]]],
                        demandee: int) -> tuple[int | None, str]:
    """
    Saison TVDB qui correspond à la saison TMDB demandée : la sienne si elle colle,
    sinon la seule autre qui colle. None si aucune ne colle, ou plusieurs.
    """
    n = len(tmdb_eps)

    def colle(saison: int) -> tuple[int, int] | None:
        scores = _season_scores(tmdb_eps, par_saison.get(saison) or [])
        if scores and (scores[0] == n or scores[1] >= max(1, round(n * _ACCORD_TITLES_MIN))):
            return scores
        return None

    if not tmdb_eps:
        return None, "saison sans épisode connu de TMDB"
    if colle(demandee):
        return demandee, ""
    autres = [(s, sc) for s in sorted(par_saison) if s != demandee for sc in [colle(s)] if sc]
    if len(autres) == 1:
        s, (dates, titres) = autres[0]
        return s, f"{dates}/{n} dates et {titres}/{n} titres identiques"
    if autres:
        return None, "plusieurs saisons TVDB correspondent : " + ", ".join(f"S{s:02d}" for s, _ in autres)
    return None, f"aucune saison TVDB de {n} épisode{'s' if n > 1 else ''} ne correspond"


def run_accord(apercu: bool) -> tuple[list[dict[str, Any]], dict[int, set[int]]]:
    """
    Règle 5, sur les demandes Seerr récentes. Elle suit sa propre simulation, sauf
    aperçu (`apercu`), qui simule tout. Retourne (verdicts, saisons écartées
    par série Sonarr) : la règle 1 ne compte plus les épisodes de ces saisons parmi
    les manquants, même en simulation.
    """
    if not (SEERR_URL and SEERR_API_KEY and SONARR_URL and SONARR_API_KEY):
        return [], {}
    dry = apercu or IMPORT_ASSIST_ACCORD_DRY_RUN
    url, cle = SONARR_URL, SONARR_API_KEY
    try:
        demandes = _seerr_tv_requests(time.time() - _ACCORD_WINDOW_DAYS * 86400)
    except Exception as e:
        log.error("  ⚠ [%s] Seerr injoignable ou réponse inattendue (%s)", _regle(5), e)
        return [], {}
    # Plusieurs fiches Seerr peuvent viser la même série TVDB : Monster et Lizzie Borden.
    par_tvdb: dict[int, list[dict[str, Any]]] = {}
    for d in demandes:
        par_tvdb.setdefault(d["media"]["tvdbId"], []).append(d)

    verdicts: list[dict[str, Any]] = []
    ecartees: dict[int, set[int]] = {}
    for tvdb, liste in par_tvdb.items():
        try:
            verdicts += _accord_series(url, cle, tvdb, liste, dry, ecartees)
        except Exception as e:
            log.error("  ⚠ [%s] [sonarr] série TVDB %s en erreur (%s)", _regle(5), tvdb, e)
            verdicts.append({"arr": "sonarr", "download_id": f"tvdb-{tvdb}", "title": f"série TVDB {tvdb}",
                             "decision": "error", "reason": str(e)})
    return verdicts, ecartees


def _accord_series(url: str, cle: str, tvdb: int, demandes: list[dict[str, Any]], dry: bool,
                   ecartees: dict[int, set[int]]) -> list[dict[str, Any]]:
    """Accorde les demandes d'une même série TVDB. Les saisons écartées s'ajoutent à `ecartees`."""
    series = _arr_request("GET", url, cle, "series", params={"tvdbId": tvdb}) or []
    if not series:
        return []
    serie = series[0]
    episodes = [e for e in _arr_request("GET", url, cle, "episode", params={"seriesId": serie["id"]}) or []
                if (e.get("seasonNumber") or 0) > 0]
    par_saison: dict[int, list[dict[str, Any]]] = {}
    for e in sorted(episodes, key=lambda e: (e["seasonNumber"], e["episodeNumber"])):
        par_saison.setdefault(e["seasonNumber"], []).append(e)

    # Toutes les correspondances d'abord : une saison qu'une autre demande vise
    # pour de bon reste surveillée.
    plan: list[tuple[dict[str, Any], int, int | None, str, str]] = []
    for d in demandes:
        tmdb = d["media"]["tmdbId"]
        titre_tmdb = (_seerr_get(f"tv/{tmdb}", language="en") or {}).get("name") or ""
        for s in d.get("seasons") or []:
            demandee = s.get("seasonNumber") or 0
            if demandee <= 0:
                continue
            tmdb_eps = (_seerr_get(f"tv/{tmdb}/season/{demandee}", language="en") or {}).get("episodes") or []
            cible, detail = _season_for_request(tmdb_eps, par_saison, demandee)
            plan.append((d, demandee, cible, detail, titre_tmdb))
    voulues = {cible for _, demandee, cible, _, _ in plan if cible == demandee}

    verdicts: list[dict[str, Any]] = []
    for d, demandee, cible, detail, titre_tmdb in plan:
        if cible == demandee:
            continue
        cle_journal = f"seerr-{d['id']}-s{demandee}"
        titre = f"{serie['title']} S{demandee:02d} (demande Seerr {d['id']}, {(d.get('requestedBy') or {}).get('displayName') or '?'})"
        verdict: dict[str, Any] = {"arr": "sonarr", "download_id": cle_journal, "title": titre}
        if cible is None:
            verdict.update(decision="accord-unknown", reason=detail)
            if not _assist_seen(cle_journal, "accord-unknown", dry):
                _assist_record(cle_journal, "sonarr", titre, "accord-unknown", detail, dry, 5)
                _assist_log(5, "⏭", "sonarr", titre, detail, dry)
            verdicts.append(verdict)
            continue
        resume, ecartee = _accord_apply(url, cle, serie, par_saison, demandee, cible, demandee not in voulues,
                                        titre_tmdb, dry, cle_journal)
        if ecartee:
            ecartees.setdefault(serie["id"], set()).add(demandee)
        if resume is None:
            continue  # déjà accordée
        verdict.update(decision="accord-fix", reason=f"saison {demandee} demandée = S{cible:02d} TVDB ({detail}) → {resume}",
                       season=cible)
        verdicts.append(verdict)
        if not _assist_seen(cle_journal, "accord-fix", dry):
            _assist_record(cle_journal, "sonarr", titre, "accord-fix", verdict["reason"], dry, 5)
            _assist_log(5, "✓", "sonarr", titre, verdict["reason"], dry)
    return verdicts


def _accord_apply(url: str, cle: str, serie: dict[str, Any], par_saison: dict[int, list[dict[str, Any]]],
                  demandee: int, cible: int, libre: bool, titre_tmdb: str, dry: bool,
                  cle_journal: str) -> tuple[str | None, bool]:
    """
    Surveille la saison TVDB qui correspond à la demande, y reprend les fichiers de
    la demande rangés dans la saison demandée, et cesse de surveiller celle-ci —
    seulement si aucune autre demande ne la vise (`libre`) et qu'elle n'a pas
    d'autres fichiers. Retourne (résumé, ou None s'il n'y a plus rien à faire ;
    saison demandée écartée).
    """
    saisons = {s["seasonNumber"]: s for s in serie.get("seasons") or []}
    surveiller = not (saisons.get(cible) or {}).get("monitored")

    # Fichiers de la demande rangés dans la saison demandée : leur release porte le
    # titre de la fiche demandée (« Monster The Lizzie Borden Story »), pas celui
    # d'une autre histoire de la même série.
    cible_par_numero = {e["episodeNumber"]: e for e in par_saison.get(cible) or []}
    fichiers = {f["id"]: f for f in _arr_request("GET", url, cle, "episodefile", params={"seriesId": serie["id"]}) or []}
    titre_cle = _series_title_key(titre_tmdb)
    a_reprendre: list[tuple[dict[str, Any], dict[str, Any]]] = []
    autres = 0
    bloque = ""
    for e in par_saison.get(demandee) or []:
        f = fichiers.get(e.get("episodeFileId"))
        if not f:
            continue
        nom = f.get("sceneName") or os.path.basename(f.get("relativePath") or "")
        info = (_arr_request("GET", url, cle, "parse", params={"title": nom}) or {}).get("parsedEpisodeInfo") or {}
        if not titre_cle or _series_title_key(info.get("seriesTitle") or "") != titre_cle:
            autres += 1
            continue
        dest = cible_par_numero.get(e["episodeNumber"])
        if dest is None or dest.get("episodeFileId"):
            bloque = f"S{cible:02d}E{e['episodeNumber']:02d} absent ou déjà pourvu"
        else:
            a_reprendre.append((f, dest))
    if bloque:
        a_reprendre = []  # tout ou rien : pas de saison à moitié réaffectée
    # Une saison qui garde des fichiers d'une autre release (une autre histoire,
    # demandée plus tôt) reste surveillée.
    ecartee = libre and not autres and not bloque
    ne_plus = ecartee and bool((saisons.get(demandee) or {}).get("monitored"))

    if not (surveiller or ne_plus or a_reprendre or bloque):
        return None, ecartee
    morceaux = []
    if surveiller:
        morceaux.append(f"S{cible:02d} surveillée")
    if ne_plus:
        morceaux.append(f"S{demandee:02d} plus surveillée")
    if a_reprendre:
        morceaux.append(f"{len(a_reprendre)} fichier(s) réaffecté(s) de S{demandee:02d} à S{cible:02d}")
    elif bloque:
        morceaux.append(f"fichiers laissés en S{demandee:02d} ({bloque})")
    resume = ", ".join(morceaux)
    if dry or _assist_seen(cle_journal, "accord-fix", False):
        return resume, ecartee

    if surveiller or ne_plus:
        a_jour = _arr_request("GET", url, cle, f"series/{serie['id']}") or {}
        for s in a_jour.get("seasons") or []:
            if s["seasonNumber"] == cible:
                s["monitored"] = True
            elif s["seasonNumber"] == demandee and ne_plus:
                s["monitored"] = False
        # Sonarr aligne la surveillance des épisodes sur celle de leur saison.
        _arr_request("PUT", url, cle, f"series/{serie['id']}", json=a_jour)
    if a_reprendre:
        # Sans downloadId, un fichier déjà dans le dossier de la série est réaffecté
        # sur place (ImportApprovedEpisodes, newDownload=false) : ni copie ni suppression.
        entrees = [{
            "path": f["path"], "seriesId": serie["id"], "episodeIds": [dest["id"]], "quality": f.get("quality"),
            "languages": f.get("languages") or [], "releaseGroup": f.get("releaseGroup") or "",
            "indexerFlags": f.get("indexerFlags") or 0, "releaseType": "singleEpisode",
        } for f, dest in a_reprendre]
        commande = _arr_request("POST", url, cle, "command", json={"name": "ManualImport", "importMode": "auto", "files": entrees})
        if not _arr_wait_command(url, cle, commande):
            return resume + " (import manuel non terminé : renommage à lancer depuis Sonarr)", ecartee
        # Le renommage range ensuite les fichiers dans le dossier de leur nouvelle saison.
        nouveaux = {e.get("episodeFileId") for e in _arr_request("GET", url, cle, "episode", params={"seriesId": serie["id"]}) or []
                    if e.get("seasonNumber") == cible and e.get("episodeFileId")}
        a_renommer = [r["episodeFileId"] for r in _arr_request("GET", url, cle, "rename", params={"seriesId": serie["id"]}) or []
                      if r.get("episodeFileId") in nouveaux]
        if a_renommer:
            _arr_request("POST", url, cle, "command", json={"name": "RenameFiles", "seriesId": serie["id"], "files": a_renommer})
    return resume, ecartee


def _arr_wait_command(url: str, cle: str, commande: dict[str, Any] | None) -> bool:
    """Attend la fin d'une commande *arr. True si elle s'est terminée sans erreur."""
    if not commande or not commande.get("id"):
        return False
    fin = time.time() + _ACCORD_COMMAND_WAIT
    while time.time() < fin:
        etat = (_arr_request("GET", url, cle, f"command/{commande['id']}") or {}).get("status")
        if etat in ("completed", "failed", "aborted", "cancelled", "orphaned"):
            return etat == "completed"
        time.sleep(2)
    return False


def _still_on_disk(event: str, path: str) -> bool:
    """
    Un fichier « supprimé » qui est toujours là n'a pas été supprimé : l'*arr l'a
    réaffecté à d'autres épisodes par un import manuel, sur place (raison
    « manualOverride », règle 5). Il supprime toujours le fichier avant d'émettre
    une vraie suppression. Déplacer son torrent le mettrait en corbeille à tort.
    """
    if not os.path.isfile(path):
        return False
    log.info("%s ignoré : %s est toujours sur le disque (fichier réaffecté)", event, path)
    return True


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
        if path and _still_on_disk(event, path):
            return jsonify({"status": "ignored", "reason": "file still on disk", "path": path})
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
        if path and _still_on_disk(event, path):
            return jsonify({"status": "ignored", "reason": "file still on disk", "path": path})
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
        # Le fichier importé est indexé tout de suite : une suppression qui
        # suivrait avant la prochaine sync doit pouvoir le retrouver.
        indexed = index_import(data)
        if not is_upgrade:
            log.info("Import simple : %d ligne(s) indexée(s)", indexed)
            return jsonify({"status": "indexed", "rows": indexed})

        log.info("Upgrade détecté (event Download)")
        moved = 0
        deleted_files = data.get("deletedFiles", [])
        for df in deleted_files:
            if isinstance(df, dict):
                path = df.get("path")
                if path:
                    log.info("Fichier remplacé: %s", path)
                    moved += move_torrents_for_path(path)

        return jsonify({"moved": moved, "event": "DownloadUpgrade", "rows": indexed})

    elif event == "Grab":
        # Règle 4 : un pack de saison vient d'être pris. Sa liste de fichiers est
        # vérifiée tout de suite, avant qu'il ait eu le temps de télécharger.
        lance = schedule_pack_check(data)
        return jsonify({"status": "pack check started" if lance else "ignored", "event": event})

    elif event == "ManualInteractionRequired":
        # L'*arr vient de bloquer un import : titre non reconnu, grab rattaché par
        # ID, ou épisodes attendus manquants. Passage de l'assistant sous deux minutes.
        programme = schedule_import_assist("webhook")
        return jsonify({"status": "assist scheduled" if programme else "assist already scheduled"})

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

    if not item_id:
        return jsonify({"error": "Champ 'ItemId' manquant"}), 400

    # Supprimer une série ou une saison n'émet qu'un seul event, sur le conteneur —
    # jamais un par épisode (vérifié en production). On remonte donc aux fichiers
    # via jellyfin_series_id / jellyfin_season_id.
    is_container = item_type in ("Series", "Season")

    paths = _jellyfin_paths_for_item(item_id, item_type)
    resolution = "index"

    if not paths and JELLYFIN_FALLBACK_MATCH:
        log.info("  Item %s absent de l'index, tentative par métadonnées", item_id)
        paths = _jellyfin_paths_by_metadata(
            data, JELLYFIN_FALLBACK_MAX_CONTAINER if is_container else JELLYFIN_FALLBACK_MAX
        )
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
    # les fichiers sont toujours là. On ne retient que les hardlinks réellement
    # supprimés — sur un conteneur, c'est ce qui distingue une suppression d'un
    # simple retrait de bibliothèque, qui laisserait tout en place.
    disparus = [p for p in paths if not os.path.exists(p)]
    if not disparus:
        log.info(
            "  ⏭ Ignoré : les %d chemin(s) sont encore présents sur le disque "
            "(retrait de bibliothèque ou refresh, pas une suppression)",
            len(paths),
        )
        return jsonify({"status": "ignored", "reason": "file still on disk"})

    # Un chemin disparu peut avoir seulement changé de nom : titre déplacé vers une
    # autre bibliothèque. Le fichier existe encore, il ne faut rien supprimer.
    deplaces = _moved_paths(disparus)
    if deplaces:
        disparus = [p for p in disparus if p not in deplaces]
        if not disparus:
            log.info(
                "  ⏭ Ignoré : les %d chemin(s) disparus ont été déplacés, pas supprimés "
                "(changement de bibliothèque)",
                len(deplaces),
            )
            return jsonify({"status": "ignored", "reason": "file moved"})
        log.info("  %d chemin(s) déplacés, pas supprimés : ignorés", len(deplaces))

    if len(disparus) < len(paths):
        log.info(
            "  %d/%d chemin(s) réellement supprimés, les autres sont conservés",
            len(disparus), len(paths),
        )

    # Traduction en action *arr quand le média y est géré : sans ça l'*arr croit
    # toujours détenir le fichier et le retélécharge. C'est lui qui émettra ensuite
    # son propre webhook, lequel déplace les torrents par le chemin de code habituel.
    if JELLYFIN_DELEGATE_TO_ARR:
        delegated = delegate_to_arr(disparus, item_type, dry_run)
        if delegated:
            return jsonify({
                "status": "delegated",
                "delegated_to": delegated,
                "deleted_paths": len(disparus),
                "resolution": resolution,
                "dry_run": dry_run,
            })
        log.info("  Média inconnu de Sonarr/Radarr — déplacement direct des torrents")

    # Un seul point d'entrée : la branche préfixe de move_torrents_for_path traite
    # tous les inodes d'un dossier en une passe, au lieu d'une session qBit par épisode.
    root = _common_root(disparus)

    if root:
        log.info("  Jellyfin ItemDeleted (%s) %s: %s", resolution, item_type or "?", root)
        moved = move_torrents_for_path(root, source="jellyfin", dry_run=dry_run)
    else:
        # Les chemins ne partagent pas de dossier commun assez profond : on traite
        # chacun séparément plutôt que de risquer un préfixe trop large.
        log.warning(
            "  Pas de dossier commun sûr pour %d chemins — traitement individuel",
            len(disparus),
        )
        moved = sum(
            move_torrents_for_path(p, source="jellyfin", dry_run=dry_run) for p in disparus
        )

    return jsonify({
        "moved": moved,
        "root": root or None,
        "deleted_paths": len(disparus),
        "resolution": resolution,
        "dry_run": dry_run,
    })


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

                # Étape 3 : étendre aux cross-seeds partageant les mêmes inodes.
                # Sans ça le cleanup ne déplace que le hash importé par l'*arr et
                # laisse ses cross-seeds derrière : ils hardlinkent le même fichier,
                # donc aucun espace n'est libéré.
                # Ils ne sont jamais dans arr_managed (cross-seed les ajoute lui-même),
                # d'où leur exclusion par l'INNER JOIN de l'étape 2.
                t3 = time.time()
                orphan_hashes = [t["torrent_hash"] for t in torrents]

                # Amorce sur deux sources : les orphelins qu'on vient de trouver, et
                # les torrents déjà dans la catégorie cible — leurs cross-seeds ont pu
                # rester derrière lors d'un move précédent, ou être ajoutés après coup.
                # Les clauses sont construites conditionnellement : avec une liste vide,
                # "NOT IN (NULL)" vaudrait NULL et écarterait toutes les lignes.
                params: list[Any] = [TARGET_CAT]
                exclusion = ""
                if orphan_hashes:
                    ph = ",".join("?" * len(orphan_hashes))
                    exclusion = f"AND torrent_hash NOT IN ({ph})"
                    params += orphan_hashes
                    amorce = f"(torrent_hash IN ({ph}) OR category = ?)"
                    params += [*orphan_hashes, TARGET_CAT]
                else:
                    amorce = "category = ?"
                    params.append(TARGET_CAT)

                voisins = conn.execute(
                    f"""
                    SELECT DISTINCT torrent_hash, torrent_name, category
                    FROM files
                    WHERE stale = 0
                      AND torrent_hash IS NOT NULL
                      AND category != ?
                      {exclusion}
                      AND inode IN (
                          SELECT DISTINCT inode FROM files
                          WHERE stale = 0 AND {amorce}
                      )
                    """,
                    params,
                ).fetchall()
                # Un torrent multi-fichiers peut partager un inode avec un
                # orphelin tout en gardant d'autres fichiers encore hardlinkés
                # (pack de saison cross-seedé avec un épisode isolé). media_hashes
                # les écarte — même garantie que _check_hardlinks_active côté webhook.
                crossseeds = [v for v in voisins if v["torrent_hash"] not in media_hashes]
                protégés = len(voisins) - len(crossseeds)
                if protégés:
                    log.info("  %d voisin(s) conservé(s) : hardlinks /data/Media actifs", protégés)
                torrents = list(torrents) + crossseeds
                log.info("Étape 3 (cross-seeds) : +%d en %.2fs",
                         len(crossseeds), time.time() - t3)

        # ── Étape 4 : délai de grâce ─────────────────────────────────────────
        # Un torrent que l'*arr vient de grab est déjà dans arr_managed — le
        # webhook a enregistré son downloadId — mais n'a pas encore de hardlink
        # tant que l'import n'a pas eu lieu. Il coche donc les deux critères
        # d'orphelin alors qu'il attend simplement d'être importé.
        if torrents and CLEANUP_GRACE_HOURS > 0:
            t4 = time.time()
            limite = time.time() - CLEANUP_GRACE_HOURS * 3600
            try:
                with qbit_session() as s:
                    infos = {x["hash"]: x for x in qbit_get_torrents(s)}
            except Exception as e:
                log.error("⚠️ Cleanup interrompu : impossible de dater les torrents (%s)", e)
                return

            def trop_recent(h: str) -> bool:
                x = infos.get(h)
                if not x:
                    return False
                quand = x.get("completion_on") or 0
                if quand <= 0:
                    quand = x.get("added_on") or 0
                return quand > limite

            gardes = [t for t in torrents if trop_recent(t["torrent_hash"])]
            if gardes:
                torrents = [t for t in torrents if not trop_recent(t["torrent_hash"])]
                log.info(
                    "Étape 4 (délai de grâce) : %d torrent(s) épargnés, terminés depuis "
                    "moins de %dh — import *arr probablement en attente",
                    len(gardes), CLEANUP_GRACE_HOURS,
                )
                for t in gardes[:10]:
                    log.info("    ⏳ %s", t["torrent_name"])
            log.info("Étape 4 (délai de grâce) : %.2fs", time.time() - t4)

        # ── Étape 5 : fichiers partagés ──────────────────────────────────────
        # Un orphelin peut occuper le même emplacement qu'un torrent qui, lui,
        # sert encore : le déplacer emporterait ses fichiers (_shared_content).
        if torrents:
            try:
                with qbit_session() as s:
                    partages = _shared_content({t["torrent_hash"] for t in torrents}, qbit_get_torrents(s))
            except Exception as e:
                log.error("⚠️ Cleanup interrompu : impossible de vérifier les fichiers partagés (%s)", e)
                return
            for t in torrents:
                autre = partages.get(t["torrent_hash"].lower())
                if autre:
                    log.info("  ⏭ [%s] %s ignoré (fichiers partagés avec [%s] %s)",
                             t["category"], t["torrent_name"], autre.get("category", "?"), autre.get("name", "?"))
            torrents = [t for t in torrents if t["torrent_hash"].lower() not in partages]

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
                r = s.post(
                    f"{QBIT_URL}/api/v2/torrents/setCategory",
                    data={"hashes": hashes, "category": TARGET_CAT},
                    timeout=10,
                )
                r.raise_for_status()
                for t in torrents:
                    log_move(t["torrent_hash"], t["torrent_name"], t["category"], "cleanup")
                log.info("=== Nettoyage terminé : %d torrents déplacés ===", len(torrents))
        except Exception as e:
            log.error("Erreur qBit lors du nettoyage: %s", e)

    threading.Thread(target=run_cleanup, daemon=True).start()
    return jsonify({"status": "cleanup started"})


# Un /cleanup trace tous ses moves juste après un seul setCategory : ses lignes de
# cleanup_log tombent dans la même poignée de secondes.
_RESTORE_WINDOW = 300


@app.route("/restore", methods=["POST"])
def restore_last_cleanup():
    """
    Remet dans leur catégorie d'origine les torrents déplacés par le dernier /cleanup.

    `since` (timestamp Unix) élargit à tous les moves tracés depuis cette date, quelle
    qu'en soit la source : webhook, jellyfin ou cleanup.

    Le bornage compte : sans lui, tout ce qui est dans la catégorie cible et figure au
    journal repartirait. Ça représente des mois de suppressions légitimes, plus les
    torrents qu'on y laisse seeder pour honorer les trackers.
    """
    token = request.args.get("token") or request.headers.get("X-Token")
    if not verify_token(token):
        return jsonify({"error": "Unauthorized"}), 401

    since_raw = request.args.get("since")
    try:
        since = int(since_raw) if since_raw else None
    except ValueError:
        return jsonify({"error": "since doit être un timestamp Unix"}), 400

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

                # Source 1 : cleanup_log, borné au dernier /cleanup ou à `since`
                with _db_lock:
                    with get_db() as conn:
                        journal_vide = conn.execute(
                            "SELECT COUNT(*) FROM cleanup_log"
                        ).fetchone()[0] == 0
                        if since is not None:
                            log.info("Moves tracés depuis %s, toutes sources", datetime.fromtimestamp(since))
                            log_rows = conn.execute(
                                "SELECT torrent_hash, torrent_name, original_cat FROM cleanup_log "
                                "WHERE original_cat != ? AND moved_at >= ? ORDER BY moved_at DESC",
                                (TARGET_CAT, since),
                            ).fetchall()
                        else:
                            dernier = conn.execute(
                                "SELECT MAX(moved_at) FROM cleanup_log WHERE source = 'cleanup'"
                            ).fetchone()[0]
                            if dernier is None:
                                log_rows = []
                                if not journal_vide:
                                    log.info("Aucun /cleanup au journal : rien à restaurer "
                                             "(passer since=<timestamp> pour les autres moves).")
                            else:
                                log.info("Dernier /cleanup : %s", datetime.fromtimestamp(dernier))
                                log_rows = conn.execute(
                                    "SELECT torrent_hash, torrent_name, original_cat FROM cleanup_log "
                                    "WHERE source = 'cleanup' AND original_cat != ? AND moved_at >= ? "
                                    "ORDER BY moved_at DESC",
                                    (TARGET_CAT, dernier - _RESTORE_WINDOW),
                                ).fetchall()

                hash_to_cat: dict[str, tuple[str, str]] = {}
                for r in log_rows:
                    if r["torrent_hash"] not in hash_to_cat:
                        hash_to_cat[r["torrent_hash"]] = (r["torrent_name"], r["original_cat"])

                # Source 2 : fallback sur la table files, pour une base sans journal
                if journal_vide:
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

                # Même garde-fou qu'à l'aller : remettre un torrent dans sa catégorie
                # déplace ses fichiers, au détriment d'un jumeau resté en corbeille.
                partages = _shared_content(
                    {h for h in hash_to_cat if h in in_target}, all_torrents
                )

                # Regroupe par catégorie d'origine
                by_cat: dict[str, list[dict[str, str]]] = {}
                for h, (name, cat) in hash_to_cat.items():
                    if h not in in_target:
                        continue
                    autre = partages.get(h.lower())
                    if autre:
                        log.info("  ⏭ [%s] %s non restauré (fichiers partagés avec [%s] %s)",
                                 TARGET_CAT, name, autre.get("category", "?"), autre.get("name", "?"))
                        continue
                    by_cat.setdefault(cat, []).append({"hash": h, "name": name})

                if not by_cat:
                    log.info("Aucun torrent à restaurer.")
                    return

                restored: list[dict[str, str]] = []
                for cat, cat_torrents in by_cat.items():
                    hashes = "|".join(t["hash"] for t in cat_torrents)
                    try:
                        rep = s.post(
                            f"{QBIT_URL}/api/v2/torrents/setCategory",
                            data={"hashes": hashes, "category": cat},
                            timeout=10,
                        )
                        rep.raise_for_status()
                    except requests.RequestException as e:
                        log.error("  ✗ Restauration vers '%s' impossible : %s", cat, e)
                        continue
                    for t in cat_torrents:
                        log.info("  ↩ Restauré [%s] %s → %s", TARGET_CAT, t["name"], cat)
                        restored.append(t)

                log.info("=== Restauration terminée : %d torrents remis en place ===", len(restored))
        except Exception as e:
            log.error("Erreur lors de la restauration: %s", e)

    threading.Thread(target=run_restore, daemon=True).start()
    return jsonify({"status": "restore started", "since": since})



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


@app.route("/assist", methods=["POST"])
def import_assist():
    """Lance un passage de l'assistant d'import et renvoie ses verdicts (param dry_run)."""
    token = request.args.get("token") or request.headers.get("X-Token")
    if not verify_token(token):
        return jsonify({"error": "Unauthorized"}), 401
    if not _assist_arrs():
        return jsonify({"error": "SONARR_URL / RADARR_URL non configurés"}), 503
    raw = request.args.get("dry_run")
    # Sans paramètre, chaque règle suit son interrupteur. Seuls 0/false/no font
    # agir : une valeur mal saisie simule toutes les règles.
    dry_run = None if raw is None else raw.strip().lower() not in ("0", "false", "no")
    verdicts = run_import_assist(dry_run=dry_run, source="manuel")
    # dry_run : la simulation de la règle 1 ; chaque verdict des autres règles dit la sienne au journal.
    regle_1 = IMPORT_ASSIST_DRY_RUN if dry_run is None else dry_run
    if verdicts is None:
        return jsonify({"dry_run": regle_1, "error": "un autre passage est en cours, réessayer dans un instant"}), 409
    return jsonify({"dry_run": regle_1, "verdicts": verdicts})


@app.route("/assist/history")
def import_assist_history():
    """Derniers verdicts de l'assistant d'import, simulation comprise (params limit, rule)."""
    token = request.args.get("token") or request.headers.get("X-Token")
    if not verify_token(token):
        return jsonify({"error": "Unauthorized"}), 401
    try:
        limit = min(int(request.args.get("limit", 50)), 500)
    except (ValueError, TypeError):
        limit = 50
    try:
        regle = int(request.args["rule"]) if "rule" in request.args else None
    except ValueError:
        return jsonify({"error": "rule : numéro de règle attendu (%s)" % ", ".join(map(str, _REGLES))}), 400
    with _db_lock:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT id, download_id, arr, title, decision, details, dry_run, rule, "
                "datetime(at, 'unixepoch') as at "
                "FROM assist_log WHERE ? IS NULL OR rule = ? ORDER BY id DESC LIMIT ?",
                (regle, regle, limit),
            ).fetchall()
    return jsonify([
        {**dict(r), "rule": _regle(r["rule"]) if r["rule"] in _REGLES else r["rule"]} for r in rows
    ])


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
    threading.Thread(target=import_assist_loop, daemon=True).start()
    return app


if __name__ == "__main__":
    init_db()
    threading.Thread(target=sync_loop, daemon=True).start()
    threading.Thread(target=import_assist_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=5000)
