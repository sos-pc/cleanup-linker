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
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Iterator

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
# Garde-fou de la sync : une lecture qui ferait perdre à l'index plus de ce pourcentage de
# ses lignes, ou de ses chemins /data/Media, est refusée, et l'index reste tel quel. Sans
# lui, un montage perdu ou un qBittorrent qui répond vide remplaçait l'index par du vide,
# avec le statut « success ». 0 le désactive. POST /sync?force=true passe outre, une fois.
SYNC_MAX_DROP_PERCENT = float(os.getenv("SYNC_MAX_DROP_PERCENT", "50"))
BACKUP_RETENTION = int(os.getenv("BACKUP_RETENTION", "5"))

# Délai de grâce du /cleanup. Un torrent tout juste terminé est déjà marqué
# arr_managed par le webhook de grab (seul Sonarr coche « On Grab »), mais n'a pas
# encore de hardlink tant que l'*arr ne l'a pas importé : il ressemble donc à un
# orphelin sans en être un.
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
# passage régulier, toutes les heures, est un renfort : l'*arr n'émet l'event
# qu'une fois par téléchargement, et l'oublie à son redémarrage. Un téléchargement
# mort ou une saison trouée n'émettent aucun event : lui seul les voit. Toutes les
# 6 h, un mort-né attendait jusqu'à 18 h sa course. 0 minute le désactive (/assist
# reste utilisable). Seuls 0/false/no font agir : une valeur mal saisie garde la
# simulation.
IMPORT_ASSIST_DRY_RUN = os.getenv("IMPORT_ASSIST_DRY_RUN", "true").strip().lower() not in ("0", "false", "no")
IMPORT_ASSIST_INTERVAL = int(os.getenv("IMPORT_ASSIST_INTERVAL_MINUTES", "60")) * 60

# Règle 2 de l'assistant (torrent mal nommé) : un import bloqué « matched by ID »,
# parce que l'*arr ne reconnaît pas le nom du torrent, alors que le titre de la
# release désigne à lui seul le même média. Le torrent est renommé avec ce titre,
# et l'*arr importe lui-même. Simulation propre, dans les mêmes conditions.
IMPORT_ASSIST_NOM_DRY_RUN = os.getenv("IMPORT_ASSIST_NOM_DRY_RUN", "true").strip().lower() not in ("0", "false", "no")
# Règle 2, identité confirmée : quand le titre de la release ne désigne pas le
# média à lui seul (titre français, mots en plus, titre d'épisode collé, année
# absente), l'assistant confirme l'identité sans l'identifiant de l'indexeur, puis
# donne au torrent un nom que l'*arr relit comme ce média. Simulation propre.
IMPORT_ASSIST_IDENTITE_DRY_RUN = os.getenv("IMPORT_ASSIST_IDENTITE_DRY_RUN", "true").strip().lower() not in ("0", "false", "no")
# Règle 2, import par titre d'épisode (Sonarr) : une release que l'*arr ne relie que
# par la prise, dont les vidéos portent les titres exacts d'épisodes de la série.
# Ces titres prouvent l'identité, et rangent chaque vidéo à sa place quand la
# release numérote autrement que TVDB. Sa propre simulation.
IMPORT_ASSIST_TITRES_DRY_RUN = os.getenv("IMPORT_ASSIST_TITRES_DRY_RUN", "true").strip().lower() not in ("0", "false", "no")

# Règle 4 de l'assistant (pack incomplet) : un pack de saison partiel est
# remplacé par un pack complet, cherché dans Prowlarr. Sa propre simulation,
# dans les mêmes conditions. Le filet après import remplace la recherche
# d'épisodes de la règle 1 : il n'agit que si la règle 1 agit aussi.
# Sans Prowlarr, la règle se contente de signaler les packs partiels.
IMPORT_ASSIST_PACK_DRY_RUN = os.getenv("IMPORT_ASSIST_PACK_DRY_RUN", "true").strip().lower() not in ("0", "false", "no")
# Mieux vaut une saison sans trou, même moins bien notée : un pack complet est
# pris s'il atteint le seuil du français de son profil, sans le bonus que le
# profil donne aux packs. Si le pack partiel est lui-même plus bas, c'est son
# score qui sert de seuil. Sous le seuil, un pack d'une autre langue ne sert qu'à
# combler les trous : le partiel est gardé, et seuls les fichiers des épisodes
# manquants sont téléchargés.
# Chaque profil a son échelle. Celui qui note l'audio français par un format à
# une seule condition, « langue : French » (le profil Opti : +20000), a pour seuil
# les trois quarts de ce score. Les autres prennent cette valeur : avec le profil
# testtesttest, une release qui porte du français fait au moins 55, une release
# sans français au plus 40 environ.
IMPORT_ASSIST_PACK_MIN_SCORE = int(os.getenv("IMPORT_ASSIST_PACK_MIN_SCORE", "50"))
# Règle 4, saisons trouées : une saison demandée dans Seerr garde des épisodes
# diffusés, sans fichier ni téléchargement, faute de release à l'unité. Un pack de
# la saison, même d'une autre langue, comble alors ces seuls trous, comme après un
# pack partiel. Sa propre simulation, qui suit aussi celle de la règle 4.
IMPORT_ASSIST_TROUS_DRY_RUN = os.getenv("IMPORT_ASSIST_TROUS_DRY_RUN", "true").strip().lower() not in ("0", "false", "no")
PROWLARR_URL = os.getenv("PROWLARR_URL", "").rstrip("/")
PROWLARR_API_KEY = os.getenv("PROWLARR_API_KEY", "")

# Règle 5 de l'assistant (bonne saison) : accorde chaque saison demandée dans
# Seerr (numérotée comme TMDB) avec la saison TVDB que Sonarr surveille.
# Simulation propre, dans les mêmes conditions. Sans Seerr, la règle ne fait rien.
IMPORT_ASSIST_ACCORD_DRY_RUN = os.getenv("IMPORT_ASSIST_ACCORD_DRY_RUN", "true").strip().lower() not in ("0", "false", "no")
SEERR_URL = os.getenv("SEERR_URL", "").rstrip("/")
SEERR_API_KEY = os.getenv("SEERR_API_KEY", "")

# Règle 6 de l'assistant (téléchargement mort) : un téléchargement de la file
# Sonarr/Radarr qui n'a rien reçu depuis ce nombre de jours, sans aucune source
# connectée, reste en place, et la meilleure autre version est prise en plus. Le
# premier fini gagne. Si l'alternative meurt à son tour, la suivante est prise,
# jusqu'à IMPORT_ASSIST_MORT_ESSAIS versions. Simulation propre, dans les mêmes
# conditions.
IMPORT_ASSIST_MORT_DRY_RUN = os.getenv("IMPORT_ASSIST_MORT_DRY_RUN", "true").strip().lower() not in ("0", "false", "no")
IMPORT_ASSIST_MORT_JOURS = float(os.getenv("IMPORT_ASSIST_MORT_JOURS", "3"))
IMPORT_ASSIST_MORT_ESSAIS = int(os.getenv("IMPORT_ASSIST_MORT_ESSAIS", "5"))
# Un torrent qui n'a jamais rien reçu ni jamais vu de copie complète depuis son
# ajout est mort-né : la course commence après ce nombre d'heures, sans attendre
# IMPORT_ASSIST_MORT_JOURS. Simulation propre.
IMPORT_ASSIST_MORT_JAMAIS_HEURES = float(os.getenv("IMPORT_ASSIST_MORT_JAMAIS_HEURES", "12"))
IMPORT_ASSIST_MORT_JAMAIS_DRY_RUN = os.getenv("IMPORT_ASSIST_MORT_JAMAIS_DRY_RUN", "true").strip().lower() not in ("0", "false", "no")

# Règle 7 de l'assistant (téléchargement inutile) : un téléchargement bloqué qui
# n'apporterait rien, la bibliothèque ayant déjà mieux, sort de la file sans
# blacklist et part en corbeille, où il continue de seeder. Simulation propre.
IMPORT_ASSIST_INUTILE_DRY_RUN = os.getenv("IMPORT_ASSIST_INUTILE_DRY_RUN", "true").strip().lower() not in ("0", "false", "no")
# Règle 7, release piégée : un torrent sans aucune vidéo, avec un exécutable ou un
# script. Le grab du même nom est marqué échoué dans l'*arr (liste noire, nouvelle
# recherche), et le torrent part en corbeille, arrêté. Simulation propre.
IMPORT_ASSIST_PIEGE_DRY_RUN = os.getenv("IMPORT_ASSIST_PIEGE_DRY_RUN", "true").strip().lower() not in ("0", "false", "no")

CROSSSEED_DB = os.getenv("CROSSSEED_DB", "/config/crossseed/cross-seed.db")

# Corbeille globale : à une vraie suppression ou à un upgrade, tous les noms du
# fichier côté téléchargement passent en corbeille, y compris ceux qu'aucun
# torrent ne tient, qui y entrent par un torrent de corbeille. « off » garde
# l'ancien geste seul. « simulation », par défaut, le garde aussi, et journalise
# ce que ferait le nouveau. « active » remplace l'ancien geste par le nouveau.
# Une valeur inconnue vaut simulation.
CORBEILLE_GLOBALE = os.getenv("CORBEILLE_GLOBALE", "simulation").strip().lower()
if CORBEILLE_GLOBALE not in ("off", "simulation", "active"):
    CORBEILLE_GLOBALE = "simulation"

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

                -- Décisions de la corbeille globale, simulées ou réelles.
                CREATE TABLE IF NOT EXISTS corbeille_log (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    source    TEXT,
                    motif     TEXT,
                    chemin    TEXT,
                    dry_run   INTEGER,
                    resume    TEXT,
                    decision  TEXT,
                    ancien    INTEGER,
                    at        INTEGER DEFAULT (strftime('%s','now'))
                );

                -- Courses de versions pour un téléchargement mort (règle 6).
                CREATE TABLE IF NOT EXISTS dead_race (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    arr       TEXT    NOT NULL,
                    media_id  INTEGER NOT NULL,
                    episodes  TEXT,
                    original  TEXT    NOT NULL,
                    hashes    TEXT    NOT NULL,
                    tried     TEXT    NOT NULL,
                    last_try  INTEGER,
                    done      INTEGER DEFAULT 0,
                    at        INTEGER DEFAULT (strftime('%s','now'))
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
    """
    Crée une session requests avec retry automatique sur erreurs transitoires.
    `retries=0` : un seul essai, et un statut d'erreur reste lisible par raise_for_status.
    """
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504] if retries else [],
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


# Racine de la bibliothèque des *arr, lue par la sync et surveillée par son garde-fou.
_SYNC_MEDIA_ROOT = "/data/Media"
# En dessous de ce nombre de lignes, l'index est trop petit pour juger une chute : une
# installation neuve se remplit sans être bloquée.
_SYNC_MIN_ROWS = 500
# Après une sync ratée ou refusée : nouvel essai au bout de 5 min, 15 min, 30 min, puis
# toutes les heures, jamais plus tard que SYNC_INTERVAL.
_SYNC_RETRY_DELAYS = (300, 900, 1800, 3600)


def _sync_drop(avant: int, apres: int, media_avant: int, media_apres: int) -> str:
    """
    La raison de refuser une sync qui ferait trop maigrir l'index, ou "" : l'index
    entier, ou ses seuls chemins de bibliothèque, perdraient plus de
    SYNC_MAX_DROP_PERCENT %. Un montage /data/Media perdu ne fait perdre que la part
    de la bibliothèque : il est vu par le second compte.
    """
    part = SYNC_MAX_DROP_PERCENT / 100
    if part <= 0:
        return ""

    def chute(a: int, b: int) -> bool:
        return a >= _SYNC_MIN_ROWS and b < a * (1 - part)

    if chute(avant, apres):
        return f"l'index passerait de {avant} à {apres} lignes"
    if chute(media_avant, media_apres):
        return (f"les chemins de {_SYNC_MEDIA_ROOT} passeraient de {media_avant} à {media_apres} "
                "(montage perdu ?)")
    return ""


def _sync_counts(conn: sqlite3.Connection) -> tuple[int, int]:
    """Lignes vivantes de l'index, et parmi elles celles de _SYNC_MEDIA_ROOT."""
    # Préfixe exact, pas LIKE : « _ » et « % » y seraient des jokers.
    racine = _SYNC_MEDIA_ROOT + "/"
    total, media = conn.execute(
        "SELECT COUNT(*), COALESCE(SUM(substr(path, 1, ?) = ?), 0) FROM files WHERE stale = 0",
        (len(racine), racine),
    ).fetchone()
    return total, media


def _sync_delay(statut: str, echecs: int) -> int:
    """
    Attente avant la sync suivante : SYNC_INTERVAL après une réussite. Après un échec
    ou un refus, `echecs` étant le nombre d'échecs qui le précèdent, de plus en plus
    long (_SYNC_RETRY_DELAYS), jamais plus que SYNC_INTERVAL.
    """
    if statut == "success":
        return SYNC_INTERVAL
    return min(SYNC_INTERVAL, _SYNC_RETRY_DELAYS[min(echecs, len(_SYNC_RETRY_DELAYS) - 1)])


@contextmanager
def _sync_exclusive():
    """
    Le verrou de la sync, partagé par tous les process : la boucle tourne dans le
    maître gunicorn, /sync, /cleanup et les ajouts Jellyfin dans les workers. Même
    principe que _assist_exclusive.
    """
    import fcntl  # POSIX seulement : importé ici pour que le module se charge ailleurs

    fd = os.open(DB_PATH + ".sync.lock", os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def _sync_reussie_depuis(instant: float) -> bool:
    """Une sync réussie a-t-elle commencé après cet instant ? Elle a alors relu tout ce qui existait à ce moment."""
    with _db_lock:
        with get_db() as conn:
            return conn.execute(
                "SELECT 1 FROM sync_log WHERE status = 'success' AND started_at > ? LIMIT 1", (int(instant),)
            ).fetchone() is not None


def sync_db(force: bool = False, sauvegarde: bool = True, motif: str = "", apres: float | None = None) -> str:
    """
    Une sync à la fois, entre tous les process : celle qui arrive pendant qu'une
    autre tourne attend la fin de la première, puis relit tout.

    `apres` : la sync est sautée ("skipped") si une autre, réussie, a commencé
    après cet instant, car elle a déjà tout relu. Les ajouts d'un même pack peuvent
    arriver dans deux workers, qui programment chacun leur sync.

    `sauvegarde=False` saute la copie de la base, pour les syncs déclenchées par un
    ajout dans Jellyfin, qui feraient tourner les sauvegardes en quelques heures.
    `motif` s'affiche dans le journal.
    """
    with _sync_exclusive():
        if apres is not None and _sync_reussie_depuis(apres):
            log.info("Sync sautée (%s) : une autre, commencée depuis, a déjà tout relu", motif or "?")
            return "skipped"
        return _sync_db(force, sauvegarde, motif)


def _sync_db(force: bool, sauvegarde: bool, motif: str) -> str:
    """
    Sync complète (atomique avec soft delete) :
    1. Backup la DB (sauf `sauvegarde=False`)
    2. Récupère les catégories qBit → dossiers à scanner
    3. Récupère tous les torrents qBit
    4. Pour chaque torrent → stat() les fichiers → inode
    5. Scanne /data/Media pour trouver les hardlinks côté *arr
    6. Soft delete (stale=1) + insert nouvelles rows + purge stale

    Dans la même transaction que l'étape 6, le garde-fou (_sync_drop) refuse un index
    qui aurait trop maigri : la transaction est annulée et l'index reste tel quel,
    sauf `force`. Retourne "success", "failed" (qBittorrent injoignable) ou "refused".
    """
    log.info("=== Début sync DB%s ===", f" ({motif})" if motif else "")
    start = time.time()

    # Backup avant modification
    if sauvegarde:
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
        return "failed"

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
    media_inode_map = scan_video_files(_SYNC_MEDIA_ROOT)
    log.info("%s : %d inodes trouvés", _SYNC_MEDIA_ROOT, len(media_inode_map))

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
            avant, media_avant = _sync_counts(conn)

            # Marque toutes les rows existantes comme stale
            conn.execute("UPDATE files SET stale = 1")

            # Insère les nouvelles rows (ou met à jour si (path, torrent_hash) existe)
            conn.executemany(_UPSERT_FILES, rows)

            # Purge les rows stale (plus présentes sur le filesystem)
            stale_count = conn.execute("SELECT COUNT(*) FROM files WHERE stale = 1").fetchone()[0]
            conn.execute("DELETE FROM files WHERE stale = 1")
            # Le compte réel de l'index, pas la longueur de la liste lue : les lignes sans
            # torrent ne sont jamais dédoublonnées (UNIQUE ne compare pas NULL).
            rows_after, media_apres = _sync_counts(conn)

            # Garde-fou, jugé sur l'index tel qu'il serait : un refus annule tout.
            refus = _sync_drop(avant, rows_after, media_avant, media_apres)
            bloquee = bool(refus) and not force
            if bloquee:
                conn.rollback()

    if bloquee:
        log.error("⛔ Sync refusée, l'index est gardé tel quel : %s. Si la chute est voulue, "
                  "POST /sync?force=true", refus)
        _log_sync_end(sync_id, "refused", avant, len(torrents), time.time() - start)
        return "refused"
    if refus:
        log.warning("⚠️ Sync forcée malgré le garde-fou : %s", refus)

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
    return "success"


def sync_loop() -> None:
    """Lance la sync au démarrage puis toutes les SYNC_INTERVAL secondes, plus tôt après
    un échec ou un refus (_sync_delay).
    Utilise _sync_started pour éviter les doubles sync_loop (multi-workers gunicorn)."""
    if _sync_started.is_set():
        log.info("sync_loop déjà active dans un autre worker, skip")
        return
    _sync_started.set()

    time.sleep(5)
    echecs = 0
    while True:
        try:
            statut = sync_db()
        except Exception as e:
            log.error("Erreur sync: %s", e)
            statut = "failed"
        delai = _sync_delay(statut, echecs)
        echecs = 0 if statut == "success" else echecs + 1
        log.info("Prochaine sync dans %s", f"{delai // 3600} h" if delai >= 3600 else f"{delai // 60} min")
        time.sleep(delai)


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

    Le payload du webhook ne transporte que l'ItemId, et l'item n'existe plus côté
    Jellyfin au moment où l'event arrive : la correspondance s'établit avant. Un item
    plus récent que cette indexation se retrouve par son id recalculé
    (_jellyfin_paths_by_id), que la même lecture permet de contrôler.
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
    _check_jellyfin_ids(items)

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


# ── Id recalculé ─────────────────────────────────────────────────────────────
# L'event ItemDeleted ne donne que l'id de l'item, jamais son fichier. Mais Jellyfin
# ne tire pas cet id au hasard : il le calcule depuis le type de l'item et son
# chemin (LibraryManager.GetNewItemId : MD5 de « type + chemin » en UTF-16LE, lu
# comme un Guid .NET). Vérifié le 2026-11-24 : 13 781 items sur 13 781. Pour un
# item qu'aucune sync n'a encore relié à son fichier, on recalcule donc l'id des
# chemins connus, et le bon se reconnaît. Ni l'*arr ni une sync ne sont attendus.

_JELLYFIN_TYPES = {
    "Movie": "MediaBrowser.Controller.Entities.Movies.Movie",
    "Episode": "MediaBrowser.Controller.Entities.TV.Episode",
    "Video": "MediaBrowser.Controller.Entities.Video",
    "Series": "MediaBrowser.Controller.Entities.TV.Series",
    "Season": "MediaBrowser.Controller.Entities.TV.Season",
}


def jellyfin_id(item_type: str, chemin_jf: str) -> str:
    """L'id que Jellyfin donne à un item de ce type, à ce chemin tel qu'il le voit (32 chiffres hexadécimaux)."""
    cle = _JELLYFIN_TYPES[item_type] + chemin_jf
    return uuid.UUID(bytes_le=hashlib.md5(cle.encode("utf-16-le")).digest()).hex


def _jellyfin_views(chemin: str) -> set[str]:
    """Les chemins sous lesquels Jellyfin peut voir ce chemin local : lui-même, et sa traduction inverse par JELLYFIN_PATH_MAP."""
    vues = {chemin}
    for jf, local in _PATH_MAP:
        if chemin == local or chemin.startswith(local + "/"):
            vues.add(jf + chemin[len(local):])
    return vues


def _ancestors(chemin: str) -> list[str]:
    """Les dossiers qui contiennent ce chemin, du plus proche à la racine exclue."""
    out = []
    dossier = os.path.dirname(chemin)
    while dossier and dossier != "/":
        out.append(dossier)
        dossier = os.path.dirname(dossier)
    return out


def _match_id(cible: str, item_type: str, chemins: Iterable[str]) -> str | None:
    """
    Le chemin local dont l'id recalculé est `cible` : un fichier pour un film ou un
    épisode, un dossier parent pour une série ou une saison. None si aucun.
    """
    conteneur = item_type in ("Series", "Season")
    vus: set[str] = set()
    for c in chemins:
        for x in (_ancestors(c) if conteneur else (c,)):
            if x in vus:
                continue
            vus.add(x)
            if any(jellyfin_id(item_type, v) == cible for v in _jellyfin_views(x)):
                return x
    return None


def _last_successful_sync() -> int:
    """Le début de la dernière sync réussie (timestamp), 0 s'il n'y en a pas."""
    with _db_lock:
        with get_db() as conn:
            r = conn.execute("SELECT MAX(started_at) FROM sync_log WHERE status = 'success'").fetchone()
    return int(r[0] or 0)


def _jellyfin_paths_by_id(item_id: str, item_type: str) -> list[str]:
    """
    Les chemins d'un item qu'aucune sync n'a encore relié à son fichier, par son id
    recalculé. D'abord sur l'index, qui reçoit chaque import dès son event (fichier
    de l'*arr et fichier du torrent) ; puis sur les torrents ajoutés depuis la
    dernière sync, lus dans qBittorrent. Un fichier pour un film ou un épisode ; les
    fichiers indexés sous le dossier pour une série ou une saison. [] si aucun
    chemin connu ne donne cet id : l'item est hors torrents et hors *arr.
    """
    if item_type not in _JELLYFIN_TYPES:
        return []
    cible = item_id.replace("-", "").lower()
    with _db_lock:
        with get_db() as conn:
            chemins = [r[0] for r in conn.execute("SELECT DISTINCT path FROM files WHERE stale = 0")]
    trouve = _match_id(cible, item_type, chemins)
    if trouve is None:
        return _jellyfin_paths_in_qbit(cible, item_type)
    if item_type in ("Series", "Season"):
        return sorted(c for c in chemins if c.startswith(trouve + "/"))
    return [trouve]


def _check_jellyfin_ids(items: list[dict[str, Any]]) -> int:
    """
    Contrôle à chaque sync : l'id que Jellyfin donne à chaque item doit être celui
    qu'on recalcule depuis son chemin. Un écart veut dire que Jellyfin a changé sa
    façon de faire : un item supprimé avant d'être indexé ne serait plus retrouvé,
    sans aucune erreur. Rend le nombre d'écarts.
    """
    ecarts = [it for it in items if it.get("Path") and it.get("Type") in _JELLYFIN_TYPES
              and jellyfin_id(it["Type"], it["Path"]) != (it.get("Id") or "").replace("-", "").lower()]
    if ecarts:
        log.warning("⚠️ %d item(s) Jellyfin sur %d n'ont pas l'id recalculé depuis leur chemin, dont %s : un item "
                    "supprimé avant d'être indexé ne sera plus retrouvé", len(ecarts), len(items), ecarts[0]["Path"])
    return len(ecarts)


def _jellyfin_paths_in_qbit(cible: str, item_type: str) -> list[str]:
    """
    Le même calcul sur les fichiers des torrents ajoutés depuis la dernière sync
    réussie, que l'index ne connaît pas encore : un téléchargement fini mais pas
    encore importé, ou un torrent ajouté à la main, dans une bibliothèque posée sur
    le dossier de téléchargement. Les chemins trouvés sont inscrits dans l'index avec
    leur torrent, pour que la suite les traite comme les autres. Un fichier déjà
    effacé reçoit un inode négatif, propre à son chemin, qui n'existe sur aucun disque.
    """
    depuis = _last_successful_sync()
    fichiers: dict[str, dict[str, Any]] = {}
    with qbit_session() as s:
        for t in qbit_get_torrents(s):
            if (t.get("added_on") or 0) < depuis:
                continue
            rep = s.get(f"{QBIT_URL}/api/v2/torrents/files", params={"hash": t["hash"]}, timeout=30)
            rep.raise_for_status()
            base = (t.get("save_path") or "").rstrip("/")
            for f in rep.json() or []:
                chemin = os.path.join(base, f.get("name") or "")
                if os.path.splitext(chemin)[1].lower() in VIDEO_EXTENSIONS:
                    fichiers[chemin] = t
    trouve = _match_id(cible, item_type, fichiers)
    if trouve is None:
        return []
    if item_type in ("Series", "Season"):
        chemins = sorted(c for c in fichiers if c.startswith(trouve + "/"))
    else:
        chemins = [trouve]
    lignes = []
    for c in chemins:
        t = fichiers[c]
        try:
            inode = os.lstat(c).st_ino
        except OSError:
            inode = -(int(hashlib.sha1(c.encode()).hexdigest()[:12], 16) or 1)
        lignes.append((inode, c, t["hash"].lower(), t.get("name"), t.get("category")))
    with _db_lock:
        with get_db() as conn:
            conn.executemany(_UPSERT_FILES, lignes)
    return chemins


# Un ajout dans Jellyfin déclenche une sync, pour que l'item soit connu de l'index
# avant qu'on puisse le supprimer. Les ajouts d'une même minute (un pack de saison,
# une bibliothèque qui se remplit) se regroupent dans une seule sync.
_JELLYFIN_INDEX_DELAY = 60
_jellyfin_index_lock = threading.Lock()
_jellyfin_index_timer: threading.Timer | None = None
_jellyfin_index_items = 0
_jellyfin_index_depuis = 0.0  # le premier ajout de la sync programmée


def schedule_jellyfin_index() -> bool:
    """
    Programme une sync dans _JELLYFIN_INDEX_DELAY secondes, après un ItemAdded.

    La porte ne relie un item Jellyfin à ses fichiers que pendant une sync. Celle
    de 12 h laissait sans effet la suppression d'un média arrivé entre-temps :
    Oppenheimer, supprimé dans Jellyfin 12 min après son import, le 2026-11-22.
    False si une sync était déjà programmée : l'ajout y est compté.
    """
    global _jellyfin_index_timer, _jellyfin_index_items, _jellyfin_index_depuis
    with _jellyfin_index_lock:
        _jellyfin_index_items += 1
        if _jellyfin_index_timer is not None and _jellyfin_index_timer.is_alive():
            return False
        _jellyfin_index_depuis = time.time()
        _jellyfin_index_timer = threading.Timer(_JELLYFIN_INDEX_DELAY, _run_jellyfin_index)
        _jellyfin_index_timer.daemon = True
        _jellyfin_index_timer.start()
        return True


def _run_jellyfin_index() -> str:
    global _jellyfin_index_timer, _jellyfin_index_items
    # Libéré avant la sync : un ajout reçu pendant qu'elle tourne en programme une autre.
    with _jellyfin_index_lock:
        _jellyfin_index_timer = None
        n, _jellyfin_index_items = _jellyfin_index_items, 0
        depuis = _jellyfin_index_depuis
    try:
        # Sautée si l'autre worker a déjà fait une sync depuis le premier de ces ajouts.
        return sync_db(sauvegarde=False, motif=f"{n} ajout(s) dans Jellyfin", apres=depuis)
    except Exception as e:
        log.error("Erreur sync après un ajout Jellyfin : %s", e)
        return "failed"


# ── Délégation à Sonarr / Radarr ─────────────────────────────────────────────


def _arr_request(
    method: str, base_url: str, api_key: str, endpoint: str, timeout: float = 30, retries: int = 3, **kwargs: Any
) -> Any:
    """
    Appel à l'API v3 d'un *arr. Retourne le JSON, ou None si corps vide.
    `retries=0` pour un appel qu'un nouvel essai ne ferait pas réussir.
    """
    session = _create_retry_session(retries)
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


def _radarr_current_file(movie: dict[str, Any]) -> str:
    """Le chemin du fichier que Radarr tient pour ce film, ou "" s'il n'en a pas."""
    mf = movie.get("movieFile") or {}
    if mf.get("path"):
        return mf["path"]
    if mf.get("relativePath") and movie.get("path"):
        return os.path.join(movie["path"], mf["relativePath"])
    return ""


def _delegate_radarr(paths: list[str], dry_run: bool, inodes: set[int]) -> dict[str, Any] | None:
    """
    Dé-monitore puis supprime le fichier côté Radarr. None si film inconnu.

    Radarr retrouve un film par son dossier, pas par son fichier : l'ancien fichier
    d'un upgrade y était aussi. `inodes` sont ceux des fichiers disparus. Si le
    fichier actuel de Radarr est encore sur le disque et n'en fait pas partie, la
    disparition ne le concerne pas : la fiche est gardée ({"garde": True}), et
    l'appelant passe les chemins disparus à la corbeille, comme l'event d'upgrade
    l'aurait fait. Le 2026-11-22, cet event perdu, la porte a supprimé No Other Land
    de Radarr, avec la version qu'il venait d'importer, sur l'ItemDeleted émis par
    Jellyfin pour l'ancien fichier.
    """
    if not (RADARR_URL and RADARR_API_KEY):
        return None

    movie = _match_by_path(_arr_request("GET", RADARR_URL, RADARR_API_KEY, "movie") or [], paths)
    if not movie:
        return None

    mid, titre = movie["id"], movie.get("title", "?")
    file_id = (movie.get("movieFile") or {}).get("id")

    actuel = _radarr_current_file(movie)
    if actuel:
        try:
            inode_actuel: int | None = os.stat(actuel).st_ino
        except OSError:
            inode_actuel = None  # son fichier a disparu : c'est bien lui qu'on a supprimé
        if inode_actuel is not None and inode_actuel not in inodes:
            log.info("  ⏭ Radarr : '%s' garde son fichier actuel (%s), qui n'est pas celui qui a disparu : "
                     "upgrade ou autre version, la fiche n'est pas touchée", titre, os.path.basename(actuel))
            return {"arr": "radarr", "movie": titre, "garde": True}

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

    Les fichiers disparus sont élargis à leurs autres noms (_arr_candidate_paths),
    pour retrouver la copie que l'*arr connaît quand Jellyfin lit le dossier du
    torrent : la bibliothèque « Films » voit le torrent, Radarr son hardlink.
    Un autre nom encore lu par une bibliothèque Jellyfin est écarté de cet
    élargissement. C'est une autre copie, toujours visible : un doublon, ou le
    nouveau nom d'un dossier déplacé. Par lui, l'*arr retrouverait la fiche de
    cette copie et la supprimerait avec ses fichiers (incident Westworld du
    2026-11-12). Si les bibliothèques Jellyfin sont illisibles, aucun autre nom
    n'est retenu : la fiche ne peut venir que des chemins supprimés eux-mêmes.
    """
    try:
        candidats = _arr_candidate_paths(paths)
    except Exception as e:
        log.error("  Résolution des hardlinks impossible, chemins Jellyfin seuls : %s", e)
        candidats = list(paths)
    autres = candidats[len(paths):]
    if autres:
        lues = _jellyfin_libraries()
        if lues is None:
            log.warning("  Bibliothèques Jellyfin illisibles : %d autre(s) nom(s) écarté(s), délégation par les "
                        "seuls chemins supprimés", len(autres))
            autres = []
        else:
            visibles = [p for p in autres if _under(p, lues)]
            if visibles:
                log.warning("  %d autre(s) nom(s) encore lu(s) par Jellyfin écarté(s), dont %s : une autre copie reste "
                            "visible, sa fiche n'est pas touchée", len(visibles), visibles[0])
            autres = [p for p in autres if not _under(p, lues)]
        candidats = list(paths) + autres
    if autres:
        log.info("  %d autre(s) chemin(s) du même fichier, via l'inode : %s", len(autres), autres[0])

    try:
        if item_type in ("Series", "Season", "Episode"):
            return _delegate_sonarr(candidats, item_type, dry_run)
        if item_type in ("Movie", "Video"):
            return _delegate_radarr(candidats, dry_run, _path_inodes(paths))
    except Exception as e:
        log.error("  Erreur délégation *arr : %s", e)
    return None


def _path_inodes(paths: list[str]) -> set[int]:
    """Les inodes que l'index donne à ces chemins : ceux de fichiers disparus, qu'on ne peut plus lire."""
    inodes: set[int] = set()
    with _db_lock:
        with get_db() as conn:
            for i in range(0, len(paths), _SQL_CHUNK):
                lot = paths[i:i + _SQL_CHUNK]
                inodes |= {r[0] for r in conn.execute(
                    f"SELECT DISTINCT inode FROM files WHERE path IN ({','.join('?' * len(lot))})", lot)}
    return inodes


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


# ── Corbeille globale ────────────────────────────────────────────────────────
#
# Un seul geste pour une vraie suppression ou un upgrade. Pour chaque fichier
# touché, ses autres noms décident :
#   - un autre nom dans /data/Media : une autre fiche ou bibliothèque l'utilise,
#     rien de ce fichier ne part ;
#   - un lien que l'index ne connaît pas (collection, dossier non scanné) : pareil ;
#   - sinon, tous ses noms côté téléchargement partent. Un torrent qui en tient un
#     part si aucune de ses autres vidéos ne sert encore. Un nom qu'aucun torrent ne
#     tient entre dans un torrent de corbeille, qui le liste et se vide comme les autres.
# « Tenu par un torrent » se lit dans la liste des fichiers du torrent, jamais dans
# son dossier : un dossier peut contenir des fichiers qui n'en font pas partie. Les
# cross-seeds suivent parce qu'ils partagent les mêmes fichiers, jamais par leur nom :
# plusieurs torrents s'appellent « Season 2 » sans rien partager.
#
# Le torrent de corbeille liste des fichiers sans lire leurs données : ses empreintes
# de pièces sont nulles. Ajouté sans vérification, arrêté, privé et sans tracker, il ne
# parle à personne et ne sert qu'à vider ces fichiers depuis qBittorrent. Le retirer
# sans ses fichiers annule le geste. Rien n'est supprimé par le service.

_CROSSSEEDS_DIR = "/data/Multimedia/cross-seeds"
_CORBEILLE_TAG = "sans-torrent"
_CORBEILLE_PREFIXE = "[sans torrent] "
# Une vidéo d'un torrent lue seulement dans un dossier de téléchargement, et de moins
# d'un quart de la plus grosse, est un bonus de la même release (making-of, clip,
# échantillon) : elle ne retient pas le torrent.
_BONUS_RATIO = 4
_CORBEILLE_PIECE = 1 << 24
# Lignes de détail au plus par sorte, dans le journal.
_CORBEILLE_LOG_MAX = 8
# Au-delà, un passage au repos qui voit autant de torrents sans plus aucune vidéo
# soupçonne un disque démonté, et n'en met aucun en corbeille.
_PERDUS_MAX = 20


def _bencode(x: Any) -> bytes:
    """Encode en bencode : entiers, octets, chaînes UTF-8, listes, dictionnaires à clés triées."""
    if isinstance(x, bool):
        raise TypeError("bool")
    if isinstance(x, int):
        return b"i%de" % x
    if isinstance(x, bytes):
        return b"%d:%s" % (len(x), x)
    if isinstance(x, str):
        return _bencode(x.encode("utf-8"))
    if isinstance(x, list):
        return b"l" + b"".join(_bencode(v) for v in x) + b"e"
    if isinstance(x, dict):
        paires = sorted(((k.encode("utf-8") if isinstance(k, str) else k), v) for k, v in x.items())
        return b"d" + b"".join(_bencode(k) + _bencode(v) for k, v in paires) + b"e"
    raise TypeError(type(x).__name__)


def _trash_torrent(racine: str, fichiers: list[str]) -> tuple[bytes, str]:
    """
    Le .torrent d'un torrent de corbeille : ces fichiers, sous `racine`, sans lire
    leurs données. Retourne (contenu du .torrent, infohash v1).

    Le dossier entre dans l'infohash (champ `source`). Deux dossiers jumeaux, la
    même release sous deux trackers de cross-seeds, ont le même nom et les mêmes
    fichiers : sans lui, ils donneraient le même torrent, et qBittorrent refuserait
    le second.
    """
    nom = os.path.basename(racine)
    if fichiers == [racine] and not os.path.isdir(racine):
        total = os.path.getsize(racine)
        info: dict[str, Any] = {"name": nom, "length": total}
    else:
        liste = [{"length": os.path.getsize(f), "path": os.path.relpath(f, racine).split(os.sep)}
                 for f in sorted(fichiers)]
        total = sum(e["length"] for e in liste)
        info = {"name": nom, "files": liste}
    pieces = max(1, -(-total // _CORBEILLE_PIECE))
    dossier = hashlib.sha1(os.fsencode(racine)).hexdigest()[:16]
    info.update({"piece length": _CORBEILLE_PIECE, "pieces": b"\0" * 20 * pieces,
                 "private": 1, "source": f"cleanup-linker {dossier}"})
    return _bencode({"info": info, "created by": "cleanup-linker"}), hashlib.sha1(_bencode(info)).hexdigest()


def _same_inode(chemin: str, inode: int) -> bool:
    try:
        return os.lstat(chemin).st_ino == inode
    except OSError:
        return False


def _under(chemin: str, dossiers: set[str]) -> bool:
    return any(chemin == d or chemin.startswith(d + "/") for d in dossiers)


def _index_names(conn: sqlite3.Connection, inodes: set[int]) -> dict[int, set[str]]:
    """Tous les noms que l'index connaît pour ces inodes."""
    noms: dict[int, set[str]] = {}
    liste = sorted(inodes)
    for i in range(0, len(liste), _SQL_CHUNK):
        lot = liste[i:i + _SQL_CHUNK]
        for r in conn.execute(f"SELECT DISTINCT inode, path FROM files WHERE stale = 0 AND inode IN "
                              f"({','.join('?' * len(lot))})", lot):
            noms.setdefault(r["inode"], set()).add(r["path"])
    return noms


def _jellyfin_libraries() -> set[str] | None:
    """Dossiers que lisent les bibliothèques Jellyfin, en chemins locaux. None si Jellyfin ne répond pas."""
    if not jellyfin_enabled():
        return set()
    try:
        r = requests.get(f"{JELLYFIN_URL}/Library/VirtualFolders", timeout=30,
                         headers={"Authorization": f'MediaBrowser Token="{JELLYFIN_API_KEY}"'})
        r.raise_for_status()
        return {map_jellyfin_path(p).rstrip("/") for b in r.json() or [] for p in b.get("Locations") or []}
    except Exception as e:
        log.warning("  Corbeille globale : bibliothèques Jellyfin illisibles (%s)", e)
        return None


def _trash_group(nom: str, dossiers: set[str]) -> str | None:
    """
    Le dossier de release d'un nom : le premier niveau sous son dossier de
    téléchargement, ou sous le tracker dans cross-seeds. Le fichier lui-même s'il
    est posé directement dans ce dossier. None hors des dossiers de téléchargement.
    """
    base = max((d for d in dossiers if nom.startswith(d + "/")), key=len, default=None)
    if base is None:
        return None
    morceaux = os.path.relpath(nom, base).split(os.sep)
    niveaux = 2 if base == _CROSSSEEDS_DIR else 1
    if len(morceaux) <= niveaux:
        return nom
    return os.path.join(base, *morceaux[:niveaux])


def _tags(t: dict[str, Any]) -> set[str]:
    return {x.strip() for x in (t.get("tags") or "").split(",") if x.strip()}


class _QbitVue:
    """
    qBittorrent lu une fois pour une décision : torrents, dossiers de téléchargement,
    et listes de fichiers, demandées au besoin.
    """

    def __init__(self, s: requests.Session) -> None:
        self.s = s
        self.torrents = {t["hash"].lower(): t for t in qbit_get_torrents(s)}
        r = s.get(f"{QBIT_URL}/api/v2/torrents/categories", timeout=10)
        r.raise_for_status()
        categories = {n: (i.get("savePath") or "").rstrip("/") for n, i in (r.json() or {}).items()}
        r = s.get(f"{QBIT_URL}/api/v2/app/defaultSavePath", timeout=10)
        r.raise_for_status()
        defaut = r.text.strip().rstrip("/")
        self.dossiers = {d for d in categories.values() if d} | {defaut, _CROSSSEEDS_DIR}
        self.dossier_corbeille = categories.get(TARGET_CAT) or f"{defaut}/{TARGET_CAT}"
        self.racines: dict[str, list[str]] = {}
        for h, t in self.torrents.items():
            self.racines.setdefault(torrent_root(t), []).append(h)
        self._listes: dict[str, set[str]] = {}
        self._tenants: dict[str, set[str]] = {}

    def categorie(self, h: str) -> str:
        return self.torrents[h].get("category") or ""

    def fichiers_de(self, h: str) -> set[str]:
        if h not in self._listes:
            rep = self.s.get(f"{QBIT_URL}/api/v2/torrents/files", params={"hash": h}, timeout=30)
            rep.raise_for_status()
            base = (self.torrents[h].get("save_path") or "").rstrip("/")
            self._listes[h] = {os.path.join(base, f.get("name") or "") for f in rep.json() or []}
        return self._listes[h]

    def tenu_par(self, nom: str) -> set[str]:
        """Les torrents qui listent ce fichier, d'après leur liste de fichiers."""
        if nom not in self._tenants:
            out: set[str] = set()
            cur = nom
            while True:
                for h in self.racines.get(cur, []):
                    if nom in self.fichiers_de(h):
                        out.add(h)
                parent = os.path.dirname(cur)
                if parent == cur:
                    break
                cur = parent
            self._tenants[nom] = out
        return self._tenants[nom]

    def corbeilles_de(self, racine: str) -> list[str]:
        """Les torrents de corbeille déjà faits pour ce dossier, restés sur place."""
        return sorted(h for h in self.racines.get(racine, [])
                      if self.categorie(h) == TARGET_CAT and _CORBEILLE_TAG in _tags(self.torrents[h]))


def _plan_vide(chemins: list[str]) -> dict[str, Any]:
    return {"chemins": chemins, "touches": 0, "octets": 0, "torrents": [], "corbeilles": [],
            "restent": [], "gardes": {}, "deja": 0, "liberes": 0, "refus": ""}


def _video_retient(st: os.stat_result, presents: set[str], lues: set[str] | None, plus_gros: int) -> bool:
    """
    Une vidéo retient son torrent si elle a un lien que l'index ne connaît pas, un nom
    dans /data/Media, ou un nom lu par Jellyfin qui n'est pas un petit bonus.
    """
    # Sans réponse de Jellyfin, chaque nom d'un dossier de téléchargement compte
    # comme visible : seul un petit bonus ne retient alors pas le torrent.
    visible = any(_under(n, lues) for n in presents) if lues is not None else bool(presents)
    return (st.st_nlink > len(presents) or any(n.startswith("/data/Media/") for n in presents)
            or (visible and st.st_size * _BONUS_RATIO >= plus_gros))


def _trash_plan(chemins: list[str], s: requests.Session, lues: set[str] | None) -> dict[str, Any]:
    """
    Ce que la corbeille globale ferait pour ces chemins supprimés. Ne change rien.
    `lues` : dossiers lus par Jellyfin, None s'il n'a pas répondu (on garde alors
    tout torrent dont une autre vidéo pourrait être visible).
    """
    plan = _plan_vide(chemins)
    for c in chemins:
        if len(c.rstrip("/").split("/")) < _MIN_ROOT_DEPTH:
            plan["refus"] = f"chemin trop haut pour un seul titre : {c}"
            return plan

    # 1. Les fichiers touchés, et tous les noms que l'index leur connaît.
    ici: dict[int, set[str]] = {}
    with _db_lock:
        with get_db() as conn:
            for c in chemins:
                c = c.rstrip("/")
                for r in conn.execute("SELECT DISTINCT inode, path FROM files WHERE stale = 0 AND (path = ? OR path LIKE ?)",
                                      (c, c + "/%")):
                    if r["path"] == c or r["path"].startswith(c + "/"):
                        ici.setdefault(r["inode"], set()).add(r["path"])
            connus = _index_names(conn, set(ici))
    plan["touches"] = len(ici)
    if not ici:
        plan["refus"] = "aucun fichier connu de l'index sous ce chemin"
        return plan

    # 2. qBittorrent, lu maintenant : torrents, dossiers de téléchargement, listes de fichiers.
    vue = _QbitVue(s)
    plan["dossier_corbeille"] = vue.dossier_corbeille

    # 3. Le verdict de chaque fichier touché.
    jetes: set[int] = set()
    gardes: dict[int, str] = {}
    presents_de: dict[int, set[str]] = {}
    orphelins: set[str] = set()
    candidats: set[str] = set()
    for i, noms_ici in ici.items():
        presents = {n for n in connus.get(i, set()) | noms_ici if _same_inode(n, i)}
        if not presents:
            continue  # plus aucun nom sur le disque : il n'y a plus rien à jeter
        st = os.lstat(next(iter(presents)))
        plan["octets"] += st.st_size
        presents_de[i] = presents
        reste = presents - noms_ici
        if st.st_nlink > len(presents):
            gardes[i] = "un nom que l'index ne connaît pas (collection, autre dossier)"
        elif any(n.startswith("/data/Media/") for n in reste):
            gardes[i] = "un autre nom dans /data/Media"
        else:
            jetes.add(i)
            for n in reste:
                tenants = vue.tenu_par(n)
                if tenants:
                    candidats |= tenants
                else:
                    orphelins.add(n)
    for raison in gardes.values():
        plan["gardes"][raison] = plan["gardes"].get(raison, 0) + 1

    # 4. Un torrent part si aucune de ses autres vidéos ne sert encore.
    partent: set[str] = set()
    a_juger: dict[str, list[tuple[str, os.stat_result]]] = {}
    for h in sorted(candidats):
        t = vue.torrents[h]
        categorie = vue.categorie(h)
        if categorie == TARGET_CAT:
            plan["deja"] += 1
            continue
        if not categorie:
            plan["restent"].append({"hash": h, "name": t.get("name"), "raison": "sans catégorie : jamais touché"})
            continue
        a_juger[h] = _videos_de(vue, h)
    autres = {st.st_ino for fs in a_juger.values() for _, st in fs} - set(ici)
    if autres:
        with _db_lock:
            with get_db() as conn:
                noms_autres = _index_names(conn, autres)
    else:
        noms_autres = {}
    for h, fichiers in a_juger.items():
        plus_gros = max((st.st_size for _, st in fichiers), default=0)
        encore = ""
        for p, st in fichiers:
            j = st.st_ino
            if j in jetes:
                continue
            if j in gardes:
                encore = p
                break
            presents = {n for n in noms_autres.get(j, set()) if _same_inode(n, j)}
            if _video_retient(st, presents, lues, plus_gros):
                encore = p
                break
        t = vue.torrents[h]
        if encore:
            plan["restent"].append({"hash": h, "name": t.get("name"),
                                    "raison": f"une autre de ses vidéos sert encore : {encore}"})
        else:
            partent.add(h)
            plan["torrents"].append({"hash": h, "name": t.get("name"), "category": t.get("category")})

    _trash_finish(plan, vue, lues, orphelins, jetes, presents_de, ici, partent, a_juger, noms_autres)
    return plan


def _videos_de(vue: _QbitVue, h: str) -> list[tuple[str, os.stat_result]]:
    """Les vidéos qu'un torrent liste et qui sont sur le disque."""
    out = []
    for p in sorted(vue.fichiers_de(h)):
        if os.path.splitext(p)[1].lower() not in VIDEO_EXTENSIONS:
            continue
        try:
            out.append((p, os.lstat(p)))
        except OSError:
            continue
    return out


def _trash_finish(plan: dict[str, Any], vue: _QbitVue, lues: set[str] | None, orphelins: set[str],
                  jetes: set[int], presents_de: dict[int, set[str]], supprimes: dict[int, set[str]],
                  partent: set[str], a_juger: dict[str, list[tuple[str, os.stat_result]]],
                  noms_autres: dict[int, set[str]]) -> None:
    """
    Fin commune du geste et du passage au repos : les noms sans torrent réunis par
    dossier, puis ce que vider la corbeille libérerait. `supprimes` : les noms que le
    geste vient de retirer, par fichier (aucun au repos).
    """
    partiront = partent | {h for h in vue.torrents if vue.categorie(h) == TARGET_CAT}

    # 5. Les noms sans torrent, réunis par dossier de release dans un torrent de corbeille.
    groupes: dict[str, list[str]] = {}
    hors: list[str] = []
    corbeille = vue.dossier_corbeille
    for n in orphelins:
        g = _trash_group(n, vue.dossiers)
        # Jamais un dossier de téléchargement entier, ni le dossier de la corbeille.
        if g is None or g in vue.dossiers or _under(n, {corbeille}) or _under(corbeille, {g}):
            hors.append(n)
        else:
            groupes.setdefault(g, []).append(n)
    listes_corbeille: set[str] = set()
    for g, noms in sorted(groupes.items()):
        # Un upgrade arrive épisode par épisode : le torrent de corbeille déjà fait pour ce
        # dossier est repris dans le nouveau, qui le remplace. Un seul par dossier.
        anciens = vue.corbeilles_de(g)
        # Même règle que pour les nouveaux : un fichier qu'un autre torrent liste n'est pas repris.
        deja = {p for h in anciens for p in vue.fichiers_de(h)
                if os.path.isfile(p) and not os.path.islink(p) and vue.tenu_par(p) <= set(anciens)}
        autres_videos: list[str] = []
        if os.path.isdir(g):
            tous = sorted(os.path.join(d, f) for d, _, fs in os.walk(g) for f in fs)
            # Une vidéo qui part avec son torrent, ou déjà en corbeille, ne retient pas le dossier.
            autres_videos = [p for p in tous if os.path.splitext(p)[1].lower() in VIDEO_EXTENSIONS
                             and p not in noms and not (vue.tenu_par(p) and vue.tenu_par(p) <= partiront)]
            # Un dossier qui ne garde aucune autre vidéo part entier, petits fichiers compris.
            fichiers = sorted(noms) if autres_videos else [p for p in tous if os.path.isfile(p) and not os.path.islink(p)]
            # Un fichier qu'un torrent liste n'entre jamais dans un torrent de corbeille.
            fichiers = [p for p in fichiers if p in noms or not vue.tenu_par(p)]
        else:
            fichiers = [g]
        fichiers = sorted(set(fichiers) | deja)
        # Le nom dit quand le dossier garde d'autres vidéos : elles ne partent pas.
        nom = _CORBEILLE_PREFIXE + os.path.basename(g)
        if autres_videos:
            dedans = sum(1 for p in fichiers if os.path.splitext(p)[1].lower() in VIDEO_EXTENSIONS)
            nom += f" ({dedans} vidéo(s) sur {dedans + len(autres_videos)})"
        listes_corbeille.update(fichiers)
        plan["corbeilles"].append({
            "racine": g, "nom": nom, "fichiers": fichiers, "videos": len(noms), "fusion": len(deja),
            "autres": len(fichiers) - len(noms) - len(deja), "remplace": anciens,
            "visible": lues is not None and _under(g, lues),
        })
    if hors:
        plan["restent"].append({"hash": "", "name": f"{len(hors)} nom(s) sans torrent",
                                "raison": f"hors des dossiers de téléchargement, ex. {sorted(hors)[0]}"})

    # 6. Ce que vider la corbeille libérerait : chaque fichier jeté dont plus aucun nom ne reste.
    for i in jetes:
        presents = presents_de[i]
        st = os.lstat(next(iter(presents)))
        if st.st_nlink == len(presents) and all(
                n in supprimes.get(i, ()) or n in listes_corbeille or vue.tenu_par(n) & partiront for n in presents):
            plan["liberes"] += st.st_size
    # Les bonus d'un torrent qui part : libérés aussi, s'ils n'ont pas d'autre nom.
    bonus_vus: set[int] = set()
    for h in partent:
        for _, st in a_juger.get(h, []):
            j = st.st_ino
            if j in jetes or j in supprimes or j in bonus_vus:
                continue
            presents = {n for n in noms_autres.get(j, set()) if _same_inode(n, j)}
            if presents and st.st_nlink == len(presents) and all(vue.tenu_par(n) & partiront for n in presents):
                plan["liberes"] += st.st_size
                bonus_vus.add(j)
    if lues is None:
        plan["jellyfin"] = "injoignable : toute vidéo d'un dossier de téléchargement a compté comme visible"


def _rest_plan(s: requests.Session, lues: set[str], en_file: set[str], geres: set[str]) -> dict[str, Any]:
    """
    La corbeille globale au repos, sans geste : ce que /cleanup mettrait en corbeille.
    Ne change rien.

    Un fichier ne sert plus s'il n'a ni nom dans /data/Media, ni lien que l'index ne
    connaît pas, ni nom lu par Jellyfin, et n'a pas bougé depuis CLEANUP_GRACE_HOURS.
    Un torrent part si aucune de ses vidéos ne le retient, comme au geste. Seuls ceux
    qu'un *arr a pris (`geres`) et les cross-seeds sont regardés, jamais un ajout à la
    main, et un téléchargement encore dans la file d'un *arr (`en_file`) attend.
    Les noms sans torrent d'un fichier qui ne sert plus vont dans un torrent de
    corbeille, sauf si un torrent qui reste tient encore ce fichier : rien ne se
    libérerait, ils attendent que ce torrent parte.
    Un torrent qui listait des vidéos et n'en a plus aucune sur le disque
    (`missingFiles`) part aussi, même ajouté à la main : il ne peut plus seeder et il
    n'y a plus rien à protéger. Pas si son dossier manque (disque démonté), ni si
    plus de _PERDUS_MAX sont dans ce cas à la fois.
    """
    plan = _plan_vide([])
    plan["repos"] = True
    limite = time.time() - CLEANUP_GRACE_HOURS * 3600

    # 1. Les noms de l'index qui existent encore, par fichier.
    with _db_lock:
        with get_db() as conn:
            lignes = conn.execute("SELECT DISTINCT inode, path FROM files WHERE stale = 0").fetchall()
    connus: dict[int, set[str]] = {}
    for r in lignes:
        connus.setdefault(r["inode"], set()).add(r["path"])
    presents_de: dict[int, set[str]] = {}
    etat: dict[int, os.stat_result] = {}
    for i, noms in connus.items():
        for n in noms:
            try:
                st = os.lstat(n)
            except OSError:
                continue
            if st.st_ino == i:
                presents_de.setdefault(i, set()).add(n)
                etat[i] = st

    def sert(i: int) -> bool:
        p, st = presents_de[i], etat[i]
        return (st.st_nlink > len(p) or any(n.startswith("/data/Media/") for n in p)
                or any(_under(n, lues) for n in p) or st.st_ctime > limite)

    vue = _QbitVue(s)
    plan["dossier_corbeille"] = vue.dossier_corbeille

    # 2. Les torrents qu'aucune vidéo ne retient plus.
    partent: set[str] = set()
    a_juger: dict[str, list[tuple[str, os.stat_result]]] = {}
    perdus: list[str] = []
    for h in sorted(vue.torrents):
        t = vue.torrents[h]
        categorie = vue.categorie(h)
        if not categorie or categorie == TARGET_CAT:
            continue
        cross = _under(torrent_root(t), {_CROSSSEEDS_DIR})
        disparu = t.get("state") == "missingFiles"
        # Un ajout à la main n'est jamais touché au repos, sauf s'il n'a plus rien : la garde
        # est plus bas, une fois ses vidéos lues. Celle-ci évite seulement de lire sa liste.
        if h not in geres and not cross and not disparu:
            continue
        fichiers = _videos_de(vue, h)
        if not fichiers:
            # Plus aucune de ses vidéos sur le disque : rien ne le retient, il ne peut plus
            # seeder. Un torrent sans vidéo (musique, livres) n'est pas regardé.
            if (disparu and h not in en_file and os.path.isdir(t.get("save_path") or "")
                    and any(os.path.splitext(p)[1].lower() in VIDEO_EXTENSIONS for p in vue.fichiers_de(h))):
                perdus.append(h)
            continue
        if h not in geres and not cross:
            continue  # il garde des vidéos : un ajout à la main reste hors de portée
        plus_gros = max(st.st_size for _, st in fichiers)
        if any(_video_retient(st, presents_de.get(st.st_ino, set()), lues, plus_gros) for _, st in fichiers):
            continue
        quand = t.get("completion_on") or 0
        if quand <= 0:
            quand = t.get("added_on") or 0
        attente = ""
        if h in en_file:
            attente = "encore dans la file d'un *arr"
        elif quand > limite:
            attente = f"fini depuis moins de {CLEANUP_GRACE_HOURS} h"
        elif (t.get("progress") or 0) < 1 and not cross:
            attente = "téléchargement pas fini"
        elif any(st.st_ctime > limite for _, st in fichiers):
            attente = f"liens changés depuis moins de {CLEANUP_GRACE_HOURS} h"
        if attente:
            plan["restent"].append({"hash": h, "name": t.get("name"), "raison": attente})
            continue
        partent.add(h)
        a_juger[h] = fichiers
        plan["torrents"].append({"hash": h, "name": t.get("name"), "category": categorie})
    if len(perdus) > _PERDUS_MAX:
        plan["restent"].append({"hash": "", "name": f"{len(perdus)} torrent(s) sans plus aucune vidéo",
                                "raison": f"plus de {_PERDUS_MAX} à la fois : un disque démonté ? aucun ne part"})
    else:
        for h in perdus:
            partent.add(h)
            plan["torrents"].append({"hash": h, "name": vue.torrents[h].get("name"), "category": vue.categorie(h),
                                     "note": "ses vidéos ont disparu du disque"})

    # 3. Les fichiers qui ne servent plus et que ce passage libère : leurs noms sans
    #    torrent partent si chaque nom tenu l'est par un torrent qui part ou déjà en corbeille.
    partiront = partent | {h for h in vue.torrents if vue.categorie(h) == TARGET_CAT}
    jetes: set[int] = set()
    orphelins: set[str] = set()
    attendent = 0
    for i in sorted(presents_de):
        if sert(i):
            continue
        presents = presents_de[i]
        tenants = {h for n in presents for h in vue.tenu_par(n)}
        libres = {n for n in presents if not vue.tenu_par(n)}
        if not libres and not tenants & partent:
            continue  # rien de neuf : déjà en corbeille
        if tenants - partiront:
            attendent += len(libres)
            continue
        jetes.add(i)
        orphelins |= libres
        plan["octets"] += etat[i].st_size
    plan["touches"] = len(jetes)
    if attendent:
        plan["restent"].append({"hash": "", "name": f"{attendent} nom(s) sans torrent",
                                "raison": "un torrent qui sert encore tient leurs fichiers : rien ne se libérerait"})

    autres = {st.st_ino for fs in a_juger.values() for _, st in fs} - jetes
    noms_autres = {j: connus.get(j, set()) for j in autres}
    _trash_finish(plan, vue, lues, orphelins, jetes, presents_de, {}, partent, a_juger, noms_autres)
    return plan


def _qbit_has(s: requests.Session, h: str) -> bool:
    """qBittorrent a-t-il ce torrent ?"""
    rep = s.get(f"{QBIT_URL}/api/v2/torrents/info", params={"hashes": h}, timeout=10)
    rep.raise_for_status()
    return bool(rep.json())


def _trash_apply(plan: dict[str, Any], s: requests.Session, source: str) -> dict[str, int]:
    """
    Exécute le plan : torrents en corbeille, torrents de corbeille ajoutés. Rien n'est
    supprimé. Ce qui est fait est compté au fur et à mesure dans plan["faits"] : après
    une erreur, l'appelant sait ce qui a déjà changé.
    """
    faits = plan["faits"] = {"torrents": 0, "corbeilles": 0}
    lot = {t["hash"] for t in plan["torrents"]}
    if lot:
        # Comme ailleurs : un torrent dont le dossier est aussi celui d'un torrent hors du
        # lot reste en place, sinon ATM emporterait les fichiers de l'autre.
        partages = _shared_content(lot, qbit_get_torrents(s))
        bouger = [t for t in plan["torrents"] if t["hash"] not in partages]
        for t in plan["torrents"]:
            if t["hash"] in partages:
                t["garde"] = "dossier partagé avec un autre torrent"
        if bouger:
            r = s.post(f"{QBIT_URL}/api/v2/torrents/setCategory", timeout=10,
                       data={"hashes": "|".join(t["hash"] for t in bouger), "category": TARGET_CAT})
            r.raise_for_status()
            for t in bouger:
                log_move(t["hash"], t.get("name") or "", t.get("category") or "", source)
            faits["torrents"] = len(bouger)
    # Les places déjà prises par un torrent : un torrent de corbeille n'y est jamais déplacé.
    occupees = {torrent_root(t) for t in qbit_get_torrents(s)} if plan["corbeilles"] else set()
    for c in plan["corbeilles"]:
        nom = c["nom"]
        try:
            contenu, h = _trash_torrent(c["racine"], c["fichiers"])
        except OSError as e:
            # Un fichier disparu depuis le plan : ce torrent-là n'est pas fait, les autres si.
            log.error("  ⚠ Corbeille globale : torrent de corbeille %s impossible à faire (%s)", nom, e)
            continue
        # Le dossier est dans l'infohash : un torrent déjà là sous ce hash est celui de ce
        # dossier, avec ces fichiers, fait par un passage précédent. Il n'est pas recompté.
        if _qbit_has(s, h):
            log.info("      = torrent de corbeille %s déjà dans qBittorrent", nom)
            continue
        data = {"category": TARGET_CAT, "tags": _CORBEILLE_TAG, "rename": nom, "savepath": os.path.dirname(c["racine"]),
                "autoTMM": "false", "skip_checking": "true", "stopped": "true", "paused": "true", "contentLayout": "Original"}
        r = s.post(f"{QBIT_URL}/api/v2/torrents/add", data=data, timeout=30,
                   files={"torrents": (f"{h}.torrent", contenu, "application/x-bittorrent")})
        # qBittorrent 5 répond 409 quand il n'ajoute rien, 415 à un .torrent invalide :
        # ce torrent-là n'est pas fait, les suivants le sont quand même.
        if 400 <= r.status_code < 500:
            log.error("  ⚠ Corbeille globale : qBittorrent refuse le torrent de corbeille %s (HTTP %d : %s)",
                      nom, r.status_code, (r.text or "").strip()[:200])
            continue
        r.raise_for_status()
        present = False
        for _ in range(10):
            if _qbit_has(s, h):
                present = True
                break
            time.sleep(1)
        if not present:
            log.error("  ⚠ Corbeille globale : qBittorrent n'a pas ajouté le torrent de corbeille %s", nom)
            continue
        c["hash"] = h
        log_move(h, nom, "", source)
        faits["corbeilles"] += 1
        # Le torrent de corbeille qu'il reprend est retiré, sans ses fichiers : ils sont
        # tous listés par le nouveau, qui est déjà là.
        anciens = [x for x in c.get("remplace") or [] if x != h]
        if anciens:
            r = s.post(f"{QBIT_URL}/api/v2/torrents/delete", timeout=10,
                       data={"hashes": "|".join(anciens), "deleteFiles": "false"})
            r.raise_for_status()
            for x in anciens:
                log.info("      ↻ torrent de corbeille %s repris dans %s, retiré sans ses fichiers", x[:8], nom)
        # Un nom lu par Jellyfin sort du dossier lu, comme le ferait ATM pour un torrent :
        # le titre disparaît de la bibliothèque. Pas si un autre torrent occupe la même place,
        # ni si la place dans la corbeille est déjà prise, par un jumeau déplacé juste avant
        # ou par un dossier resté sur le disque : les deux se mêleraient.
        if c["visible"]:
            cible = os.path.join(plan["dossier_corbeille"], os.path.basename(c["racine"]))
            if c["racine"] in occupees:
                log.info("      ↷ %s reste à sa place, encore lu par Jellyfin : un autre torrent a ce dossier", nom)
            elif cible in occupees or os.path.lexists(cible):
                log.info("      ↷ %s reste à sa place, encore lu par Jellyfin : %s est déjà pris", nom, cible)
            else:
                r = s.post(f"{QBIT_URL}/api/v2/torrents/setLocation", timeout=30,
                           data={"hashes": h, "location": plan["dossier_corbeille"]})
                r.raise_for_status()
                occupees.add(cible)
    return faits


def _trash_summary(plan: dict[str, Any]) -> str:
    if plan["refus"]:
        return f"rien à faire : {plan['refus']}"
    noms = sum(c["videos"] for c in plan["corbeilles"])
    repos = plan.get("repos")
    morceaux = [f"{plan['touches']} fichier(s) " + ("ne servent plus" if repos else "touché(s)")
                + f" ({plan['octets'] / 1e9:.1f} Go)",
                f"{len(plan['torrents'])} torrent(s) en corbeille"]
    if plan["corbeilles"]:
        morceaux.append(f"{len(plan['corbeilles'])} torrent(s) de corbeille pour {noms} nom(s) sans torrent")
    if plan["deja"]:
        morceaux.append(f"{plan['deja']} déjà en corbeille")
    if plan["restent"]:
        morceaux.append(f"{len(plan['restent'])} " + ("en attente" if repos else "torrent(s) restent"))
    if plan["gardes"]:
        morceaux.append(f"{sum(plan['gardes'].values())} fichier(s) gardés")
    # Ce que le vidage libère peut dépasser les fichiers touchés : un torrent qui part
    # emporte aussi ses autres vidéos et ses bonus.
    resume = ", ".join(morceaux) + f" ; {plan['liberes'] / 1e9:.1f} Go libérés au vidage"
    if plan.get("jellyfin"):
        resume += f" (Jellyfin {plan['jellyfin']})"
    return resume


def _corbeille_record(source: str, motif: str, chemins: list[str], dry_run: bool,
                      resume: str, plan: dict[str, Any] | None) -> int:
    decision = None
    if plan is not None:
        decision = json.dumps({
            "torrents": [{"name": t.get("name"), "category": t.get("category"), "garde": t.get("garde"),
                          "note": t.get("note")} for t in plan.get("torrents", [])],
            "corbeilles": [{"racine": c["racine"], "nom": c.get("nom"), "videos": c["videos"], "autres": c["autres"],
                            "fusion": c.get("fusion", 0),
                            "fichiers": [os.path.relpath(f, c["racine"]) if f != c["racine"] else os.path.basename(f)
                                         for f in c["fichiers"]][:200]} for c in plan.get("corbeilles", [])],
            "restent": [{"name": t.get("name"), "raison": t.get("raison")} for t in plan.get("restent", [])],
            "gardes": plan.get("gardes", {}),
            "liberes": plan.get("liberes", 0),
            "octets": plan.get("octets", 0),
        }, ensure_ascii=False)
    with _db_lock:
        with get_db() as conn:
            cur = conn.execute(
                "INSERT INTO corbeille_log (source, motif, chemin, dry_run, resume, decision) VALUES (?, ?, ?, ?, ?, ?)",
                (source, motif, " | ".join(chemins)[:1000], int(dry_run), resume, decision),
            )
            return int(cur.lastrowid)


def global_trash(chemins: list[str], source: str, motif: str, dry_run: bool,
                 fichiers_gardes: bool = False) -> dict[str, Any]:
    """
    La corbeille globale pour des chemins de la bibliothèque qui viennent d'être
    supprimés ou remplacés. `fichiers_gardes` : l'*arr a retiré sa fiche sans
    supprimer les fichiers (case décochée), rien ne part. En simulation, rien ne
    change : la décision est journalisée et tracée dans corbeille_log.
    """
    etiquette = "[SIMULATION] " if dry_run else ""
    if fichiers_gardes:
        resume = "fichiers gardés (case décochée) : rien ne part en corbeille"
        log.info("  %s🗑 Corbeille globale (%s) : %s", etiquette, motif, resume)
        return {"id": _corbeille_record(source, motif, chemins, dry_run, resume, None), "resume": resume}
    plan = None
    try:
        lues = _jellyfin_libraries()
        with qbit_session() as s:
            plan = _trash_plan(chemins, s, lues)
            faits = _trash_apply(plan, s, source) if not dry_run and not plan["refus"] else {}
    except Exception as e:
        return _trash_failure(e, plan, source, motif, chemins, dry_run)
    return _trash_report(plan, source, motif, chemins, dry_run, faits)


def _trash_failure(e: Exception, plan: dict[str, Any] | None, source: str, motif: str,
                   chemins: list[str], dry_run: bool) -> dict[str, Any]:
    """
    Une erreur de la corbeille globale, journalisée avec ce qui a déjà changé : une
    erreur au milieu du geste laisse des torrents en corbeille, qu'il faut savoir.
    """
    etiquette = "[SIMULATION] " if dry_run else ""
    faits = (plan or {}).get("faits") or {}
    fait = []
    if faits.get("torrents"):
        fait.append(f"{faits['torrents']} torrent(s) mis en corbeille")
    if faits.get("corbeilles"):
        fait.append(f"{faits['corbeilles']} torrent(s) de corbeille ajouté(s)")
    resume = f"erreur après {' et '.join(fait)} : {e}" if fait else f"erreur, rien n'a été fait : {e}"
    log.error("  ⚠ %sCorbeille globale (%s) : %s", etiquette, motif, resume)
    rid = _corbeille_record(source, motif, chemins, dry_run, resume, plan)
    return {"id": rid, "resume": resume, "error": str(e), "moved": sum(faits.values())}


def _trash_report(plan: dict[str, Any], source: str, motif: str, chemins: list[str], dry_run: bool,
                  faits: dict[str, int]) -> dict[str, Any]:
    """Journalise une décision de la corbeille globale et la trace dans corbeille_log."""
    etiquette = "[SIMULATION] " if dry_run else ""
    resume = _trash_summary(plan)
    log.info("  %s🗑 Corbeille globale (%s) : %s", etiquette, motif, resume)
    for t in plan["torrents"][:_CORBEILLE_LOG_MAX]:
        log.info("      → torrent en corbeille : [%s] %s%s%s", t.get("category"), t.get("name"),
                 f" ({t['note']})" if t.get("note") else "",
                 f" (laissé en place : {t['garde']})" if t.get("garde") else "")
    for c in plan["corbeilles"][:_CORBEILLE_LOG_MAX]:
        log.info("      → torrent de corbeille : %s, %d vidéo(s)%s%s", c["nom"], c["videos"],
                 f" et {c['autres']} autre(s) fichier(s)" if c["autres"] else "",
                 f", plus les {c['fusion']} fichier(s) du torrent de corbeille qu'il remplace" if c.get("fusion") else "")
    for t in plan["restent"][:_CORBEILLE_LOG_MAX]:
        log.info("      → %s : %s : %s", "attend" if plan.get("repos") else "reste", t.get("name"), t.get("raison"))
    for raison, n in plan["gardes"].items():
        log.info("      → gardé : %d fichier(s), %s", n, raison)
    rid = _corbeille_record(source, motif, chemins, dry_run, resume, plan)
    return {"id": rid, "resume": resume, "plan": plan,
            "moved": (faits.get("torrents", 0) + faits.get("corbeilles", 0)) if faits else 0}


def rest_trash(dry_run: bool, en_file: set[str]) -> dict[str, Any]:
    """
    La corbeille globale au repos, pour /cleanup. Sans réponse de Jellyfin, rien :
    on ne saurait pas ce qu'il montre. En simulation, rien ne change.
    """
    etiquette = "[SIMULATION] " if dry_run else ""
    motif = "cleanup"
    plan = None
    try:
        lues = _jellyfin_libraries()
        if lues is None:
            resume = "rien à faire : Jellyfin ne répond pas, on ne sait pas ce qu'il montre"
            log.info("  %s🗑 Corbeille globale (%s) : %s", etiquette, motif, resume)
            return {"id": _corbeille_record("cleanup", motif, [], dry_run, resume, None), "resume": resume}
        with _db_lock:
            with get_db() as conn:
                geres = {r[0].lower() for r in conn.execute("SELECT torrent_hash FROM arr_managed") if r[0]}
        with qbit_session() as s:
            plan = _rest_plan(s, lues, en_file, geres)
            faits = _trash_apply(plan, s, "cleanup") if not dry_run else {}
    except Exception as e:
        return _trash_failure(e, plan, "cleanup", motif, [], dry_run)
    return _trash_report(plan, "cleanup", motif, [], dry_run, faits)


def _delete_gesture(chemins: list[str], source: str, motif: str, check_links: bool,
                    deleted_files: bool | None = None, dry_run: bool = False,
                    anciens: list[str] | None = None) -> int:
    """
    Le geste d'une suppression ou d'un upgrade, selon CORBEILLE_GLOBALE. Retourne le
    nombre de torrents déplacés ou ajoutés.

    `chemins` : les chemins réellement supprimés, et eux seuls — la corbeille globale
    traite tout ce qu'ils contiennent comme parti. `anciens` : les chemins que
    l'ancien geste reçoit, s'ils diffèrent (le dossier commun d'une suppression
    Jellyfin). En simulation, la corbeille globale lit l'index avant l'ancien geste,
    qui en retire les fichiers qu'il traite.
    """
    if CORBEILLE_GLOBALE == "active" and not dry_run:
        return int(global_trash(chemins, source, motif, False, deleted_files is False).get("moved") or 0)
    sim = global_trash(chemins, source, motif, True, deleted_files is False) if CORBEILLE_GLOBALE != "off" else None
    n = sum(move_torrents_for_path(c, check_links=check_links, source=source, dry_run=dry_run)
            for c in (anciens or chemins))
    if sim and sim.get("id"):
        with _db_lock:
            with get_db() as conn:
                conn.execute("UPDATE corbeille_log SET ancien = ? WHERE id = ?", (n, sim["id"]))
        log.info("  [SIMULATION] Corbeille globale (%s) : l'ancien geste a déplacé %d torrent(s)", motif, n)
    return n


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
_REGLES = {1: "débloquer la file", 2: "torrent mal nommé", 4: "pack incomplet", 5: "bonne saison",
           6: "téléchargement mort", 7: "téléchargement inutile"}


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
    trous = (mode(IMPORT_ASSIST_PACK_DRY_RUN or IMPORT_ASSIST_TROUS_DRY_RUN) if SEERR_URL and SEERR_API_KEY
             else "inactives (pas de Seerr)")
    r4 = (f"{mode(IMPORT_ASSIST_PACK_DRY_RUN)} (saisons trouées {trous})" if sonarr and PROWLARR_URL and PROWLARR_API_KEY
          else "signale seulement (pas de Prowlarr)" if sonarr else "inactive (pas de Sonarr)")
    r5 = mode(IMPORT_ASSIST_ACCORD_DRY_RUN) if sonarr and SEERR_URL and SEERR_API_KEY else "inactive (Seerr ou Sonarr non configuré)"
    return (f"{_regle(1)} {mode(IMPORT_ASSIST_DRY_RUN)}, {_regle(2)} {mode(IMPORT_ASSIST_NOM_DRY_RUN)} "
            f"(identité confirmée {mode(IMPORT_ASSIST_IDENTITE_DRY_RUN)}, import par titre d'épisode "
            f"{mode(IMPORT_ASSIST_NOM_DRY_RUN or IMPORT_ASSIST_TITRES_DRY_RUN) if sonarr else 'inactif (pas de Sonarr)'}), "
            f"{_regle(4)} {r4}, {_regle(5)} {r5}, {_regle(6)} {mode(IMPORT_ASSIST_MORT_DRY_RUN)} "
            f"(mort-nés après {IMPORT_ASSIST_MORT_JAMAIS_HEURES:g} h {mode(IMPORT_ASSIST_MORT_JAMAIS_DRY_RUN)}), "
            f"{_regle(7)} {mode(IMPORT_ASSIST_INUTILE_DRY_RUN)} (releases piégées {mode(IMPORT_ASSIST_PIEGE_DRY_RUN)})")


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


def _assist_recent(download_id: str, decisions: tuple[str, ...], dry_run: bool, duree: float) -> bool:
    """Un de ces verdicts a-t-il été rendu pour ce téléchargement depuis `duree` secondes ?"""
    with _db_lock:
        with get_db() as conn:
            return conn.execute(
                f"SELECT 1 FROM assist_log WHERE download_id = ? AND decision IN ({','.join('?' * len(decisions))}) "
                "AND dry_run = ? AND at > ?",
                (download_id, *decisions, int(dry_run), int(time.time() - duree)),
            ).fetchone() is not None


def _assist_last(download_id: str, decision: str, dry_run: bool) -> int | None:
    """Date du dernier verdict de ce type pour ce téléchargement, ou None."""
    with _db_lock:
        with get_db() as conn:
            return conn.execute(
                "SELECT MAX(at) FROM assist_log WHERE download_id = ? AND decision = ? AND dry_run = ?",
                (download_id, decision, int(dry_run)),
            ).fetchone()[0]


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

        # Les files d'abord. Une seule lecture par *arr : bloqués pour la règle 1, en
        # cours pour la règle 6.
        bloques: list[tuple[str, str, str, dict[str, list[dict[str, Any]]]]] = []
        en_cours: list[tuple[str, str, str, dict[str, list[dict[str, Any]]]]] = []
        for arr, url, cle in _assist_arrs():
            try:
                par_dl = _assist_queue(arr, url, cle, _ASSIST_STATES | _MORT_ETATS_FILE)
            except Exception as e:
                log.error("Assistant d'import : %s injoignable ou réponse inattendue (%s)", arr, e)
                continue
            bloque = {dl: r for dl, r in par_dl.items() if _assist_stuck(r[0])}
            cours = {dl: r for dl, r in par_dl.items() if dl not in bloque}
            if bloque:
                bloques.append((arr, url, cle, bloque))
            if cours:
                en_cours.append((arr, url, cle, cours))

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

        verdicts_morts, nouveaux_morts = run_dead_downloads(en_cours, ctx)
        verdicts += verdicts_morts
        nouveaux += nouveaux_morts
        verdicts_oublies, nouveaux_oublies = run_forgotten({dl for *_, p in bloques + en_cours for dl in p}, ctx)
        verdicts += verdicts_oublies
        nouveaux += nouveaux_oublies
        verdicts_trous, nouveaux_trous = run_season_holes(ctx)
        verdicts += verdicts_trous
        nouveaux += nouveaux_trous
        if nouveaux:
            # Chaque ligne au-dessus dit sa règle, et si elle n'est qu'une simulation.
            log.info(
                "=== Assistant d'import (%s) : %d téléchargement(s) bloqué(s), %d mort(s), %d nouveau(x) verdict(s) ===",
                source, sum(len(p) for *_, p in bloques), len(verdicts_morts), nouveaux,
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


def _queue_pages(url: str, cle: str, params: dict[str, str] | None = None) -> Iterator[dict[str, Any]]:
    """Toutes les lignes de la file d'un *arr, page par page."""
    page, lues = 1, 0
    while True:
        rep = _arr_request("GET", url, cle, "queue",
                           params={"page": page, "pageSize": _ASSIST_QUEUE_PAGE, **(params or {})}) or {}
        lot = rep.get("records") or []
        yield from lot
        lues += len(lot)
        if not lot or lues >= (rep.get("totalRecords") or 0):
            return
        page += 1


def _assist_blocked(url: str, cle: str, etats: set[str] | None = None) -> dict[str, list[dict[str, Any]]]:
    """Lignes de file d'un *arr dans ces états (les bloqués par défaut), regroupées par téléchargement."""
    etats = _ASSIST_STATES if etats is None else etats
    bloques: dict[str, list[dict[str, Any]]] = {}
    # Sans les éléments « inconnus » : quand Sonarr et Radarr partagent une
    # catégorie, chacun liste les téléchargements de l'autre, sans média.
    for r in _queue_pages(url, cle):
        if r.get("downloadId") and r.get("trackedDownloadState") in etats:
            bloques.setdefault(r["downloadId"].lower(), []).append(r)
    return bloques


def _assist_stuck(r: dict[str, Any]) -> bool:
    """
    L'*arr a tenté l'import et attend une intervention : `importBlocked`, ou
    `importPending` avec un avertissement. Il laisse dans ce second état une vidéo
    unique refusée (RejectedImportService) ou un dossier sans rien à importer, sans
    émettre aucun event. Sans avertissement, `importPending` n'est que l'attente de l'import.
    """
    etat = r.get("trackedDownloadState")
    return etat in _ASSIST_STATES or (etat == "importPending" and r.get("trackedDownloadStatus") == "warning")


def _assist_queue(arr: str, url: str, cle: str, etats: set[str]) -> dict[str, list[dict[str, Any]]]:
    """
    Comme _assist_blocked, en une seule lecture, avec tout téléchargement coincé
    (_assist_stuck), plus les téléchargements bloqués que la file montre sans média
    alors que cet *arr les a pris lui-même. Il a perdu leur média en relisant le
    seul nom du torrent, après un autre import du même film par exemple. Le média
    revient de l'historique du grab, et la ligne est marquée « orphelin ». Les
    lignes inconnues de l'autre *arr, dans une catégorie partagée, n'ont pas de
    grab ici : elles restent écartées.
    """
    cle_media = "movieId" if arr == "radarr" else "seriesId"
    inconnus = "includeUnknownMovieItems" if arr == "radarr" else "includeUnknownSeriesItems"
    par_dl: dict[str, list[dict[str, Any]]] = {}
    sans_media: dict[str, dict[str, Any]] = {}
    for r in _queue_pages(url, cle, {inconnus: "true"}):
        if not r.get("downloadId") or not (r.get("trackedDownloadState") in etats or _assist_stuck(r)):
            continue
        if r.get(cle_media):
            par_dl.setdefault(r["downloadId"].lower(), []).append(r)
        elif r.get("trackedDownloadState") in _ASSIST_STATES:
            sans_media.setdefault(r["downloadId"].lower(), r)
    for dl, r in sans_media.items():
        if dl in par_dl:
            continue
        try:
            grabs = [g for g in (_arr_request("GET", url, cle, "history",
                                              params={"downloadId": r["downloadId"], "pageSize": 200})
                                 or {}).get("records") or []
                     if g.get("eventType") == "grabbed" and g.get(cle_media)]
        except Exception as e:
            log.error("Assistant d'import : historique de %s illisible (%s)", r.get("title") or dl, e)
            continue
        if arr == "radarr" and grabs:
            par_dl[dl] = [dict(r, movieId=grabs[0]["movieId"], orphelin=True)]
        elif grabs:
            episodes = sorted({g["episodeId"] for g in grabs if g.get("episodeId")})
            if episodes:
                par_dl[dl] = [dict(r, seriesId=grabs[0]["seriesId"], episodeId=e, orphelin=True) for e in episodes]
    return par_dl


def _seed_obstacle(categorie: str, t: dict[str, Any], ctx: dict[str, Any]) -> str:
    """
    Ce qui empêche de sortir un torrent de la file en le passant dans la catégorie
    après import de son client, ou "" si rien ne l'empêche.
    """
    if not categorie:
        return "pas de catégorie après import sur le client"
    if categorie == t.get("category"):
        # Changer de catégorie ne sortirait pas le torrent de la file : l'action
        # serait rejouée chaque jour, recherche comprise.
        return f"déjà dans la catégorie après import {categorie}"
    if categorie not in ctx["categories"]:
        return f"catégorie {categorie} absente de qBittorrent"
    if t.get("auto_tmm") and ctx["categories"][categorie] != (t.get("save_path") or "").rstrip("/"):
        # Garde-fou : l'assistant ne doit rien déplacer sur le disque.
        return f"la catégorie {categorie} a un autre dossier"
    return ""


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
        # Ces lignes-là n'ont déclenché aucun event : le motif de l'*arr dit pourquoi elles restent.
        if records[0].get("trackedDownloadState") == "importPending":
            motif = next((m for s in records[0].get("statusMessages") or [] for m in s.get("messages") or []), "")
            raison = f"{raison} : {motif[:150]}" if motif else raison
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
        # Règle 7 : sans vidéo, un exécutable ou un script fait une release piégée.
        pieges = [n for n in _suspects(dl) if os.path.splitext(n)[1].lower() in _PIEGE_EXTENSIONS]
        if pieges:
            return _trapped(arr, url, cle, dl, records, t, ctx, verdict, pieges)
        return ecarte("aucune vidéo dans le torrent")
    lies = 0
    for v in videos:
        try:
            lies += os.stat(v).st_ino in inodes
        except OSError:
            continue
    categorie = _imported_category(clients, records[0].get("downloadClient") or "")
    if lies < len(videos):
        # Règle 4 : le pack complet ajouté à la place d'un pack partiel n'importe
        # seul que les épisodes manquants. Le remplacement se termine ici.
        remplacement = _pack_replacement(dl) if arr == "sonarr" else None
        if remplacement:
            return _pack_finish(url, cle, dl, titre, inodes, remplacement, ctx, verdict, ecarte, t, categorie)
        # Rien n'est importé. Règle 7 : l'*arr refuse chaque vidéo parce que la
        # bibliothèque a déjà mieux. Règle 2 : il ne reconnaît pas le nom du torrent.
        if not lies:
            refusees = _rejected_as_no_upgrade(records)
            if refusees:
                return _useless(arr, url, cle, dl, records, t, ctx, verdict,
                                f"{arr.capitalize()} refuse les {refusees} vidéo(s) : la bibliothèque a déjà mieux pour chacune")
            if _matched_by_id(records):
                return _misnamed(arr, url, cle, dl, records, t, media_id, ctx, verdict, ecarte)
        # Règle 2, import par titre d'épisode : ses vidéos utiles sont importées, les autres
        # n'apportent rien. Il quitte alors la file comme un import complet, et seede.
        if not _titles_done(arr, url, cle, dl, records, t, media_id, ctx, lies):
            return ecarte(f"{lies}/{len(videos)} vidéo(s) dans la bibliothèque")

    obstacle = _seed_obstacle(categorie, t, ctx)
    if obstacle:
        return ecarte(obstacle)
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

    # Règle 4 : ce téléchargement est-il lui-même un pack ajouté par la règle ? On
    # ne lui cherche jamais de remplaçant. Un pack en VO n'apportait que des trous :
    # les autres épisodes viennent du partiel, et ceux qu'il a comblés sont
    # aussitôt cherchés en français, pour remplacer la VO.
    ajoute = _pack_replacement(dl) if arr == "sonarr" else None
    recherche_vo: list[int] = []
    if ajoute is not None and ajoute["stage"] == "vo":
        manquants = []
        recherche_vo = sorted(json.loads(ajoute["episodes"] or "[]"))[:_ASSIST_MAX_SEARCH]

    # Règle 4, filet après import : un pack de saison qui n'a pas tout apporté est
    # remplacé par un pack complet, plutôt que de chercher ses épisodes un à un.
    pack: dict[str, Any] | None = None
    note = ""
    if manquants and not deja_vu and ajoute is None:
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
            seulement = _pack_files_for(pack, set(manquants)) if pack.get("vo") else None
            _qbit_add(pack, pack["category"], t, seulement=seulement)
            _pack_register(media_id, pack["season"], dl, pack, "vo" if pack.get("vo") else "import", pack["episodes"])
        except Exception as e:
            remplace, note = False, f"règle 4 : ajout du pack complet {pack['title']} impossible : {e}"
            log.error("  ⚠ [%s] [%s] %s : %s", _regle(4), arr, titre, note)

    recherche = manquants if len(manquants) <= _ASSIST_MAX_SEARCH and not remplace else []
    resume = (f"{lies}/{len(videos)} vidéo(s) déjà dans la bibliothèque"
              + (", les autres n'apportent rien (import par titre d'épisode)" if lies < len(videos) else "")
              + f" → {categorie}")
    if remplace and pack.get("vo"):
        resume += (f", règle 4 : les {len(manquants)} épisode(s) manquant(s) pris en VO dans {pack['title']}, "
                   "seuls leurs fichiers téléchargés")
    elif remplace and pack.get("combler"):
        resume += (f", règle 4 : pack complet {pack['title']} pour combler les {len(manquants)} épisode(s) "
                   "manquant(s), moins bien noté : les épisodes déjà là restent")
    elif remplace:
        resume += f", règle 4 : pack complet {pack['title']} à la place des {len(manquants)} épisode(s) manquant(s)"
    elif recherche:
        resume += f", recherche de {len(recherche)} épisode(s) manquant(s)"
    elif manquants:
        resume += (f", {len(manquants)} épisodes manquants : trop pour une recherche "
                   "automatique, à lancer depuis Sonarr")
    if recherche_vo:
        resume += f", règle 4 : recherche en français des {len(recherche_vo)} épisode(s) pris en VO"
    if note:
        resume += f" ({note})"
    elif pack is not None and not remplace and pack.get("vo"):
        resume += f" (règle 4 en simulation : prendrait les épisodes manquants en VO dans {pack['title']})"
    elif pack is not None and not remplace:
        resume += f" (règle 4 en simulation : remplacerait par le pack complet {pack['title']})"
    if note_accord:
        resume += f" ({note_accord})"
    recherche = recherche + [i for i in recherche_vo if i not in recherche]
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
            log.error("  ⚠ [%s] [%s] %s : recherche des épisodes non lancée (%s), "
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


# ── Assistant d'import, règle 2 : torrent mal nommé ─────────────────────────
#
# Au bout du téléchargement, Sonarr et Radarr reconnaissent le média d'après le
# nom du torrent dans qBittorrent, relu à chaque rafraîchissement, et non
# d'après le titre de la release. Un nom qui ne dit rien
# (« Resident.Evil.2026.2026.1080p… ») est rattaché par l'historique du grab.
# Si ce grab l'avait été par l'identifiant de l'indexeur, l'import est bloqué
# (« matched to movie by ID ») : un identifiant faux ferait sinon importer un
# autre média. La règle renomme le torrent avec le titre de la release, à
# condition que ce titre désigne à lui seul le même média. L'*arr importe alors
# lui-même, avec ses contrôles habituels. Seul le nom affiché change : aucun
# fichier ne bouge.

_NOM_BLOCAGE = re.compile(r"matched to (movie|series) by id", re.IGNORECASE)
# Exécutables, scripts, raccourcis et archives : une fausse release les glisse à
# côté d'une vidéo leurre. Sonarr et Radarr ne les signalent que si rien d'autre
# n'est importé ; renommer un tel torrent ferait importer le leurre. Sans aucune
# vidéo, un exécutable fait une release piégée (règle 7) ; une archive seule non :
# une release de scène peut être en RAR.
_PIEGE_EXTENSIONS = frozenset({
    ".exe", ".msi", ".bat", ".cmd", ".com", ".sh", ".ps1", ".scr", ".pif", ".lnk", ".url", ".vbs", ".vbe",
    ".js", ".jse", ".wsf", ".wsh", ".hta", ".jar", ".dll", ".cpl", ".reg", ".apk",
})
_ARCHIVE_EXTENSIONS = frozenset({".zip", ".zipx", ".rar", ".r00", ".7z", ".arj", ".lzh", ".gz", ".bz2", ".tar", ".tgz"})
_NOM_SUSPECTS = _PIEGE_EXTENSIONS | _ARCHIVE_EXTENSIONS
# L'*arr relit le nom du torrent à son rafraîchissement suivant, chaque minute :
# passé ce délai, le renommage n'a pas suffi.
_NOM_DELAI = 600


def _matched_by_id(records: list[dict[str, Any]]) -> bool:
    return any(_NOM_BLOCAGE.search(m) for r in records for s in r.get("statusMessages") or []
               for m in s.get("messages") or [])


def _sans_extension(nom: str) -> str:
    base, ext = os.path.splitext(nom)
    return (base if ext.lower() in VIDEO_EXTENSIONS else nom).lower()


def _suspects(dl: str) -> list[str]:
    """Fichiers suspects du torrent, d'après sa liste dans qBittorrent, cochés ou non."""
    with qbit_session() as s:
        r = s.get(f"{QBIT_URL}/api/v2/torrents/files", params={"hash": dl}, timeout=10)
        r.raise_for_status()
        noms = [f.get("name") or "" for f in r.json()]
    return [os.path.basename(n) for n in noms if os.path.splitext(n)[1].lower() in _NOM_SUSPECTS]


# ── Assistant d'import, règle 2 : identité confirmée ────────────────────────
#
# Sonarr et Radarr n'importent seuls un téléchargement rattaché à son média par
# l'identifiant de l'indexeur que si son nom redonne, après leur normalisation,
# exactement un titre qu'ils connaissent. Le moindre écart bloque : « N.A » lu
# comme un sigle quand le titre dit « n'a », « Film 14 » en plus, le titre de
# l'épisode collé à celui de la série, une série enregistrée avec son année. Dans
# tous les cas relevés, l'identifiant était juste. L'assistant confirme alors
# l'identité sans lui : tous les mots utiles d'un titre connu du média (de l'*arr,
# ou de Seerr en français et en anglais), dans l'ordre, dans la partie titre du
# nom ; l'année du nom, s'il en porte une, à un an près ; des vidéos d'une taille
# plausible pour leur qualité. Le nom est refait : sa partie titre devient le
# titre de l'*arr, le reste (année, numérotation, qualité, langue, groupe) ne
# bouge pas. L'*arr relit ce nom avant tout renommage : il doit y voir le même
# média, la même qualité et les épisodes de la file. Il importe ensuite lui-même,
# avec tous ses contrôles.

# Articles et liaisons : ils varient d'une langue ou d'une release à l'autre.
_ID_VIDES = frozenset({"le", "la", "les", "un", "une", "des", "de", "du", "et", "au", "aux", "en",
                       "the", "an", "of", "and", "or", "on", "to", "in"})
# Ce qui suit la partie titre d'un nom de release : année, numérotation, qualité, source, langue.
_ID_SUITE = re.compile(
    r"^(19\d\d|20\d\d|s\d{1,2}(e\d{1,3})*|e\d{1,3}|saison|season|integrale|complete|\d{3,4}p|web|webrip|webdl|"
    r"bluray|bdrip|brrip|hdtv|dvdrip|remux|x264|x265|h264|h265|hevc|avc|multi|french|truefrench|vff|vfq|vf2|"
    r"vostfr|subfrench|vo|dual|amzn|nf|dsnp|hmax|cr)$")
_ID_NUMERO = re.compile(r"(?<![a-z0-9])(s\d{1,2}(e\d{1,3})*|\d{1,2}x\d{2,3})(?![a-z0-9])", re.IGNORECASE)
_ID_ANNEE_OU_NUMERO = re.compile(r"(?<![a-z0-9])((19|20)\d\d|s\d{1,2}(e\d{1,3})*)(?![a-z0-9])", re.IGNORECASE)
# Source de chaque qualité *arr, ajoutée au nom refait si l'*arr y lisait une autre qualité.
_ID_SOURCES = (("remux", "BluRay Remux"), ("bluray", "BluRay"), ("webdl", "WEB-DL"), ("webrip", "WEBRip"),
               ("hdtv", "HDTV"), ("dvd", "DVDRip"))


def _id_mots(texte: str) -> list[str]:
    """Mots d'un texte, en minuscules, sans accents ni ponctuation."""
    s = unicodedata.normalize("NFKD", texte or "").encode("ascii", "ignore").decode().lower()
    return re.findall(r"[a-z0-9]+", s)


def _id_utiles(mots: list[str]) -> list[str]:
    """Les mots qui distinguent un titre : les nombres, et les mots de deux lettres ou plus hors articles."""
    return [m for m in mots if m.isdigit() or (len(m) > 1 and m not in _ID_VIDES)]


def _id_zone(nom: str) -> list[str]:
    """Mots de la partie titre d'un nom de release : ce qui précède l'année, la numérotation ou la qualité."""
    mots = _id_mots(nom)
    for i, m in enumerate(mots):
        if i and _ID_SUITE.match(m):
            return mots[:i]
    return mots


def _id_dans_l_ordre(cherches: list[str], mots: list[str]) -> bool:
    reste = iter(mots)
    return all(any(c == m for m in reste) for c in cherches)


def _id_annees(nom: str) -> list[int]:
    return [int(a) for a in re.findall(r"(?<!\d)(19\d\d|20\d\d)(?!\d)", nom or "")]


def _id_titre_reconnu(nom: str, titres: list[str], annee: int | None) -> str | None:
    """
    Le titre connu dont tous les mots utiles sont, dans l'ordre, dans la partie titre
    du nom ; None sinon. L'année du nom, s'il en porte une, doit être celle du média
    à un an près. Un titre d'un seul mot utile exige cette année dans le nom.
    """
    annees = _id_annees(nom)
    if annees and annee and not any(abs(a - annee) <= 1 for a in annees):
        return None
    zone = _id_utiles(_id_zone(nom))
    for t in titres:
        cherches = _id_utiles(_id_mots(re.sub(r"\(\d{4}\)", " ", t or "")))
        if not cherches or (len(cherches) == 1 and not annees):
            continue
        if _id_dans_l_ordre(cherches, zone):
            return t
    return None


def _id_nom_refait(nom: str, titre_arr: str, annee: int | None = None, code: str = "") -> str:
    """
    Le nom de release dont la partie titre devient le titre de l'*arr ; le reste est
    gardé tel quel. Pour un film, `annee` est ajoutée si le reste ne commence pas par
    une année ; pour une série, `code` (S01, S01E02) si le nom n'a pas de numérotation.
    """
    base, ext = os.path.splitext(nom or "")
    origine = base if ext.lower() in VIDEO_EXTENSIONS else (nom or "")
    debut = next((m.start() for i, m in enumerate(re.finditer(r"[^\W_]+", origine))
                  if i and _ID_SUITE.match((_id_mots(m.group()) or [""])[0])), len(origine))
    reste = origine[debut:]
    if annee and not re.match(r"(19|20)\d\d", reste):
        reste = f"{annee} {reste}"
    if code and not _ID_NUMERO.search(reste):
        reste = f"{code} {reste}"
    titre = re.sub(r"[:/\\]", " ", re.sub(r"\((\d{4})\)", r"\1", titre_arr or ""))
    return re.sub(r"\s+", " ", f"{titre} {reste}").strip()


def _id_avec_source(nom_refait: str, qualite: str) -> str | None:
    """Le nom refait, avec la source de `qualite` juste après l'année ou la numérotation ; None si elle est inconnue."""
    q = (qualite or "").lower().replace("-", "")
    source = next((s for cle_q, s in _ID_SOURCES if cle_q in q), None)
    if not source:
        return None
    m = _ID_ANNEE_OU_NUMERO.search(nom_refait)
    if not m:
        return f"{nom_refait} {source}"
    return f"{nom_refait[:m.end()]} {source}{nom_refait[m.end():]}"


def _id_titres(arr: str, media: dict[str, Any], ctx: dict[str, Any]) -> list[str]:
    """Titres connus du média : ceux de l'*arr, puis ceux de Seerr (TMDB) en français et en anglais."""
    titres = [media.get("title"), media.get("originalTitle")]
    titres += [a.get("title") for a in media.get("alternateTitles") or []]
    tmdb = media.get("tmdbId")
    if tmdb and SEERR_URL and SEERR_API_KEY:
        cache = ctx.setdefault("titres_seerr", {})
        if (arr, tmdb) not in cache:
            trouves: list[str] = []
            for langue in ("fr", "en"):
                try:
                    info = _seerr_get(f"{'movie' if arr == 'radarr' else 'tv'}/{tmdb}", language=langue) or {}
                except Exception as e:
                    log.warning("  Titres Seerr de %s illisibles (%s)", media.get("title"), e)
                    continue
                trouves += [info.get("title") or info.get("name"), info.get("originalTitle") or info.get("originalName")]
            cache[(arr, tmdb)] = trouves
        titres += cache[(arr, tmdb)]
    return list(dict.fromkeys(t for t in titres if t))


def _id_minimum(url: str, cle: str, qualite: str, ctx: dict[str, Any]) -> float:
    """Taille minimale de cette qualité dans l'*arr, en Mo par minute ; 0 si elle n'en a pas."""
    cache = ctx.setdefault("qualites", {})
    if url not in cache:
        cache[url] = {d["quality"]["name"]: d.get("minSize") or 0
                      for d in _arr_request("GET", url, cle, "qualitydefinition") or [] if d.get("quality")}
    return float(cache[url].get(qualite) or 0)


def _id_taille(videos: list[str]) -> int:
    total = 0
    for v in videos:
        try:
            total += os.path.getsize(v)
        except OSError:
            continue
    return total


def _id_code(url: str, cle: str, serie_id: int, records: list[dict[str, Any]]) -> str:
    """S01, ou S01E02 pour un seul épisode : la numérotation des épisodes de la file."""
    voulus = {r.get("episodeId") for r in records if r.get("episodeId")}
    eps = [e for e in _arr_request("GET", url, cle, "episode", params={"seriesId": serie_id}) or []
           if e.get("id") in voulus]
    saisons = {e.get("seasonNumber") for e in eps}
    if len(saisons) != 1:
        return ""
    s = saisons.pop()
    return f"S{s:02d}E{eps[0]['episodeNumber']:02d}" if len(eps) == 1 else f"S{s:02d}"


def _id_confirmee(
    arr: str, url: str, cle: str, media_id: int, records: list[dict[str, Any]], t: dict[str, Any],
    release: str, ctx: dict[str, Any],
) -> tuple[str, str, dict[str, Any]] | str:
    """
    Identité confirmée (voir plus haut) : (nom refait, preuve, lecture de ce nom par
    l'*arr), ou la raison pour laquelle l'identité reste à vérifier à la main.
    """
    media = _arr_request("GET", url, cle, f"{'movie' if arr == 'radarr' else 'series'}/{media_id}") or {}
    annee = media.get("year") or None
    titres = _id_titres(arr, media, ctx)
    nom, reconnu = next(((n, r) for n in dict.fromkeys(x for x in (t.get("name") or "", release) if x)
                         for r in [_id_titre_reconnu(n, titres, annee)] if r), ("", None))
    if not reconnu:
        return (f"aucun titre connu de {media.get('title')} dans le nom, à la bonne année : seul l'identifiant "
                "de l'indexeur relie ce téléchargement au média")
    videos = _torrent_videos(t) or []
    if not videos:
        return "aucune vidéo dans le torrent"
    info = "parsedMovieInfo" if arr == "radarr" else "parsedEpisodeInfo"
    lu = _arr_request("GET", url, cle, "parse", params={"title": nom}) or {}
    qualite = (((lu.get(info) or {}).get("quality") or {}).get("quality") or {}).get("name") or ""
    duree = (media.get("runtime") or 0) * (max(1, len(records)) if arr == "sonarr" else 1)
    if duree and qualite:
        mini = _id_minimum(url, cle, qualite, ctx)
        par_minute = _id_taille(videos) / 1e6 / duree
        if mini and par_minute < mini:
            return (f"vidéos trop petites pour du {qualite} : {par_minute:.1f} Mo par minute attendue, "
                    f"{arr.capitalize()} en demande au moins {mini:g}")
    code = _id_code(url, cle, media_id, records) if arr == "sonarr" and not _ID_NUMERO.search(nom) else ""
    refait = _id_nom_refait(nom, media.get("title") or "", annee if arr == "radarr" else None, code)
    attendus = {r.get("episodeId") for r in records if r.get("episodeId")}
    raison = ""
    for essai in dict.fromkeys(x for x in (refait, _id_avec_source(refait, qualite)) if x):
        p = _arr_request("GET", url, cle, "parse", params={"title": essai}) or {}
        cible = p.get("movie" if arr == "radarr" else "series") or {}
        lue = (((p.get(info) or {}).get("quality") or {}).get("quality") or {}).get("name") or ""
        if cible.get("id") != media_id:
            raison = f"{arr.capitalize()} ne relit pas le nom refait « {essai} » comme {media.get('title')}"
        elif arr == "sonarr" and not attendus <= {e.get("id") for e in p.get("episodes") or []}:
            raison = f"le nom refait « {essai} » ne couvre pas les épisodes de la file"
        elif lue != qualite:
            raison = f"le nom refait « {essai} » changerait la qualité lue ({qualite} → {lue})"
        else:
            an = _id_annees(nom)
            preuve = f"identité confirmée, « {reconnu} » dans « {nom} »" + (f", année {an[-1]}" if an else "")
            return essai, preuve, p
    return raison


# ── Assistant d'import, règle 2 : import par titre d'épisode ────────────────
#
# Sonarr range une vidéo d'après le numéro de son nom. Une release qui numérote
# autrement que TVDB est donc mal rangée : la version Netflix de Pokémon a retiré
# trois épisodes interdits (S01E18, E35, E38), et décale de 1 à 3 tous les
# suivants. « Tentacool and Tentacruel », 18e chez Netflix, finirait sous « Beauty
# and the Beach ». Quand Sonarr ne relie la release que par la prise, mais que ses
# vidéos portent les titres exacts d'épisodes, le titre fait foi : il prouve
# l'identité, et range chaque vidéo à sa place.

# Part minimale des vidéos reconnues par leur titre : en dessous, rien n'est importé.
_TITRES_PART_MIN = 0.8
# Un titre d'épisode plus court (lettres et chiffres) ne prouve rien.
_TITRES_LONGUEUR_MIN = 6
_TITRES_EPISODE_RE = re.compile(r"S(\d{1,2})E(\d{1,3})", re.IGNORECASE)
# Mots de scène qui peuvent précéder le titre : « S01E52.FiNAL.The.Breeding.Center.Secret ».
_TITRES_TETE_RE = re.compile(r"^(?:final|proper|repack|internal)+")


def _titre_cle(texte: str) -> str:
    """Titre comparable : sans casse, accents ni ponctuation, « & » lu « and »."""
    return _episode_title_key((texte or "").replace("&", " and "))


def _episode_by_title(nom: str, par_cle: dict[str, list[dict[str, Any]]]) -> dict[str, Any] | None:
    """
    L'épisode dont le titre ouvre ce qui suit SxxEyy dans le nom de la vidéo : le plus
    long des titres qui conviennent, s'il désigne un seul épisode (à défaut, un seul de
    la saison du nom). None sans titre reconnu : pour une vidéo de plusieurs épisodes
    (« S01E21-E22 »), le second numéro empêche déjà de lire un titre.
    """
    base = os.path.splitext(os.path.basename(nom))[0]
    m = _TITRES_EPISODE_RE.search(base)
    if not m:
        return None
    reste = _titre_cle(base[m.end():])
    essais = {reste, _TITRES_TETE_RE.sub("", reste)}
    titre = max((k for k in par_cle if len(k) >= _TITRES_LONGUEUR_MIN and any(e.startswith(k) for e in essais)),
                key=len, default="")
    episodes = par_cle.get(titre) or []
    if len(episodes) > 1:
        episodes = [e for e in episodes if e.get("seasonNumber") == int(m.group(1))]
    return episodes[0] if len(episodes) == 1 else None


def _titles_plan(
    url: str, cle: str, dl: str, records: list[dict[str, Any]], t: dict[str, Any], media_id: int,
    ctx: dict[str, Any],
) -> dict[str, Any] | None:
    """
    Ce que l'import par titre ferait de ce téléchargement, vidéo par vidéo. None si
    les titres ne suffisent pas : moins de _TITRES_PART_MIN des vidéos reconnues, ou
    deux vidéos pour un même épisode. Sinon, parmi les reconnues (épisodes de la prise,
    hors saison écartée par la règle 5) :
    - `vides` : l'épisode n'a pas de fichier ;
    - `mieux` : la vidéo est d'une qualité de rang supérieur dans le profil (480p →
      1080p) : elle remplace le fichier en place, comme un upgrade ;
    - `deja` : le fichier en place est d'une qualité au moins égale ;
    - `doute` : Sonarr ne permet pas de comparer (qualité inconnue du profil, pas de
      proposition d'import) : le fichier en place reste, et rien n'est jeté.
    La qualité d'une vidéo est celle que Sonarr lui donne à l'import (proposition
    d'import manuel). Le score de formats ne départage pas : celui d'un fichier en
    place dépend de son nom. Les fichiers YLAMARCH de Pokémon, en français mais
    renommés sans « VFF », comptent 0, et la version Netflix MULTi 85 : comparer les
    scores remplacerait 50 bons fichiers français.
    """
    videos = _torrent_videos(t) or []
    if not videos:
        return None
    episodes = _arr_request("GET", url, cle, "episode", params={"seriesId": media_id}) or []
    par_cle: dict[str, list[dict[str, Any]]] = {}
    for e in episodes:
        k = _titre_cle(e.get("title") or "")
        if len(k) >= _TITRES_LONGUEUR_MIN:
            par_cle.setdefault(k, []).append(e)
    attendus = {r["episodeId"] for r in records if r.get("episodeId")}
    ecartees = (ctx.get("ecartees") or {}).get(media_id) or set()
    reconnus: dict[str, dict[str, Any]] = {}
    for v in videos:
        e = _episode_by_title(v, par_cle)
        if e is not None and e["id"] in attendus and e.get("seasonNumber") not in ecartees:
            reconnus[v] = e
    if len(reconnus) < max(1, -(-len(videos) * _TITRES_PART_MIN // 1)):
        return None
    if len({e["id"] for e in reconnus.values()}) < len(reconnus):
        return None  # deux vidéos pour un même épisode : les titres ne départagent rien
    propositions = {it.get("path"): it for it in _arr_request(
        "GET", url, cle, "manualimport", timeout=300,
        params={"downloadId": dl.upper(), "filterExistingFiles": "false"}) or []}
    fichiers = {f["id"]: f for f in _arr_request("GET", url, cle, "episodefile", params={"seriesId": media_id}) or []}
    rang = _quality_rank(url, cle, (_arr_request("GET", url, cle, f"series/{media_id}") or {}).get("qualityProfileId"))

    def niveau(objet: dict[str, Any]) -> int | None:
        return rang.get(((objet.get("quality") or {}).get("quality") or {}).get("id"))

    plan: dict[str, Any] = {"videos": videos, "reconnus": reconnus, "propositions": propositions,
                            "vides": {}, "mieux": {}, "deja": {}, "doute": {}}
    for v, e in reconnus.items():
        proposition = propositions.get(v) or {}
        if not proposition.get("quality"):
            plan["doute"][v] = e  # sans qualité, Sonarr n'importerait pas
        elif not e.get("hasFile"):
            plan["vides"][v] = e
        else:
            r_video, r_place = niveau(proposition), niveau(fichiers.get(e.get("episodeFileId")) or {})
            if r_video is None or r_place is None:
                plan["doute"][v] = e
            elif r_video > r_place:
                plan["mieux"][v] = e
            else:
                plan["deja"][v] = e
    return plan


def _titles_done(arr: str, url: str, cle: str, dl: str, records: list[dict[str, Any]], t: dict[str, Any],
                 media_id: int, ctx: dict[str, Any], lies: int) -> bool:
    """
    Un téléchargement importé en partie par titre d'épisode n'a-t-il plus rien à
    apporter ? Ses vidéos utiles sont dans la bibliothèque, les autres ne valent pas
    les fichiers en place. Il peut alors quitter la file comme un import complet.
    """
    if arr != "sonarr" or not lies or _assist_last(dl, "import-titles", False) is None:
        return False
    plan = _titles_plan(url, cle, dl, records, t, media_id, ctx)
    return plan is not None and not plan["vides"] and not plan["mieux"]


def _import_by_titles(
    url: str, cle: str, dl: str, records: list[dict[str, Any]], t: dict[str, Any], media_id: int,
    ctx: dict[str, Any], verdict: dict[str, Any],
) -> tuple[dict[str, Any], bool] | None:
    """
    Règle 2, import par titre d'épisode : chaque vidéo reconnue par son titre est
    importée (hardlink) sous cet épisode, s'il est vide ou si elle vaut mieux que le
    fichier en place (_titles_plan). Si toutes ses vidéos sont reconnues et qu'aucune
    n'apporte rien, le téléchargement est confié à la règle 7. None si les titres ne
    suffisent pas.
    """
    dry = ctx["apercu"] or IMPORT_ASSIST_NOM_DRY_RUN or IMPORT_ASSIST_TITRES_DRY_RUN
    titre = records[0].get("title") or dl
    plan = _titles_plan(url, cle, dl, records, t, media_id, ctx)
    if plan is None:
        return None
    videos, reconnus = plan["videos"], plan["reconnus"]
    a_importer = {**plan["vides"], **plan["mieux"]}
    if not a_importer:
        if len(reconnus) < len(videos) or plan["doute"]:
            return None  # une vidéo non reconnue, ou impossible à comparer, apporte peut-être quelque chose
        # Tant que l'import par titre simule, la règle 7 qui en découle simule aussi.
        return _useless("sonarr", url, cle, dl, records, t, dict(ctx, apercu=True) if dry else ctx, verdict,
                        f"ses {len(videos)} vidéo(s), reconnues au titre exact de leur épisode, n'apportent rien : "
                        "chaque épisode a déjà un fichier de qualité au moins égale")
    decales = [(nom[m.start():], e) for v, e in sorted(reconnus.items())
               for nom in [os.path.splitext(os.path.basename(v))[0]] if (m := _TITRES_EPISODE_RE.search(nom))
               and (int(m.group(1)), int(m.group(2))) != (e.get("seasonNumber"), e.get("episodeNumber"))]
    details = (f"{len(reconnus)}/{len(videos)} vidéo(s) reconnue(s) au titre exact de leur épisode, "
               f"{len(plan['vides'])} importée(s) dans un épisode vide")
    if plan["mieux"]:
        _, e = sorted(plan["mieux"].items())[0]
        details += (f", {len(plan['mieux'])} à la place d'un fichier moins bon "
                    f"(S{e['seasonNumber']:02d}E{e['episodeNumber']:02d} « {e.get('title')} »)")
    if plan["deja"]:
        details += f", {len(plan['deja'])} laissée(s) : le fichier en place est de qualité au moins égale"
    if plan["doute"]:
        details += f", {len(plan['doute'])} laissée(s) faute de pouvoir les comparer au fichier en place"
    if decales:
        nom, e = decales[0]
        details += (f" ; {len(decales)} rangée(s) sous un autre numéro que celui de leur nom "
                    f"(« {nom[:50]} » → S{e['seasonNumber']:02d}E{e['episodeNumber']:02d} « {e.get('title')} »)")
    if len(reconnus) < len(videos):
        details += f" ; {len(videos) - len(reconnus)} sans épisode reconnu parmi ceux de la prise, à importer à la main"
    verdict.update(decision="import-titles", reason=details, files=len(a_importer))
    if _assist_seen(dl, "import-titles", dry):
        return verdict, False
    if not dry:
        entrees = [{"path": v, "seriesId": media_id, "episodeIds": [e["id"]], "quality": p["quality"],
                    "languages": p.get("languages") or [], "releaseGroup": p.get("releaseGroup") or "",
                    "indexerFlags": p.get("indexerFlags") or 0, "releaseType": "singleEpisode",
                    "downloadId": dl.upper()}
                   for v, e in sorted(a_importer.items()) for p in [plan["propositions"][v]]]
        # « copy » : hardlink depuis le torrent, qui continue de seeder. Un fichier en place
        # moins bon est remplacé, comme lors d'un upgrade.
        _arr_request("POST", url, cle, "command", json={"name": "ManualImport", "importMode": "copy", "files": entrees})
    _assist_record(dl, "sonarr", titre, "import-titles", details, dry, 2)
    _assist_log(2, "✓", "sonarr", titre, details, dry)
    return verdict, True


def _misnamed(
    arr: str, url: str, cle: str, dl: str, records: list[dict[str, Any]], t: dict[str, Any],
    media_id: int, ctx: dict[str, Any], verdict: dict[str, Any], ecarte: Any,
) -> tuple[dict[str, Any], bool]:
    """
    Règle 2 sur un téléchargement bloqué « matched by ID », dont rien n'est dans la
    bibliothèque. Le torrent prend le titre de la release, s'il désigne à lui seul ce
    média ; sinon un nom refait, si l'identité est confirmée (_id_confirmee), sous
    l'interrupteur de simulation de l'identité confirmée.
    """
    dry = ctx["apercu"] or IMPORT_ASSIST_NOM_DRY_RUN
    titre = records[0].get("title") or dl

    def signaler(decision: str, details: str) -> tuple[dict[str, Any], bool]:
        verdict.update(decision=decision, reason=details)
        if _assist_seen(dl, decision, dry):
            return verdict, False
        _assist_record(dl, arr, titre, decision, details, dry, 2)
        _assist_log(2, "⚠", arr, titre, details, dry)
        return verdict, True

    grabs = [g for g in (_arr_request("GET", url, cle, "history",
                                      params={"downloadId": records[0]["downloadId"], "pageSize": 50})
                         or {}).get("records") or []
             if g.get("eventType") == "grabbed" and g.get("sourceTitle")]
    if not grabs:
        return ecarte("aucun grab dans l'historique : rien ne prouve le média", 2, dry)
    release = max(grabs, key=lambda g: g.get("date") or "")["sourceTitle"]
    nom_actuel = t.get("name") or ""
    renomme = _assist_last(dl, "rename", False)

    suspects = _suspects(dl)
    if suspects:
        return signaler("rename-suspect", f"{', '.join(suspects[:3])} dans le torrent : release suspecte, "
                                          "rien n'est renommé")

    # Le nom à donner : le titre de la release, s'il désigne ce média sans l'aide de
    # l'identifiant ; sinon un nom refait, une fois l'identité confirmée sans lui.
    cible_nom, p, preuve = "", {}, ""
    if _sans_extension(release) != _sans_extension(nom_actuel):
        lu = _arr_request("GET", url, cle, "parse", params={"title": release}) or {}
        autre = lu.get("movie" if arr == "radarr" else "series") or {}
        if autre.get("id") == media_id:
            cible_nom, p = release, lu
        elif autre:
            return ecarte(f"le titre de la release désigne un autre média ({autre.get('title')})", 2, dry)
    elif renomme is not None:
        cible_nom = release  # déjà renommé avec le titre de la release : en attente, ou bloqué malgré tout
    if not cible_nom:
        confirme = _id_confirmee(arr, url, cle, media_id, records, t, release, ctx)
        if isinstance(confirme, str):
            # Sans titre de la série dans le nom, les titres des épisodes peuvent suffire.
            par_titres = _import_by_titles(url, cle, dl, records, t, media_id, ctx, verdict) if arr == "sonarr" else None
            return par_titres or ecarte(confirme, 2, dry)
        cible_nom, preuve, p = confirme
        dry = dry or IMPORT_ASSIST_IDENTITE_DRY_RUN
        # Tant que l'identité confirmée est en simulation, rien de ce qui en découle n'agit.
        if IMPORT_ASSIST_IDENTITE_DRY_RUN:
            ctx = dict(ctx, apercu=True)

    if _sans_extension(cible_nom) == _sans_extension(nom_actuel):
        if renomme is None:
            return ecarte(f"le torrent porte déjà le nom « {cible_nom} », que {arr.capitalize()} bloque pourtant", 2, dry)
        if time.time() - renomme < _NOM_DELAI:
            verdict.update(decision="rename-pending", reason="renommé à l'instant, pas encore relu par l'*arr")
            return verdict, False
        return signaler("rename-blocked", f"renommé {cible_nom}, mais {arr.capitalize()} le bloque toujours : "
                                          "import à faire à la main")

    cible = p.get("movie" if arr == "radarr" else "series") or {}
    if arr == "sonarr":
        attendus = {r["episodeId"] for r in records if r.get("episodeId")}
        if not attendus <= {e.get("id") for e in p.get("episodes") or []}:
            return ecarte("le titre de la release ne couvre pas les épisodes de la file", 2, dry)
        saison = (p.get("parsedEpisodeInfo") or {}).get("seasonNumber")
        if saison in ((ctx.get("ecartees") or {}).get(media_id) or set()):
            return ecarte(f"règle 5 : la saison {saison} demandée est une autre saison TVDB", 2, dry)
    q = _arr_request("GET", url, cle, "parse", params={"title": t.get("name") or ""}) or {}
    actuel = q.get("movie" if arr == "radarr" else "series") or {}
    if actuel and actuel.get("id") != media_id:
        return ecarte(f"le nom du torrent désigne un autre média ({actuel.get('title')})", 2, dry)

    # Règle 7 : un autre import a pu passer devant entre-temps — l'*arr refuserait alors celui-ci.
    mieux = _library_better(arr, url, cle, media_id, records, p)
    if mieux:
        return _useless(arr, url, cle, dl, records, t, ctx, verdict,
                        f"la bibliothèque a déjà mieux ({mieux}) que la release {release} ({_release_desc(arr, p)})")
    # Un orphelin a perdu son média dans l'*arr : relu sous le bon nom, il serait reconnu,
    # mais l'import partirait sans film ni série (Import() n'utilise que celui du suivi).
    if records[0].get("orphelin"):
        return signaler("rename-orphan", f"{arr.capitalize()} a détaché ce téléchargement de son média : "
                                         "renommer ne suffirait plus, import manuel à faire")

    nom = f"{cible.get('title')} ({cible.get('year')})" if arr == "radarr" else cible.get("title")
    preuve = preuve or f"le titre de la release désigne bien {nom}"
    details = f"{preuve} : torrent renommé {cible_nom} → {arr.capitalize()} l'importera seul"
    verdict.update(decision="rename", reason=details, release=cible_nom)
    if _assist_seen(dl, "rename", dry):
        return verdict, False
    if not dry:
        try:
            _qbit_post("rename", {"hash": dl, "name": cible_nom})
        except Exception as e:
            return signaler("rename-failed", f"renommage en {cible_nom} impossible ({e}) : import à faire à la main")
    _assist_record(dl, arr, titre, "rename", details, dry, 2)
    _assist_log(2, "✓", arr, titre, details, dry)
    return verdict, True


# ── Assistant d'import, règle 7 : téléchargement inutile ────────────────────
#
# Un téléchargement fini que l'*arr n'importera jamais, parce que la
# bibliothèque a déjà mieux : un pack qui n'améliore aucun épisode, ou un
# upgrade dépassé par un autre arrivé entre-temps. Il reste bloqué dans la file
# indéfiniment, et Jellyfin, qui lit les dossiers de téléchargement, le montre
# en double. La règle le retire de la file, sans blacklist : l'*arr ne le
# reprendra pas, puisqu'il n'est pas un upgrade. Le torrent part en corbeille,
# où il continue de seeder. Rien n'est supprimé. La règle 6 lui confie aussi un
# téléchargement mort dont la bibliothèque a déjà mieux : inutile de le remplacer.
# Une vidéo unique refusée n'émet aucun event : seul le passage de renfort la voit.
#
# Le filet hors file. La corbeille attend un signal : un event sur un fichier de la
# bibliothèque, une ligne de file. Un signal raté n'est jamais rattrapé, et le
# torrent reste rangé « après import » sans plus servir à rien : un import refusé
# puis sorti de la file, un upgrade passé avant que l'index connaisse le fichier.
# Le filet regarde l'état réel, à chaque passage.

# Au plus tant de torrents par passage : une telle vague serait suspecte.
_INUTILE_HORS_FILE_MAX = 10

_INUTILE_REJET = re.compile(r"not an upgrade|not a custom format upgrade|not a quality revision upgrade",
                            re.IGNORECASE)
_INUTILE_ECHANTILLON = re.compile(r"\bsample\b", re.IGNORECASE)


def _rejected_as_no_upgrade(records: list[dict[str, Any]]) -> int:
    """
    Nombre de vidéos que l'*arr a refusées parce que la bibliothèque a déjà mieux,
    si c'est le cas de toutes ; 0 sinon. Un échantillon refusé comme tel ne compte
    pas. Pour plusieurs vidéos, la file donne une entrée de statut par fichier, plus
    un en-tête ; pour une seule, une entrée au nom du téléchargement, parfois sans
    extension.
    """
    titre = records[0].get("title")
    refus = [s.get("messages") or [] for s in records[0].get("statusMessages") or []
             if s.get("title") == titre or os.path.splitext(s.get("title") or "")[1].lower() in VIDEO_EXTENSIONS]
    refus = [m for m in refus if not any(_INUTILE_ECHANTILLON.search(x) for x in m)]
    if refus and all(any(_INUTILE_REJET.search(x) for x in m) for m in refus):
        return len(refus)
    return 0


def _quality_rank(url: str, cle: str, profil: int | None) -> dict[int, int]:
    """Rang de chaque qualité du profil, de la plus basse à la plus haute ; les qualités d'un groupe partagent son rang."""
    items = ((_arr_request("GET", url, cle, f"qualityprofile/{profil}") or {}).get("items") or []) if profil else []
    return {q["quality"]["id"]: i for i, item in enumerate(items) for q in [item, *(item.get("items") or [])]
            if q.get("quality")}


def _release_desc(arr: str, p: dict[str, Any]) -> str:
    info = p.get("parsedMovieInfo" if arr == "radarr" else "parsedEpisodeInfo") or {}
    return f"{((info.get('quality') or {}).get('quality') or {}).get('name')}, score {p.get('customFormatScore')}"


def _library_better(arr: str, url: str, cle: str, media_id: int, records: list[dict[str, Any]],
                    p: dict[str, Any]) -> str:
    """
    Le fichier en place, s'il vaut au moins la release `p` (lue par /parse) : une
    qualité de rang égal ou supérieur dans le profil, et à rang égal un score de
    formats au moins égal. Pour une série, chaque épisode de la file doit avoir un
    tel fichier. "" sinon, ou dans le doute.
    """
    info = p.get("parsedMovieInfo" if arr == "radarr" else "parsedEpisodeInfo") or {}
    q_release = ((info.get("quality") or {}).get("quality") or {}).get("id")
    score = p.get("customFormatScore")
    if q_release is None or score is None:
        return ""
    if arr == "radarr":
        media = _arr_request("GET", url, cle, f"movie/{media_id}") or {}
        en_place = _arr_request("GET", url, cle, "moviefile", params={"movieId": media_id}) or []
    else:
        media = _arr_request("GET", url, cle, f"series/{media_id}") or {}
        voulus = {r.get("episodeId") for r in records if r.get("episodeId")}
        fichiers = {f["id"]: f for f in _arr_request("GET", url, cle, "episodefile", params={"seriesId": media_id}) or []}
        episodes = [e for e in _arr_request("GET", url, cle, "episode", params={"seriesId": media_id}) or []
                    if e.get("id") in voulus]
        if not voulus or len(episodes) != len(voulus):
            return ""
        en_place = [fichiers.get(e.get("episodeFileId")) for e in episodes]
    rang = _quality_rank(url, cle, media.get("qualityProfileId"))
    r_release = rang.get(q_release)
    if not en_place or r_release is None:
        return ""
    for f in en_place:
        r = rang.get((((f or {}).get("quality") or {}).get("quality") or {}).get("id"))
        s = (f or {}).get("customFormatScore")
        if r is None or s is None or r < r_release or (r == r_release and s < score):
            return ""
    f = en_place[0]
    return (f"{f['quality']['quality']['name']}, score {f['customFormatScore']}"
            + (f", pour les {len(en_place)} épisodes" if len(en_place) > 1 else ""))


def _useless(arr: str, url: str, cle: str, dl: str, records: list[dict[str, Any]], t: dict[str, Any],
             ctx: dict[str, Any], verdict: dict[str, Any], constat: str, arreter: bool = False) -> tuple[dict[str, Any], bool]:
    """
    Règle 7 : retire de la file, sans blacklist, un téléchargement qui n'apporterait
    rien, et le met en corbeille. Il y continue de seeder, sauf `arreter` : un
    téléchargement mort n'a rien à seeder. Une ligne sans id : il n'est déjà plus
    dans la file (run_forgotten).
    """
    dry = ctx["apercu"] or IMPORT_ASSIST_INUTILE_DRY_RUN
    titre = records[0].get("title") or dl
    en_file = bool(records[0].get("id"))
    corbeille = "en corbeille et arrêté : il n'a rien à seeder" if arreter else "en corbeille, où il continue de seeder"
    details = f"{constat} → {'retiré de la file sans blacklist, ' if en_file else ''}{corbeille}"
    verdict.update(decision="useless", reason=details)
    if _assist_seen(dl, "useless", dry):
        return verdict, False
    if not dry:
        if en_file:
            _arr_request("DELETE", url, cle, f"queue/{records[0]['id']}",
                         params={"removeFromClient": "false", "blocklist": "false", "skipRedownload": "true"})
        if not _assist_trash(t, arreter=arreter):
            details = (f"{constat} → {'retiré de la file sans blacklist ; ' if en_file else ''}laissé dans sa catégorie, "
                       "son dossier est aussi celui d'un autre torrent")
            verdict["reason"] = details
    _assist_record(dl, arr, titre, "useless", details, dry, 7)
    _assist_log(7, "✓", arr, titre, details, dry)
    return verdict, True


# Une release piégée n'a souvent aucun grab à son identifiant : le hash annoncé par
# l'indexeur n'est pas celui du torrent reçu. Son grab se retrouve alors à son nom,
# parmi les prises récentes du média dont le hash n'est celui d'aucun torrent.
_PIEGE_JOURS = 14


def _trap_grab(arr: str, url: str, cle: str, dl: str, records: list[dict[str, Any]], t: dict[str, Any],
               ctx: dict[str, Any]) -> dict[str, Any] | None:
    """Le grab de cette release, par son identifiant, sinon par son nom (voir plus haut) ; None s'il est introuvable."""
    rep = _arr_request("GET", url, cle, "history", params={"downloadId": dl.upper(), "pageSize": 50}) or {}
    grabs = [g for g in rep.get("records") or [] if g.get("eventType") == "grabbed"]
    if not grabs:
        route, cle_media = ("history/movie", "movieId") if arr == "radarr" else ("history/series", "seriesId")
        media = records[0].get(cle_media)
        if not media:
            return None
        depuis = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(time.time() - _PIEGE_JOURS * 86400))
        grabs = [g for g in _arr_request("GET", url, cle, route, params={cle_media: media}) or []
                 if g.get("eventType") == "grabbed" and (g.get("date") or "") >= depuis
                 and _nom_compact(g.get("sourceTitle") or "") == _nom_compact(t.get("name") or "")
                 and (g.get("downloadId") or "").lower() not in ctx["torrents"]]
    return max(grabs, key=lambda g: g.get("date") or "") if grabs else None


def _nom_compact(nom: str) -> str:
    """Un nom de release sans extension, ponctuation, espaces ni accents : « DDP5 1 » et « DDP 5.1 » se valent."""
    base, ext = os.path.splitext(nom)
    return "".join(_id_mots(base if ext.lower() in VIDEO_EXTENSIONS | _NOM_SUSPECTS else nom))


def _trapped(arr: str, url: str, cle: str, dl: str, records: list[dict[str, Any]], t: dict[str, Any],
             ctx: dict[str, Any], verdict: dict[str, Any], pieges: list[str]) -> tuple[dict[str, Any], bool]:
    """
    Règle 7, release piégée : aucune vidéo, mais un exécutable ou un script. Le
    réglage « Fail Downloads » de l'*arr ne s'applique pas quand il ne relie pas le
    téléchargement à son grab. Le grab est marqué échoué dans l'*arr (liste noire,
    nouvelle recherche s'il est réglé pour), et le torrent part en corbeille, arrêté.
    Rien n'est supprimé : les fichiers partent au vidage de la corbeille.
    """
    dry = ctx["apercu"] or IMPORT_ASSIST_PIEGE_DRY_RUN
    titre = records[0].get("title") or dl
    grab = _trap_grab(arr, url, cle, dl, records, t, ctx)
    if grab:
        geste = (f"grab ({(grab.get('data') or {}).get('indexer') or 'indexeur inconnu'}) marqué échoué dans "
                 f"{arr.capitalize()}, qui met la release en liste noire")
    else:
        geste = f"aucun grab de ce nom dans {arr.capitalize()} pour la liste noire"
    constat = f"{', '.join(pieges[:3])} et aucune vidéo : release piégée"
    details = f"{constat} → {geste}, torrent arrêté et mis en corbeille"
    verdict.update(decision="trap", reason=details)
    if _assist_seen(dl, "trap", dry):
        return verdict, False
    if not dry:
        if grab:
            _arr_request("POST", url, cle, f"history/failed/{grab['id']}", retries=0)
        if not _assist_trash(t, arreter=True):
            details = f"{constat} → {geste} ; torrent arrêté, laissé dans sa catégorie : son dossier est aussi celui d'un autre torrent"
            verdict["reason"] = details
    _assist_record(dl, arr, titre, "trap", details, dry, 7)
    _assist_log(7, "⚠", arr, titre, details, dry)
    return verdict, True


def _no_longer_used(t: dict[str, Any]) -> tuple[str, str, str, str] | None:
    """
    L'*arr qui a pris ce torrent, et le constat, si la bibliothèque n'en utilise plus
    rien et a tout par d'autres fichiers ; None sinon, ou dans le doute. Les inodes
    sont lus sur le disque, pas dans l'index : un import à la main n'y figure qu'à
    la sync suivante.
    """
    for arr, url, cle in _assist_arrs():
        rep = _arr_request("GET", url, cle, "history",
                           params={"downloadId": t["hash"].upper(), "pageSize": 200, "eventType": 1}) or {}
        grabs = [g for g in rep.get("records") or [] if g.get("eventType") == "grabbed"]
        if grabs:
            break
    else:
        return None  # jamais pris par un *arr : un ajout manuel
    cle_media, route = ("movieId", "movie") if arr == "radarr" else ("seriesId", "series")
    media = grabs[0].get(cle_media)
    dossier = (_arr_request("GET", url, cle, f"{route}/{media}") or {}).get("path") if media else ""
    inodes = set(scan_video_files(dossier)) if dossier else set()
    videos = _torrent_videos(t)
    # Rien à juger, ou bibliothèque absente (montage perdu) : on ne conclut rien.
    if not videos or not inodes or any(os.stat(v).st_ino in inodes for v in videos):
        return None
    if arr == "radarr":
        en_place = _arr_request("GET", url, cle, "moviefile", params={"movieId": media}) or []
    else:
        voulus = {g.get("episodeId") for g in grabs if g.get("episodeId")}
        fichiers = {f["id"]: f for f in _arr_request("GET", url, cle, "episodefile", params={"seriesId": media}) or []}
        episodes = [e for e in _arr_request("GET", url, cle, "episode", params={"seriesId": media}) or []
                    if e.get("id") in voulus]
        if len(episodes) != len(voulus):
            return None
        en_place = [fichiers.get(e.get("episodeFileId")) for e in episodes]
    if not en_place or not all(f and os.path.exists(f.get("path") or "") for f in en_place):
        return None
    return arr, url, cle, (f"hors de la file, aucune de ses {len(videos)} vidéo(s) n'est dans la bibliothèque, "
                           "qui a déjà tout par d'autres fichiers")


def run_forgotten(en_file: set[str], ctx: dict[str, Any]) -> tuple[list[dict[str, Any]], int]:
    """
    Règle 7, le filet hors file : un torrent fini depuis plus de CLEANUP_GRACE_HOURS,
    rangé dans une catégorie après import, qu'un *arr a pris et que la bibliothèque
    n'utilise plus. Il part en corbeille, où il continue de seeder. Un torrent qui en
    est déjà sorti n'y retourne pas : quelqu'un l'a voulu. Retourne (verdicts,
    journalisés maintenant).
    """
    try:
        rangees: set[str] = set()
        for arr, url, cle in _assist_arrs():
            clients = _arr_request("GET", url, cle, "downloadclient") or []
            rangees |= {c for c in (_imported_category(clients, x.get("name") or "") for x in clients) if c}
    except Exception as e:
        log.error("  ⚠ [%s] clients des *arr illisibles (%s)", _regle(7), e)
        return [], 0
    # L'index écarte d'emblée la plupart des torrents, sans appel à l'*arr.
    with _db_lock:
        with get_db() as conn:
            utilises = {r[0] for r in conn.execute("SELECT DISTINCT torrent_hash FROM files WHERE path LIKE '/data/Media/%' "
                                                   "AND stale = 0 AND torrent_hash IS NOT NULL")}
            sortis = {r[0] for r in conn.execute("SELECT DISTINCT torrent_hash FROM cleanup_log")}
    limite = time.time() - CLEANUP_GRACE_HOURS * 3600
    candidats = sorted(
        (t for h, t in ctx["torrents"].items()
         if t.get("category") in rangees and (t.get("progress") or 0) >= 1 and 0 < (t.get("completion_on") or 0) <= limite
         and h not in en_file and h not in utilises and h not in sortis),
        key=lambda t: t["completion_on"],
    )
    verdicts: list[dict[str, Any]] = []
    nouveaux = attente = 0
    for t in candidats:
        dl, titre = t["hash"].lower(), t.get("name") or t["hash"]
        try:
            inutile = _no_longer_used(t)
            if not inutile:
                continue
            if nouveaux >= _INUTILE_HORS_FILE_MAX:
                attente += 1
                continue
            arr, url, cle, constat = inutile
            verdict, nouveau = _useless(arr, url, cle, dl, [{"title": titre}], t, ctx,
                                        {"arr": arr, "download_id": dl, "title": titre}, constat)
        except Exception as e:
            # Un torrent en erreur n'empêche pas de traiter les suivants.
            log.error("  ⚠ [%s] %s en erreur (%s)", _regle(7), titre, e)
            continue
        verdicts.append(verdict)
        nouveaux += nouveau
    if attente:
        log.info("  [%s] %d autre(s) torrent(s) inutile(s) attendront le passage suivant", _regle(7), attente)
    return verdicts, nouveaux


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
# Saisons trouées : un épisode n'est un trou que diffusé depuis ce délai, dans une
# série ajoutée et demandée depuis ce délai, le temps que Sonarr cherche et que le
# RSS passe.
_TROUS_ATTENTE = 24 * 3600
# Une saison déjà examinée ne l'est de nouveau qu'après ce délai.
_TROUS_REPRISE = 3 * 86400
# Saisons cherchées au plus par passage : chaque recherche interroge tous les indexeurs.
_TROUS_MAX_PAR_PASSAGE = 2


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
        if cand["vo"]:
            # Pas de pack complet en français : le partiel est gardé, et seuls ses
            # épisodes manquants sont pris dans le pack d'une autre langue.
            trous = voulus - couverts
            seulement = _pack_files_for(cand, trous)
            details = (f"{resume} → gardé, ses {len(trous)} épisode(s) manquant(s) pris en VO dans {cand['title']} "
                       f"({cand['indexer']}, {cand['seeders']} sources, score {cand['score']}) : "
                       f"{len(seulement)} fichier(s) téléchargé(s) sur {len(cand['par_fichier'])}")
            verdict["vo"] = cand["title"]
            if dry_run:
                return fin("pack-fill", details, "✓")
            _qbit_add(cand, t.get("category") or "", t, seulement=seulement)
            if arrete:
                _qbit_post("start", {"hashes": dl})
                arrete = False
            _pack_register(serie_id, saison, dl, cand, "vo", trous)
            return fin("pack-fill", details, "✓")
        moins = f", moins bien noté que le partiel ({ref['score']}) mais complet" if cand["score"] < ref["score"] else ""
        details = (f"{resume} → remplacé par {cand['title']} ({cand['indexer']}, "
                   f"{cand['seeders']} sources, score {cand['score']}{moins})")
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
    if not _assist_trash(t):
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
        # Ajouté par un passage précédent, ou au grab : ses épisodes sont en route.
        return {"title": deja["new_title"], "deja": True, "vo": deja["stage"] == "vo"}, ""
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
    # Moins bien noté que le partiel, il ne fait que combler les trous : les
    # épisodes déjà importés gardent leur meilleure version. En VO, seuls les
    # fichiers des trous seront téléchargés.
    combler = cand["vo"] or cand["score"] < ref["score"]
    cand.update(season=saison, episodes=set(manquants) if combler else a_couvrir, category=categorie, combler=combler)
    return cand, ""


def _pack_finish(
    url: str, cle: str, dl: str, titre: str, inodes_media: set[int], remplacement: sqlite3.Row,
    ctx: dict[str, Any], verdict: dict[str, Any], ecarte: Any, t: dict[str, Any], categorie: str,
) -> tuple[dict[str, Any], bool]:
    """
    Règle 4, fin du remplacement. Le pack complet est téléchargé : Sonarr a importé
    seul les épisodes manquants, mais refuse de remplacer ceux du pack partiel,
    qu'il n'améliore pas (même score). On les importe à la main, en hardlink, à la
    place des fichiers du pack partiel — de ceux-là seulement : un épisode remplacé
    entre-temps par autre chose n'est pas touché. Le pack partiel, qui n'a plus de
    fichier dans la bibliothèque, part ensuite en corbeille par l'event d'upgrade.
    Un pack complet moins bien noté n'a été prévu que pour les trous : les épisodes
    du partiel, meilleurs, restent.

    Quand chaque épisode prévu a son fichier, le pack complet sort de la file par
    la catégorie après import, comme le ferait la règle 1 : ses autres vidéos
    (épisodes déjà mieux pourvus) restent en seed. Suit la simulation de la
    règle 4 seule, sauf aperçu.
    """
    dry_run = ctx["apercu"] or IMPORT_ASSIST_PACK_DRY_RUN
    if not dry_run and _assist_seen(dl, "pack-reimport", False):
        return ecarte("remplacement déjà lancé dans les dernières 24 h, toujours dans la file", 4, dry_run)
    serie_id = remplacement["series_id"]
    # Au grab, le partiel a été arrêté avant d'avoir rien apporté : seuls les trous
    # comptent. Après l'import, ses épisodes se reconnaissent à ses inodes.
    inodes_ancien: set[int] = set()
    if remplacement["stage"] not in ("grab", "vo"):
        ancien = ctx["torrents"].get(remplacement["old_hash"])
        for v in (_torrent_videos(ancien) or []) if ancien else []:
            try:
                inodes_ancien.add(os.stat(v).st_ino)
            except OSError:
                continue
        if not inodes_ancien:
            # Retiré de qBittorrent : ses épisodes ne se reconnaissent plus.
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

    restants = {i for i in prevus if du_partiel(i)}
    if not restants:
        # Remplacement terminé : le pack complet sort de la file.
        obstacle = _seed_obstacle(categorie, t, ctx)
        if obstacle:
            return ecarte(f"remplacement terminé, mais {obstacle}", 4, dry_run)
        resume = f"remplacement terminé : les {len(prevus)} épisode(s) prévu(s) sont en place → {categorie}"
        # Les trous comblés en VO sont aussitôt cherchés en français.
        recherche_vo = sorted(prevus)[:_ASSIST_MAX_SEARCH] if remplacement["stage"] == "vo" else []
        if recherche_vo:
            resume += f", recherche en français des {len(recherche_vo)} épisode(s) pris en VO"
        verdict.update(decision="pack-done", category=categorie)
        if _assist_seen(dl, "pack-done", dry_run):
            return verdict, False
        if not dry_run:
            _qbit_post("setCategory", {"hashes": dl, "category": categorie})
        _assist_record(dl, "sonarr", titre, "pack-done", resume, dry_run, 4)
        _assist_log(4, "✓", "sonarr", titre, resume, dry_run)
        if recherche_vo and not dry_run:
            try:
                _arr_request("POST", url, cle, "command", json={"name": "EpisodeSearch", "episodeIds": recherche_vo})
            except Exception as e:
                log.error("  ⚠ [%s] [sonarr] %s : recherche en français non lancée (%s), à lancer depuis Sonarr",
                          _regle(4), titre, e)
        return verdict, True

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
        return ecarte(f"{len(restants)} épisode(s) prévu(s) sans fichier importable du pack complet : "
                      "import à finir à la main", 4, dry_run)

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
    """Épisodes de la saison qu'apportent ces vidéos, toutes ensemble."""
    return set().union(*_pack_file_episodes(url, cle, serie_id, saison, episodes, noms).values())


def _pack_file_episodes(url: str, cle: str, serie_id: int, saison: int,
                        episodes: list[dict[str, Any]], noms: list[str]) -> dict[str, set[int]]:
    """
    Épisodes de la saison qu'apporte chaque vidéo, d'après le parseur de Sonarr.
    Seul le nom du fichier compte, comme à l'import : une vidéo qui ne porte pas
    son numéro ne couvre rien, même rangée dans un dossier « S01 ».
    """
    par_numero = {e["episodeNumber"]: e["id"] for e in episodes if e.get("seasonNumber") == saison}
    par_absolu = {e["absoluteEpisodeNumber"]: e["id"] for e in episodes
                  if e.get("seasonNumber") == saison and e.get("absoluteEpisodeNumber")}
    par_fichier: dict[str, set[int]] = {}
    for nom in noms:
        p = _arr_request("GET", url, cle, "parse", params={"title": os.path.basename(nom)}) or {}
        info = p.get("parsedEpisodeInfo") or {}
        couverts: set[int] = set()
        serie = p.get("series") or {}
        if not info or info.get("fullSeason"):
            pass
        elif serie:
            # Série reconnue : la correspondance de Sonarr fait foi, numérotation
            # de scène comprise. Une autre série ne couvre rien.
            if serie.get("id") == serie_id:
                couverts = {e["id"] for e in p.get("episodes") or [] if e.get("seasonNumber") == saison}
        elif info.get("episodeNumbers"):
            if info.get("seasonNumber") == saison:
                couverts = {par_numero[n] for n in info["episodeNumbers"] if n in par_numero}
        else:
            couverts = {par_absolu[n] for n in info.get("absoluteEpisodeNumbers") or [] if n in par_absolu}
        par_fichier[nom] = couverts
    return par_fichier


_LANGUE_FRENCH = 2  # identifiant de la langue French dans Radarr et Sonarr


def _francais_seul(cf: dict[str, Any]) -> bool:
    """Format dont toutes les conditions disent « langue : French » : celui qui porte l'audio français d'un profil."""
    specs = cf.get("specifications") or []

    def champ(spec: dict[str, Any], nom: str) -> Any:
        return next((c.get("value") for c in spec.get("fields") or [] if c.get("name") == nom), None)

    return bool(specs) and all(
        s.get("implementation") == "LanguageSpecification" and not s.get("negate")
        and champ(s, "value") == _LANGUE_FRENCH and not champ(s, "exceptLanguage") for s in specs)


def _quality_profile(url: str, cle: str, profil_id: int | None) -> dict[str, Any]:
    """
    Qualités autorisées par le profil, et leur rang : l'ordre du profil, de la
    moins bonne à la meilleure. Plus le bonus que le profil donne aux packs
    eux-mêmes : score des formats dont une condition porte sur le type de release.
    Plus le seuil du français, à l'échelle du profil : les trois quarts du score
    de son format « langue : French », ou IMPORT_ASSIST_PACK_MIN_SCORE sans ce format.
    """
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
    notes = {f.get("format"): f.get("score") or 0 for f in profil.get("formatItems") or [] if f.get("score")}
    formats = (_arr_request("GET", url, cle, "customformat") or []) if notes else []
    bonus_pack = {f["id"]: notes[f["id"]] for f in formats if f.get("id") in notes and any(
        c.get("implementation") == "ReleaseTypeSpecification" for c in f.get("specifications") or [])}
    francais = [notes[f["id"]] for f in formats if notes.get(f.get("id"), 0) > 0 and _francais_seul(f)]
    return {"autorisees": autorisees, "rangs": rangs, "bonus_pack": bonus_pack,
            "score_min": profil.get("minFormatScore") or 0,
            "seuil_fr": max(francais) * 3 // 4 if francais else IMPORT_ASSIST_PACK_MIN_SCORE}


def _score_hors_pack(p: dict[str, Any], profil: dict[str, Any]) -> int:
    """
    Score de formats d'une release lue par /parse, sans le bonus de pack : le pack
    partiel et ses remplaçants en sont tous, il ne départage rien, et ne doit pas
    faire passer le seuil à une release sans français.
    """
    bonus = sum(profil["bonus_pack"].get(f.get("id"), 0) for f in p.get("customFormats") or [])
    return (p.get("customFormatScore") or 0) - bonus


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
        # Sonarr reconnaît-il la série à ce titre seul ? « Pokemon The Series » : non.
        "connu": bool(p.get("series")),
        "season": info.get("seasonNumber"),
        "season_pack": bool(info.get("fullSeason") or info.get("isPartialSeason")) and not info.get("isMultiSeason"),
        "score": _score_hors_pack(p, profil),
        "rang": profil["rangs"].get(qid, -1),
    }


def _pack_queries(serie: dict[str, Any], saison: int) -> list[str]:
    """
    Requêtes « Titre S01 » : titre de la série et titres alternatifs valables pour cette
    saison, deux au plus, sans accents comme les noms de releases. « Pokémon » cherche
    « Pokemon S04 », puis « Pokemon Johto League Champions S04 », et non le titre de
    scène d'une autre saison (« Pokemon Indigo League », celui de la saison 1).
    """
    titres = [serie.get("title") or ""] + [
        a.get("title") or "" for a in serie.get("alternateTitles") or []
        if a.get("seasonNumber") in (None, -1, saison) and a.get("sceneSeasonNumber") in (None, -1, saison)
    ]
    requetes: list[str] = []
    for brut in titres:
        titre = unicodedata.normalize("NFKD", brut).encode("ascii", "ignore").decode()
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
    couvrir. Mieux vaut une saison sans trou : son score de formats peut être plus
    bas que celui du pack partiel, tant qu'il atteint le seuil du français du profil
    (le français reste le français), ou le score du partiel s'il est plus bas.
    À défaut, un pack d'une autre langue que Sonarr accepterait encore (score
    minimal du profil) : marqué « vo », il ne sert qu'à combler les trous, et
    seuls les fichiers de ces épisodes seront téléchargés.
    Sa qualité est autorisée par le profil, et au moins aussi bonne.

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

    seuil = min(ref["score"], profil.get("seuil_fr", IMPORT_ASSIST_PACK_MIN_SCORE))
    # Pour combler des trous, une autre langue suffit, tant que Sonarr l'accepterait.
    plancher = min(seuil, profil["score_min"])
    # Saisons trouées : plusieurs titres admis, faute de pack partiel de référence.
    titres_admis = set(ref.get("series_titles") or ({ref["series_title"]} if ref.get("series_title") else set()))
    if titres_admis and ref.get("connu") is False:
        # Sonarr ne reconnaît pas le titre du pack partiel (« Pokemon The Series », la
        # version Netflix) : ses propres titres pour la série sont admis aussi. Un titre
        # qu'il connaît, comme « Monster The Lizzie Borden Story », reste seul juge.
        titres_admis |= _series_titles(url, cle, serie, saison, [])
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
        if titres_admis and _series_title_key(titre_serie) not in titres_admis:
            autres_titres.add(titre_serie)
            continue
        qid = ((info.get("quality") or {}).get("quality") or {}).get("id")
        score, rang = _score_hors_pack(p, profil), profil["rangs"].get(qid, -1)
        if qid not in profil["autorisees"] or rang < ref["rang"] or score < plancher:
            continue
        candidats.append((score, rang, r.get("seeders") or 0, r))
    ecartes: list[str] = []
    if autres_titres:
        ecartes.append("sous un autre titre : " + ", ".join(sorted(autres_titres)[:3]))
    if not candidats:
        return None, ["aucun pack de la saison à la hauteur dans Prowlarr"] + ecartes

    candidats.sort(key=lambda c: (-c[0], -c[1], -c[2]))
    # Les packs au seuil d'abord ; à défaut, ceux d'une autre langue, qui ne
    # serviront qu'à combler les trous. Chaque groupe a son lot de .torrent à lire.
    for vo, groupe in ((False, [c for c in candidats if c[0] >= seuil]),
                       (True, [c for c in candidats if c[0] < seuil])):
        for score, _, seeders, r in groupe[:_PACK_MAX_TORRENTS]:
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
            par_fichier = _pack_file_episodes(url, cle, serie["id"], saison, episodes, noms)
            couverts = set().union(*par_fichier.values()) & a_couvrir
            if couverts == a_couvrir:
                return {"title": titre, "indexer": r.get("indexer") or "?", "seeders": seeders, "score": score,
                        "hash": meta["hash"], "torrent": contenu, "vo": vo, "par_fichier": par_fichier}, ecartes
            ecartes.append(f"{titre} : {len(couverts)}/{len(a_couvrir)} épisodes")
    return None, ecartes


def _pack_files_for(cand: dict[str, Any], trous: set[int]) -> list[str]:
    """Vidéos du pack qui apportent au moins un de ces épisodes : les seules à télécharger."""
    return sorted(nom for nom, eps in cand["par_fichier"].items() if eps & trous)


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


def _qbit_add(cand: dict[str, Any], categorie: str, ref: dict[str, Any], seulement: list[str] | None = None) -> None:
    """
    Ajoute le pack complet dans la catégorie de téléchargement du client Sonarr,
    rangé comme le torrent qu'il remplace. Il porte le titre de sa release : Sonarr,
    qui ne l'a pas pris lui-même, le reconnaît par ce titre. Vérifie qu'il est là.

    `seulement` : chemins (dans le .torrent) des seuls fichiers à télécharger. Le
    pack est alors ajouté à l'arrêt, les autres fichiers passent en « ne pas
    télécharger », puis il démarre : Sonarr n'importera que ce qu'il trouve.
    """
    if not categorie:
        raise RuntimeError("catégorie de téléchargement inconnue")
    arret = "true" if seulement is not None else "false"
    data = {"category": categorie, "rename": cand["title"], "tags": "cleanup-linker",
            "stopped": arret, "paused": arret}
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
                break
            time.sleep(1)
        else:
            raise RuntimeError("qBittorrent n'a pas ajouté le torrent")
        if seulement is None:
            return
        r = s.get(f"{QBIT_URL}/api/v2/torrents/files", params={"hash": cand["hash"]}, timeout=10)
        r.raise_for_status()
        fichiers = r.json() or []
        # qBittorrent préfixe les chemins du dossier racine du torrent, ou pas, selon sa disposition.
        garder = [any(f.get("name") == g or (f.get("name") or "").endswith("/" + g) for g in seulement) for f in fichiers]
        if not any(garder):
            # Laissé à l'arrêt : rien de ce qu'il fallait n'est dans le torrent.
            raise RuntimeError("aucun des fichiers voulus dans le torrent ajouté, laissé à l'arrêt")
        sauter = [str(f.get("index", i)) for i, (f, g) in enumerate(zip(fichiers, garder)) if not g]
        if sauter:
            r = s.post(f"{QBIT_URL}/api/v2/torrents/filePrio", timeout=10,
                       data={"hash": cand["hash"], "id": "|".join(sauter), "priority": "0"})
            r.raise_for_status()
        r = s.post(f"{QBIT_URL}/api/v2/torrents/start", data={"hashes": cand["hash"]}, timeout=10)
        r.raise_for_status()


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


def _pure_crossseeds(s: requests.Session, t: dict[str, Any], tous: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Les torrents dont chaque vidéo est un lien d'une vidéo de `t` : ses cross-seeds, ou la
    même release prise sur un autre tracker. Ils ne servent à rien de plus que lui. Lus
    dans les listes de fichiers ; les candidats viennent de l'index et du nom, jamais du
    nom seul (plusieurs torrents s'appellent « Season 2 » sans rien partager).
    """
    def videos(h: str, base: str) -> set[int]:
        rep = s.get(f"{QBIT_URL}/api/v2/torrents/files", params={"hash": h}, timeout=30)
        rep.raise_for_status()
        inodes: set[int] = set()
        for f in rep.json() or []:
            p = os.path.join(base.rstrip("/"), f.get("name") or "")
            if os.path.splitext(p)[1].lower() in VIDEO_EXTENSIONS:
                try:
                    inodes.add(os.lstat(p).st_ino)
                except OSError:
                    continue
        return inodes

    par_hash = {x["hash"].lower(): x for x in tous}
    h = t["hash"].lower()
    moi = par_hash.get(h, t)
    mes = videos(h, moi.get("save_path") or "")
    if not mes:
        return []
    candidats = {x for x, o in par_hash.items() if o.get("name") == moi.get("name")}
    liste = sorted(mes)
    with _db_lock:
        with get_db() as conn:
            for i in range(0, len(liste), _SQL_CHUNK):
                lot = liste[i:i + _SQL_CHUNK]
                candidats |= {r[0].lower() for r in conn.execute(
                    f"SELECT DISTINCT torrent_hash FROM files WHERE stale = 0 AND torrent_hash IS NOT NULL "
                    f"AND inode IN ({','.join('?' * len(lot))})", lot)}
    candidats.discard(h)
    out = []
    for c in sorted(candidats):
        o = par_hash.get(c)
        if not o or not o.get("category") or o.get("category") == TARGET_CAT:
            continue
        siennes = videos(c, o.get("save_path") or "")
        if siennes and siennes <= mes:
            out.append(o)
    return out


def _assist_trash(t: dict[str, Any], arreter: bool = True) -> bool:
    """
    Le torrent part en corbeille, arrêté : un pack partiel à peine entamé, un
    téléchargement mort n'ont rien à seeder (`arreter=False` le laisse en route).
    Sauf s'il partage son dossier avec un autre torrent — un pack complet publié
    sous le même nom : ATM emporterait les fichiers de l'autre dans le dossier de
    la corbeille. Il reste alors dans sa catégorie. True s'il est parti.

    Ses cross-seeds aux mêmes fichiers le suivent (_pure_crossseeds) : un média part
    dans toutes ses versions. Un cross-seed fini continue de seeder dans la corbeille.
    Si leur recherche échoue, le torrent part seul, comme avant.
    """
    jumeaux: list[dict[str, Any]] = []
    with qbit_session() as s:
        tous = qbit_get_torrents(s)
        partage = _shared_content({t["hash"]}, tous)
        actions = [("stop", {"hashes": t["hash"]})] if arreter else []
        if not partage:
            actions.insert(0, ("setCategory", {"hashes": t["hash"], "category": TARGET_CAT}))
        for action, data in actions:
            r = s.post(f"{QBIT_URL}/api/v2/torrents/{action}", data=data, timeout=10)
            r.raise_for_status()
        if not partage:
            try:
                candidats = _pure_crossseeds(s, t, tous)
            except Exception as e:
                log.warning("  Cross-seeds de %s non cherchés : %s", t.get("name"), e)
                candidats = []
            # Comme pour lui : un cross-seed dont le dossier est celui d'un autre torrent reste.
            partages = _shared_content({t["hash"]} | {c["hash"] for c in candidats}, tous)
            jumeaux = [c for c in candidats if c["hash"].lower() not in partages]
            for c in jumeaux:
                r = s.post(f"{QBIT_URL}/api/v2/torrents/setCategory", timeout=10,
                           data={"hashes": c["hash"], "category": TARGET_CAT})
                r.raise_for_status()
                if arreter and not _must_seed(c):
                    r = s.post(f"{QBIT_URL}/api/v2/torrents/stop", data={"hashes": c["hash"]}, timeout=10)
                    r.raise_for_status()
    if partage:
        return False
    log_move(t["hash"], t.get("name", ""), t.get("category", ""), "assist")
    for c in jumeaux:
        log_move(c["hash"], c.get("name", ""), c.get("category", ""), "assist")
        log.info("    + en corbeille aussi, mêmes fichiers : [%s] %s", c.get("category"), c.get("name"))
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


# ── Assistant d'import, règle 4 : saisons trouées ───────────────────────────
#
# Sonarr cherche les épisodes d'une série à son ajout, puis n'écoute plus que le
# RSS. Un épisode sans release à l'unité reste donc manquant, même quand un pack
# de la saison existe : Sonarr le refuse, parce que d'autres épisodes ont déjà
# mieux ou sont dans la file. StuGo, demandé le 20 novembre 2026 : 30 épisodes
# sur 40 sans aucune release à l'unité, seul un pack 720p hongrois et anglais.
# Comme après un pack partiel, ce pack comble les seuls trous : ses autres
# fichiers ne sont pas téléchargés. Seules les séries demandées dans Seerr ces
# derniers jours sont regardées : une vieille série incomplète ne se comble pas
# d'elle-même.


def _iso_epoch(texte: str | None) -> float:
    """Date ISO 8601 (UTC) en secondes. 0 si absente ou illisible."""
    try:
        return (datetime.strptime((texte or "")[:19], "%Y-%m-%dT%H:%M:%S") - datetime(1970, 1, 1)).total_seconds()
    except ValueError:
        return 0.0


def _series_client(clients: list[dict[str, Any]], serie: dict[str, Any]) -> str:
    """Le client qBittorrent de Sonarr qui prend cette série : celui de ses étiquettes, sinon un client sans étiquette."""
    actifs = sorted((c for c in clients if c.get("enable") and (c.get("implementation") or "").lower() == "qbittorrent"),
                    key=lambda c: c.get("priority") or 1)
    etiquettes = set(serie.get("tags") or [])
    for c in actifs:
        if etiquettes & set(c.get("tags") or []):
            return c.get("name") or ""
    return next((c.get("name") or "" for c in actifs if not c.get("tags")), "")


def _series_titles(url: str, cle: str, serie: dict[str, Any], saison: int, releases: list[str]) -> set[str]:
    """
    Titres de série qu'un pack de cette saison peut porter : celui de Sonarr, ses
    titres alternatifs valables pour elle, et ceux des releases déjà prises pour elle.
    Un titre alternatif propre à une autre saison de scène n'en est pas : « Pokemon
    Horizons » est la saison 25 de Pokémon, pas sa saison 1. Garde-fou des
    anthologies, comme dans _pack_candidate.
    """
    noms = [serie.get("title") or ""] + [a.get("title") or "" for a in serie.get("alternateTitles") or []
                                         if a.get("seasonNumber") in (None, -1, saison)
                                         and a.get("sceneSeasonNumber") in (None, -1, saison)]
    cles = {_series_title_key(n) for n in noms}
    for titre in list(dict.fromkeys(releases))[:4]:
        info = (_arr_request("GET", url, cle, "parse", params={"title": titre}) or {}).get("parsedEpisodeInfo") or {}
        cles.add(_series_title_key(info.get("seriesTitle") or ""))
    return cles - {""}


def _dead_waiting(dl: str, torrents: dict[str, dict[str, Any]], maintenant: float) -> bool:
    """Ce téléchargement est-il mort, sans autre version trouvée par la règle 6 ces derniers jours ?"""
    t = torrents.get(dl)
    inactif = _dead_since(t, maintenant)
    if inactif is None or inactif < _dead_threshold(t, True):
        return False
    return any(_assist_recent(dl, ("dead-wait",), simulation, _TROUS_REPRISE) for simulation in (False, True))


def _holes_pending(serie_id: int, saison: int, torrents: dict[str, dict[str, Any]]) -> bool:
    """Un pack ajouté pour cette saison est-il encore en route dans qBittorrent ?"""
    with _db_lock:
        with get_db() as conn:
            rows = conn.execute("SELECT new_hash FROM pack_replace WHERE series_id = ? AND season = ? AND at > ?",
                                (serie_id, saison, int(time.time()) - _PACK_ATTEMPTS_WINDOW)).fetchall()
    return any((t.get("progress") or 0) < 1 for r in rows for t in [torrents.get(r["new_hash"])] if t is not None)


def run_season_holes(ctx: dict[str, Any]) -> tuple[list[dict[str, Any]], int]:
    """
    Règle 4, saisons trouées, sur les séries demandées dans Seerr ces derniers
    jours. Suit sa propre simulation et celle de la règle 4, sauf aperçu (tout
    simulé). Retourne (verdicts, journalisés maintenant).
    """
    if not (SONARR_URL and SONARR_API_KEY and SEERR_URL and SEERR_API_KEY and PROWLARR_URL and PROWLARR_API_KEY):
        return [], 0
    dry = ctx["apercu"] or IMPORT_ASSIST_PACK_DRY_RUN or IMPORT_ASSIST_TROUS_DRY_RUN
    url, cle = SONARR_URL, SONARR_API_KEY
    maintenant = time.time()
    try:
        demandes = _seerr_tv_requests(maintenant - _ACCORD_WINDOW_DAYS * 86400)
    except Exception as e:
        log.error("  ⚠ [%s] saisons trouées : Seerr injoignable ou réponse inattendue (%s)", _regle(4), e)
        return [], 0
    # Chaque saison compte depuis sa dernière demande ; une saison qu'aucune demande ne
    # nomme, depuis la plus récente de la série (clé None). Pokémon : la saison 1,
    # demandée ce matin, ne retarde pas la saison 4, demandée la veille.
    demandee_le: dict[int, dict[int | None, float]] = {}
    for d in demandes:
        quand = _iso_epoch(d.get("createdAt"))
        par_saison = demandee_le.setdefault(d["media"]["tvdbId"], {})
        for s in [x.get("seasonNumber") for x in d.get("seasons") or []] + [None]:
            par_saison[s] = max(par_saison.get(s, 0.0), quand)
    diffuse_avant = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(maintenant - _TROUS_ATTENTE))
    verdicts: list[dict[str, Any]] = []
    nouveaux = cherchees = attente = 0
    clients: list[dict[str, Any]] | None = None
    for tvdb, depuis in demandee_le.items():
        try:
            serie = next(iter(_arr_request("GET", url, cle, "series", params={"tvdbId": tvdb}) or []), None)
            if not serie or not serie.get("monitored"):
                continue
            if maintenant - _iso_epoch(serie.get("added")) < _TROUS_ATTENTE:
                continue
            serie_id = serie["id"]
            episodes = _arr_request("GET", url, cle, "episode", params={"seriesId": serie_id}) or []
            file = _arr_request("GET", url, cle, "queue/details", params={"seriesId": serie_id}) or []
            # Un téléchargement mort pour lequel la règle 6 n'a trouvé aucune autre version
            # ne tient pas ses épisodes : un pack peut partir en parallèle, sans que le mort
            # soit retiré. Le premier fini gagne. Pokémon S04 : un magnet resté sans liste
            # de fichiers.
            en_file = {q.get("episodeId") for q in file
                       if not _dead_waiting((q.get("downloadId") or "").lower(), ctx["torrents"], maintenant)}
            # Les saisons que la règle 5 a écartées ne sont pas celles de la demande.
            ecartees = (ctx.get("ecartees") or {}).get(serie_id) or set()
            saisons = sorted({s["seasonNumber"] for s in serie.get("seasons") or []
                              if s.get("monitored") and (s.get("seasonNumber") or 0) > 0} - ecartees)
            for saison in saisons:
                if maintenant - depuis.get(saison, depuis[None]) < _TROUS_ATTENTE:
                    continue
                de_la_saison = [e for e in episodes if e.get("seasonNumber") == saison]
                voulus = _episodes_wanted(de_la_saison)
                trous = {e["id"] for e in de_la_saison if e["id"] in voulus
                         and e["id"] not in en_file and (e.get("airDateUtc") or "9999") <= diffuse_avant}
                cle_journal = f"trous-{serie_id}-s{saison}"
                if (not trous
                        or _assist_recent(cle_journal, ("pack-hole-fill", "pack-hole-none", "pack-hole-skip"),
                                          dry, _TROUS_REPRISE)
                        or _assist_seen(cle_journal, "pack-hole-error", dry)
                        or _holes_pending(serie_id, saison, ctx["torrents"])):
                    continue
                if cherchees >= _TROUS_MAX_PAR_PASSAGE:
                    attente += 1
                    continue
                cherchees += 1
                if clients is None:
                    clients = _arr_request("GET", url, cle, "downloadclient") or []
                titre = f"{serie.get('title') or tvdb} S{saison:02d}"
                try:
                    verdicts.append(_fill_season_holes(url, cle, serie, saison, de_la_saison, trous, file, clients, dry))
                except Exception as e:
                    log.error("  ⚠ [%s] [sonarr] %s : saison trouée en erreur (%s)", _regle(4), titre, e)
                    _assist_record(cle_journal, "sonarr", titre, "pack-hole-error", str(e), dry, 4)
                    verdicts.append({"arr": "sonarr", "download_id": cle_journal, "title": titre,
                                     "decision": "pack-hole-error", "reason": str(e)})
                nouveaux += 1
        except Exception as e:
            log.error("  ⚠ [%s] [sonarr] série TVDB %s : saisons trouées en erreur (%s)", _regle(4), tvdb, e)
    if attente:
        log.info("  [%s] %d autre(s) saison(s) trouée(s) attendront le passage suivant", _regle(4), attente)
    return verdicts, nouveaux


def _fill_season_holes(url: str, cle: str, serie: dict[str, Any], saison: int, episodes: list[dict[str, Any]],
                       trous: set[int], file: list[dict[str, Any]], clients: list[dict[str, Any]],
                       dry: bool) -> dict[str, Any]:
    """
    Comble les trous d'une saison par un pack trouvé dans Prowlarr : seuls les
    fichiers des trous sont téléchargés, dans la catégorie du client de la série.
    Le pack est noté comme un comblement en VO de la règle 4, qui le suit jusqu'à
    l'import puis cherche ces épisodes en français.
    """
    serie_id = serie["id"]
    cle_journal = f"trous-{serie_id}-s{saison}"
    titre = f"{serie.get('title')} S{saison:02d}"
    verdict: dict[str, Any] = {"arr": "sonarr", "download_id": cle_journal, "title": titre, "stage": "trous"}

    def fin(decision: str, details: str, signe: str = "⏭") -> dict[str, Any]:
        verdict.update(decision=decision, reason=details)
        _assist_record(cle_journal, "sonarr", titre, decision, details, dry, 4)
        _assist_log(4, signe, "sonarr", titre, details, dry)
        return verdict

    resume = (f"{len(trous)} épisode(s) diffusé(s) depuis plus de {_TROUS_ATTENTE // 3600} h, "
              "sans fichier ni téléchargement")
    if _pack_attempts(serie_id, saison) >= _PACK_MAX_ATTEMPTS:
        return fin("pack-hole-skip", f"{resume}, limite de remplacements atteinte pour cette saison : à voir à la main", "⚠")
    client = _series_client(clients, serie)
    categorie = _client_field(clients, client, ("tvCategory",)) if client else ""
    if not categorie:
        return fin("pack-hole-skip", f"{resume}, aucun client qBittorrent de Sonarr ne prend cette série", "⚠")
    de_la_saison = {e["id"] for e in episodes}
    releases = [q.get("title") or "" for q in file if q.get("episodeId") in de_la_saison]
    releases += [f.get("sceneName") or "" for f in _arr_request("GET", url, cle, "episodefile",
                                                                  params={"seriesId": serie_id}) or []
                 if f.get("seasonNumber") == saison]
    # Ni score ni qualité de référence : un pack que Sonarr accepterait vaut mieux que rien. Au seuil du
    # français du profil, il est pris pour la saison ; en dessous, il ne fait que combler, en VO.
    profil = _quality_profile(url, cle, serie.get("qualityProfileId"))
    ref = {"series_title": "", "series_titles": _series_titles(url, cle, serie, saison, [r for r in releases if r]),
           "season": saison, "season_pack": True, "score": profil["seuil_fr"], "rang": -1}
    with _pack_slots:
        cand, ecartes = _pack_candidate(url, cle, serie, saison, episodes, trous, ref, profil, set())
    if cand is None:
        return fin("pack-hole-none", f"{resume}, aucun pack de la saison ne les apporte{_ecarts(ecartes)}")
    seulement = _pack_files_for(cand, trous)
    details = (f"{resume} → pris {'en VO ' if cand['vo'] else ''}dans {cand['title']} ({cand['indexer']}, "
               f"{cand['seeders']} sources, score {cand['score']}) : {len(seulement)} fichier(s) téléchargé(s) "
               f"sur {len(cand['par_fichier'])}, dans {categorie}")
    verdict["vo"] = cand["title"]
    if not dry:
        _qbit_add(cand, categorie, {"auto_tmm": True}, seulement=seulement)
        _pack_register(serie_id, saison, cle_journal, cand, "vo", trous)
    return fin("pack-hole-fill", details, "✓")


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


# ── Assistant d'import, règle 6 : téléchargements morts ─────────────────────
#
# Un torrent sans source reste dans la file de Sonarr ou de Radarr indéfiniment.
# Aucun des deux ne le remplace : il refuse même toute autre version tant que
# celle-ci occupe la file. La demande attend, des mois parfois. Passé
# IMPORT_ASSIST_MORT_JOURS sans rien recevoir, l'assistant lance une course :
# la meilleure autre version, choisie par l'*arr lui-même, est prise en plus,
# sans rien retirer — un seeder peut revenir, même après des mois. Le premier
# téléchargement fini et importé gagne, original compris ; les autres sortent
# de la file sans blacklist et partent en corbeille. Si l'alternative meurt à
# son tour, la suivante est prise. Rien n'est supprimé. Une prise refusée par
# l'*arr compte comme un essai : la suivante attend la période suivante.

# Files *arr d'un téléchargement en cours.
_MORT_ETATS_FILE = {"downloading"}
# États qBittorrent où le torrent attend son tour, se vérifie ou a perdu ses
# fichiers : pas un téléchargement mort, ou un autre problème.
_MORT_ETATS_ATTENTE = {"queuedDL", "checkingDL", "checkingResumeData", "moving", "allocating", "missingFiles", "error"}
# Au plus tant de recherches par passage, les téléchargements les plus anciens
# d'abord : une recherche interroge tous les indexeurs.
_MORT_MAX_PAR_PASSAGE = 5
# Refus de l'*arr qui ne disqualifient pas une autre version : la place est prise
# dans la file — justement par le téléchargement mort — ou il lui manque des
# sources, qui peuvent revenir comme pour l'original.
_MORT_REFUS_ADMIS = ("in queue", "seeder")
# États de file où une version de la course peut encore se trouver.
_MORT_ETATS_COURSE = {"downloading", "importBlocked", "importPending", "importing", "failedPending"}


def _dead_since(t: dict[str, Any] | None, maintenant: float) -> float | None:
    """
    Depuis combien de secondes ce torrent n'a rien reçu, sans source connectée.
    None s'il est fini, actif, en attente, ou s'il ne peut pas être jugé (pas de
    date d'ajout).
    """
    if not t or (t.get("progress") or 0) >= 1 or not t.get("added_on"):
        return None
    if t.get("state") in _MORT_ETATS_ATTENTE or t.get("num_seeds") or t.get("dlspeed"):
        return None
    return maintenant - max(t.get("last_activity") or 0, t["added_on"])


def _never_started(t: dict[str, Any] | None) -> bool:
    """Rien reçu depuis son ajout, et aucune copie complète jamais vue : un torrent mort-né."""
    return bool(t) and not (t.get("downloaded") or 0) and not (t.get("completed") or 0) \
        and (t.get("seen_complete") or 0) <= 0


def _dead_threshold(t: dict[str, Any] | None, court: bool) -> float:
    """Inactivité au-delà de laquelle ce torrent est mort : IMPORT_ASSIST_MORT_JAMAIS_HEURES pour un mort-né si `court`."""
    jours = IMPORT_ASSIST_MORT_JOURS * 86400
    return min(jours, IMPORT_ASSIST_MORT_JAMAIS_HEURES * 3600) if court and _never_started(t) else jours


def _dead_desc(t: dict[str, Any] | None, inactif: float) -> str:
    if not _never_started(t):
        return f"plus rien reçu depuis {inactif / 86400:.0f} j"
    duree = f"{inactif / 3600:.0f} h" if inactif < 2 * 86400 else f"{inactif / 86400:.0f} j"
    return f"rien reçu depuis son ajout il y a {duree}, aucune copie complète jamais vue"


def _must_seed(t: dict[str, Any]) -> bool:
    """Un torrent fini, ou un privé entamé (hit-and-run), continue de seeder dans la corbeille."""
    progres = t.get("progress") or 0
    return progres >= 1 or (bool(t.get("private")) and progres > 0)


def _arr_conf(arr: str) -> tuple[str, str]:
    return (SONARR_URL, SONARR_API_KEY) if arr == "sonarr" else (RADARR_URL, RADARR_API_KEY)


def _races() -> list[sqlite3.Row]:
    with _db_lock:
        with get_db() as conn:
            return conn.execute("SELECT * FROM dead_race WHERE done = 0 ORDER BY id").fetchall()


def _race_update(race_id: int, **champs: Any) -> None:
    with _db_lock:
        with get_db() as conn:
            conn.execute(f"UPDATE dead_race SET {', '.join(f'{k} = ?' for k in champs)} WHERE id = ?",
                         (*champs.values(), race_id))


def run_dead_downloads(
    en_cours: list[tuple[str, str, str, dict[str, list[dict[str, Any]]]]], ctx: dict[str, Any],
) -> tuple[list[dict[str, Any]], int]:
    """Règle 6, sur les téléchargements en cours des files. Retourne (verdicts, journalisés maintenant)."""
    dry = ctx["apercu"] or IMPORT_ASSIST_MORT_DRY_RUN
    maintenant = time.time()
    seuil = IMPORT_ASSIST_MORT_JOURS * 86400
    verdicts: list[dict[str, Any]] = []
    nouveaux = 0

    def noter(verdict: dict[str, Any] | None, nouveau: bool) -> None:
        nonlocal nouveaux
        if verdict is not None:
            verdicts.append(verdict)
            nouveaux += nouveau

    # 1. Les courses déjà lancées : le premier fini gagne. Aucune n'existe en simulation.
    #    Une course close à l'instant vient de retirer ses perdants de la file, qui
    #    a été lue avant : l'étape 2 ne doit pas les reprendre pour des morts.
    sortis: set[str] = set()
    if not dry:
        for course in _races():
            try:
                noter(*_race_resolve(course, ctx, sortis))
            except Exception as e:
                log.error("  ⚠ [%s] [%s] course %s en erreur (%s)", _regle(6), course["arr"], course["original"], e)

    # 2. Les téléchargements morts : une course commence, ou repart avec la version
    #    suivante quand tous ses téléchargements sont morts depuis une période. Un
    #    mort-né est jugé après IMPORT_ASSIST_MORT_JAMAIS_HEURES, sous sa propre
    #    simulation tant qu'il n'a pas atteint IMPORT_ASSIST_MORT_JOURS.
    court_actif = not IMPORT_ASSIST_MORT_JAMAIS_DRY_RUN
    courses = _races()
    files = {arr: par_dl for arr, _, _, par_dl in en_cours}
    morts = []
    for arr, url, cle, par_dl in en_cours:
        for dl, records in par_dl.items():
            t = ctx["torrents"].get(dl)
            inactif = _dead_since(t, maintenant)
            if inactif is not None and inactif >= _dead_threshold(t, True) and dl not in sortis:
                morts.append((inactif, arr, url, cle, dl, records))
    morts.sort(key=lambda m: -m[0])
    vues: set[int] = set()
    lancees = attente = 0
    for inactif, arr, url, cle, dl, records in morts:
        course = next((c for c in courses if c["arr"] == arr and dl in json.loads(c["hashes"])), None)
        if course is not None:
            if course["id"] in vues:
                continue
            vues.add(course["id"])
            presents = [ctx["torrents"][h] for h in json.loads(course["hashes"]) if h in ctx["torrents"]]
            inactifs = [_dead_since(x, maintenant) for x in presents]
            seuil_course = max(_dead_threshold(x, court_actif) for x in presents)
            if any(d is None or d < _dead_threshold(x, court_actif) for x, d in zip(presents, inactifs, strict=True)) \
                    or maintenant - (course["last_try"] or 0) < seuil_course:
                continue
            inactif = min(inactifs)
            if course["original"] in files[arr]:
                # La course se poursuit au nom de l'original, dont le titre est alors exclu.
                dl, records = course["original"], files[arr][course["original"]]
        # Un mort-né pas encore mort depuis IMPORT_ASSIST_MORT_JOURS : délai court, simulation propre.
        court = inactif < seuil
        seuil_dl = _dead_threshold(ctx["torrents"].get(dl), True) if court else seuil
        dry_dl = dry or (court and IMPORT_ASSIST_MORT_JAMAIS_DRY_RUN)
        if _assist_recent(dl, ("dead-race", "dead-wait"), dry_dl, seuil_dl) or _assist_seen(dl, "dead-error", dry_dl):
            continue  # déjà cherché il y a moins d'une période, ou en erreur il y a moins de 24 h
        # Une course qui a épuisé ses essais ne cherche plus : elle ne prend pas de place.
        cherche = course is None or len(json.loads(course["tried"])) < IMPORT_ASSIST_MORT_ESSAIS
        if cherche and lancees >= _MORT_MAX_PAR_PASSAGE:
            attente += 1
            continue
        lancees += cherche
        try:
            # En simulation du délai court, rien de ce qui en découle n'agit, règle 7 comprise.
            ctx_dl = dict(ctx, apercu=True) if court and IMPORT_ASSIST_MORT_JAMAIS_DRY_RUN else ctx
            noter(*_race_try(arr, url, cle, dl, records, course, inactif, dry_dl, ctx_dl))
        except Exception as e:
            # Journalisé : sans ça, chaque passage relancerait une recherche sur tous les indexeurs.
            titre = records[0].get("title") or dl
            log.error("  ⚠ [%s] [%s] %s en erreur (%s)", _regle(6), arr, titre, e)
            _assist_record(dl, arr, titre, "dead-error", str(e), dry_dl, 6)
            noter({"arr": arr, "download_id": dl, "title": titre, "decision": "dead-error", "reason": str(e)}, True)
    if attente:
        log.info("  [%s] %d autre(s) téléchargement(s) mort(s) attendront le passage suivant", _regle(6), attente)
    return verdicts, nouveaux


def _race_grabs(url: str, cle: str, hashes: set[str]) -> tuple[set[str], set[tuple[str, str]]]:
    """
    Ce que l'*arr a noté à la prise de ces téléchargements : le guid de chaque
    release chez son indexeur, et les couples (indexeur, titre). Son historique ne
    répond qu'au downloadId écrit comme il le note, en majuscules.
    """
    guids: set[str] = set()
    prises: set[tuple[str, str]] = set()
    for h in hashes:
        rep = _arr_request("GET", url, cle, "history", params={"downloadId": h.upper(), "pageSize": 200}) or {}
        for g in rep.get("records") or []:
            if g.get("eventType") == "grabbed":
                data = g.get("data") or {}
                if data.get("guid"):
                    guids.add(data["guid"])
                prises.add(((data.get("indexer") or "").lower(), (g.get("sourceTitle") or "").lower()))
    return guids, prises


def _alternatives(arr: str, url: str, cle: str, film: int | None, episodes: set[int], guids: set[str],
                  prises: set[tuple[str, str]], essayees: list[str], hashes: set[str]) -> list[dict[str, Any]]:
    """
    Les autres versions du même film ou des mêmes épisodes, d'après la recherche
    interactive de l'*arr, dans son ordre de préférence. Seuls les refus « la place
    est prise dans la file » et « pas assez de sources » sont passés outre : une
    version sans source peut revenir, comme l'original.

    La release déjà prise se reconnaît à son guid, ou à son titre sur le même
    indexeur. Le nom du torrent ne dit rien (`Parking.mkv`), et le titre noté à la
    prise peut différer de celui de la recherche (accents abîmés). La même release
    sur un autre tracker reste une vraie alternative : c'est un autre torrent, avec
    d'autres sources. Un torrent que qBittorrent a déjà, `hashes`, est écarté quand
    l'indexeur donne son hash ; sinon, il refusera la prise, comptée comme un essai.
    """
    if arr == "radarr":
        resultats = _arr_request("GET", url, cle, "release", params={"movieId": film}, timeout=300) or []
        memes = [r for r in resultats if r.get("mappedMovieId") == film]
    else:
        if not episodes:
            return []
        resultats = _arr_request("GET", url, cle, "release", params={"episodeId": min(episodes)}, timeout=300) or []
        memes = [r for r in resultats if episodes <= {e.get("id") for e in r.get("mappedEpisodeInfo") or []}]
    deja = {t.lower() for t in essayees}
    return [
        r for r in memes
        if r.get("protocol") == "torrent" and r.get("guid") not in guids
        and ((r.get("indexer") or "").lower(), (r.get("title") or "").lower()) not in prises
        and (r.get("title") or "").lower() not in deja
        and (r.get("infoHash") or "").lower() not in hashes
        and all(any(a in m.lower() for a in _MORT_REFUS_ADMIS) for m in r.get("rejections") or [])
    ]


def _race_try(arr: str, url: str, cle: str, dl: str, records: list[dict[str, Any]], course: sqlite3.Row | None,
              inactif: float, dry: bool, ctx: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    """
    Prend en plus la meilleure version pas encore essayée. Sans elle, l'original
    reste seul et en route. Si la bibliothèque a déjà mieux, il n'y a rien à
    remplacer : la règle 7 retire le téléchargement mort, sans rien chercher.
    """
    titre = records[0].get("title") or dl
    if course:
        essayees, hashes = json.loads(course["tried"]), set(json.loads(course["hashes"]))
        media, episodes = course["media_id"], set(json.loads(course["episodes"] or "[]"))
    else:
        essayees, hashes = [], {dl}
        media = records[0].get("movieId") if arr == "radarr" else records[0].get("seriesId")
        episodes = {r["episodeId"] for r in records if r.get("episodeId")} if arr == "sonarr" else set()
    verdict: dict[str, Any] = {"arr": arr, "download_id": dl, "title": titre}
    depuis = _dead_desc(ctx["torrents"].get(dl), inactif)
    if not course:
        # La qualité et le score que l'*arr donne à cette release, tels que les lit _library_better.
        info = "parsedMovieInfo" if arr == "radarr" else "parsedEpisodeInfo"
        p = {info: {"quality": records[0].get("quality")}, "customFormatScore": records[0].get("customFormatScore")}
        mieux = _library_better(arr, url, cle, media, records, p)
        if mieux:
            t = ctx["torrents"][dl]
            return _useless(arr, url, cle, dl, records, t, ctx, verdict,
                            f"{depuis}, et la bibliothèque a déjà mieux ({mieux}) que cette release ({_release_desc(arr, p)})",
                            arreter=not _must_seed(t))
    if len(essayees) >= IMPORT_ASSIST_MORT_ESSAIS:
        details = f"{depuis}, {len(essayees)} autre(s) version(s) déjà essayée(s), sans résultat : on attend que l'une reparte"
        decision = "dead-wait"
    else:
        guids, prises = _race_grabs(url, cle, hashes)
        prises.add(((records[0].get("indexer") or "").lower(), titre.lower()))
        cands = _alternatives(arr, url, cle, media, episodes, guids, prises, essayees, hashes | set(ctx["torrents"]))
        if not cands:
            details = f"{depuis}, aucune autre version trouvée : on attend, le torrent reste en route"
            decision = "dead-wait"
        else:
            alt = cands[0]
            decision = "dead-race"
            details = (f"{depuis} → {'essai %d : ' % (len(essayees) + 1) if essayees else ''}autre version prise en plus, "
                       f"{alt.get('title')} ({alt.get('indexer')}, {alt.get('seeders')} source(s), score "
                       f"{alt.get('customFormatScore')}) ; la première finie gagnera")
            verdict["alternative"] = alt.get("title")
            if not dry:
                try:
                    # Comme « télécharger quand même » dans l'*arr : il passe outre la place prise dans sa file.
                    # Un seul appel : qBittorrent refuserait de nouveau un torrent qu'il a déjà.
                    _arr_request("POST", url, cle, "release", json={"guid": alt["guid"], "indexerId": alt["indexerId"]},
                                 timeout=120, retries=0)
                except Exception as e:
                    decision = "dead-refused"
                    details = (f"{depuis} → {alt.get('title')} ({alt.get('indexer')}) refusée par {arr.capitalize()} "
                               f"({e}) : comptée comme un essai, une autre version à la période suivante")
                essayees = essayees + [alt.get("title")]
                if course:
                    _race_update(course["id"], tried=json.dumps(essayees), last_try=int(time.time()))
                else:
                    with _db_lock:
                        with get_db() as conn:
                            conn.execute(
                                "INSERT INTO dead_race (arr, media_id, episodes, original, hashes, tried, last_try) "
                                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                                (arr, media, json.dumps(sorted(episodes)) if episodes else None, dl, json.dumps([dl]),
                                 json.dumps(essayees), int(time.time())),
                            )
    if decision == "dead-wait" and course and not dry:
        _race_update(course["id"], last_try=int(time.time()))  # on y revient dans une période
    verdict.update(decision=decision, reason=details)
    # Une fois par période au plus : le verdict lui-même espace les recherches.
    _assist_record(dl, arr, titre, decision, details, dry, 6)
    _assist_log(6, {"dead-race": "✓", "dead-refused": "⚠"}.get(decision, "⏭"), arr, titre, details, dry)
    return verdict, True


def _race_resolve(course: sqlite3.Row, ctx: dict[str, Any], sortis: set[str]) -> tuple[dict[str, Any] | None, bool]:
    """
    Le premier téléchargement de la course fini et importé — ses vidéos sont dans
    la bibliothèque du média — gagne. Les autres sortent de la file sans
    blacklist et partent en corbeille, arrêtés, sauf un privé entamé ou un fini,
    qui continuent de seeder. Une course close ajoute ses téléchargements à `sortis`.
    """
    arr, media = course["arr"], course["media_id"]
    url, cle = _arr_conf(arr)
    hashes = json.loads(course["hashes"])
    essayees = set(json.loads(course["tried"]))
    # Les versions prises se retrouvent par leur grab dans l'historique.
    route, param = ("history/movie", "movieId") if arr == "radarr" else ("history/series", "seriesId")
    for g in _arr_request("GET", url, cle, route, params={param: media}) or []:
        h = (g.get("downloadId") or "").lower()
        if g.get("eventType") == "grabbed" and g.get("sourceTitle") in essayees and h and h not in hashes:
            hashes.append(h)
    if hashes != json.loads(course["hashes"]):
        _race_update(course["id"], hashes=json.dumps(hashes))
    dossier = (_arr_request("GET", url, cle, f"{'movie' if arr == 'radarr' else 'series'}/{media}") or {}).get("path") or ""
    inodes = set(scan_video_files(dossier)) if dossier else set()

    gagnant, finis = None, []
    for h in hashes:
        t = ctx["torrents"].get(h)
        if not t or (t.get("progress") or 0) < 1:
            continue
        lies = 0
        for v in _torrent_videos(t) or []:
            try:
                lies += os.stat(v).st_ino in inodes
            except OSError:
                continue
        if lies:
            gagnant = t
            break
        finis.append(t)
    if gagnant is None:
        if not any(ctx["torrents"].get(h) for h in hashes):
            _race_update(course["id"], done=1)  # tous retirés de qBittorrent : plus de course
            sortis.update(hashes)
            return None, False
        if not finis or _assist_seen(course["original"], "dead-blocked", False):
            return None, False
        details = (f"{finis[0].get('name')} a fini, mais {arr.capitalize()} ne l'a pas importé : "
                   "import à faire à la main ; les autres versions continuent")
        _assist_record(course["original"], arr, finis[0].get("name") or "", "dead-blocked", details, False, 6)
        _assist_log(6, "⚠", arr, finis[0].get("name") or "", details, False)
        return {"arr": arr, "download_id": course["original"], "title": finis[0].get("name"),
                "decision": "dead-blocked", "reason": details}, True

    # Le gagnant est là : les autres versions sortent de la file, sans blacklist.
    file = {dl: recs for dl, recs in _assist_blocked(url, cle, _MORT_ETATS_COURSE).items()}
    perdants = [h for h in hashes if h != gagnant["hash"].lower()]
    retires = 0
    for h in perdants:
        if h in file:
            _arr_request("DELETE", url, cle, f"queue/{file[h][0]['id']}",
                         params={"removeFromClient": "false", "blocklist": "false", "skipRedownload": "true"})
        t = ctx["torrents"].get(h)
        if t and t.get("category") != TARGET_CAT:
            retires += _assist_trash(t, arreter=not _must_seed(t))
    _race_update(course["id"], hashes=json.dumps(hashes), done=1)
    sortis.update(hashes)
    details = f"{gagnant.get('name')} a fini le premier → {retires} autre(s) version(s) retirée(s) de la file, sans blacklist, et en corbeille"
    _assist_record(course["original"], arr, gagnant.get("name") or "", "dead-won", details, False, 6)
    _assist_log(6, "✓", arr, gagnant.get("name") or "", details, False)
    return {"arr": arr, "download_id": course["original"], "title": gagnant.get("name"),
            "decision": "dead-won", "reason": details}, True


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
            n = _delete_gesture([path], "webhook", f"Sonarr {event}", check_links=True)
            return jsonify({"moved": n, "path": path})

    elif event == "SeriesDelete":
        path = data.get("series", {}).get("path")
        if path:
            log.info("Sonarr SeriesDelete: %s", path)
            n = _delete_gesture([path], "webhook", "Sonarr SeriesDelete", check_links=False,
                                deleted_files=data.get("deletedFiles"))
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
            n = _delete_gesture([path], "webhook", f"Radarr {event}", check_links=True)
            return jsonify({"moved": n, "path": path})

    elif event == "MovieDelete":
        path = data.get("movie", {}).get("folderPath")
        if path:
            log.info("Radarr MovieDelete: %s", path)
            n = _delete_gesture([path], "webhook", "Radarr MovieDelete", check_links=False,
                                deleted_files=data.get("deletedFiles"))
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
                    moved += _delete_gesture([path], "webhook", "upgrade", check_links=True)

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
    ItemAdded programme justement une sync, pour qu'un média tout juste arrivé
    soit connu avant qu'on puisse le supprimer.
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

    if notif == "ItemAdded":
        # Un par épisode d'un pack : journalisé en debug, la sync qui suit dit combien.
        log.debug("Reçu event Jellyfin: ItemAdded | type=%s | %s", item_type, label)
        if item_type not in ("Movie", "Episode", "Video"):
            return jsonify({"status": "ignored", "notification": notif, "type": item_type})
        programme = schedule_jellyfin_index()
        return jsonify({"status": "index scheduled" if programme else "index already scheduled"})

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
    paths = _jellyfin_paths_for_item(item_id, item_type)
    resolution = "index"

    # Pas encore relié par une sync : l'id se recalcule depuis les chemins connus.
    if not paths:
        try:
            paths = _jellyfin_paths_by_id(item_id, item_type)
        except Exception as e:
            log.error("  Id recalculé : recherche impossible (%s)", e)
            paths = []
        if paths:
            resolution = "id recalculé"
            log.info("  Retrouvé par son id recalculé : %s",
                     paths[0] if len(paths) == 1 else f"{len(paths)} fichier(s) sous {os.path.dirname(paths[0])}")

    if not paths:
        log.warning(
            "  Item Jellyfin non résolu: %s (%s). Aucun chemin connu ne donne cet id : hors torrents et hors "
            "*arr, rien à faire.",
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
        if delegated and not delegated.get("garde"):
            vides = _trash_emptied_torrents(disparus, dry_run)
            return jsonify({
                "status": "delegated",
                "delegated_to": delegated,
                "deleted_paths": len(disparus),
                "emptied_torrents": vides,
                "resolution": resolution,
                "dry_run": dry_run,
            })
        if delegated:
            log.info("  Fiche gardée : les torrents du fichier remplacé passent par la corbeille")
        else:
            log.info("  Média inconnu de Sonarr/Radarr — déplacement direct des torrents")

    # Un seul point d'entrée : la branche préfixe de move_torrents_for_path traite
    # tous les inodes d'un dossier en une passe, au lieu d'une session qBit par épisode.
    root = _common_root(disparus)

    if root:
        log.info("  Jellyfin ItemDeleted (%s) %s: %s", resolution, item_type or "?", root)
        # La corbeille globale ne reçoit que les chemins disparus : le dossier commun
        # peut contenir des fichiers encore en place.
        moved = _delete_gesture(list(disparus), "jellyfin", f"Jellyfin ItemDeleted {item_type or '?'}",
                                check_links=True, dry_run=dry_run, anciens=[root])
    else:
        # Les chemins ne partagent pas de dossier commun assez profond : on traite
        # chacun séparément plutôt que de risquer un préfixe trop large.
        log.warning(
            "  Pas de dossier commun sûr pour %d chemins — traitement individuel",
            len(disparus),
        )
        moved = _delete_gesture(list(disparus), "jellyfin", f"Jellyfin ItemDeleted {item_type or '?'}",
                                check_links=True, dry_run=dry_run)

    return jsonify({
        "moved": moved,
        "root": root or None,
        "deleted_paths": len(disparus),
        "emptied_torrents": _trash_emptied_torrents(disparus, dry_run),
        "resolution": resolution,
        "dry_run": dry_run,
    })


def _a_des_videos(racine: str) -> bool:
    """Une vidéo est-elle encore sur le disque sous cette racine de torrent, fichier ou dossier ?"""
    if os.path.isfile(racine):
        return os.path.splitext(racine)[1].lower() in VIDEO_EXTENSIONS
    return any(os.path.splitext(n)[1].lower() in VIDEO_EXTENSIONS for _, _, noms in os.walk(racine) for n in noms)


def _trash_emptied_torrents(chemins: list[str], dry_run: bool) -> int:
    """
    Les torrents dont Jellyfin vient d'effacer le fichier : quand sa bibliothèque lit
    le dossier de téléchargement (« Films »), c'est le fichier du torrent qu'il
    supprime. S'il ne reste aucune de leurs vidéos sur le disque, ils ne peuvent plus
    seeder. La corbeille globale ne les voit pas, elle part des noms restés sur le
    disque. Et qBittorrent ne les marque pas « fichiers manquants » de lui-même :
    Oppenheimer est resté 16 h en seed sur du vide. Ils partent en corbeille, comme
    les torrents perdus de /cleanup. Rend leur nombre.
    """
    hashes: set[str] = set()
    with _db_lock:
        with get_db() as conn:
            for i in range(0, len(chemins), _SQL_CHUNK):
                lot = chemins[i:i + _SQL_CHUNK]
                hashes |= {r[0].lower() for r in conn.execute(
                    f"SELECT DISTINCT torrent_hash FROM files WHERE torrent_hash IS NOT NULL "
                    f"AND path IN ({','.join('?' * len(lot))})", lot)}
    if not hashes:
        return 0
    try:
        with qbit_session() as s:
            vides = [t for t in qbit_get_torrents(s) if (t.get("hash") or "").lower() in hashes
                     and t.get("category") and t.get("category") != TARGET_CAT
                     and not _a_des_videos(torrent_root(t))]
            for t in vides:
                log.info("  %s🗑 Plus aucune vidéo sur le disque, torrent en corbeille : [%s] %s",
                         "[DRY RUN] " if dry_run else "", t.get("category"), t.get("name"))
            if not vides or dry_run:
                return len(vides)
            plan = {"torrents": [{"hash": t["hash"], "name": t.get("name"), "category": t.get("category")}
                                 for t in vides], "corbeilles": []}
            return _trash_apply(plan, s, "jellyfin")["torrents"]
    except Exception as e:
        log.error("  Torrents vidés par Jellyfin non traités : %s", e)
        return 0


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
    """Endpoint pour forcer une sync manuelle. `force=true` passe outre son garde-fou, une fois."""
    token = request.args.get("token") or request.headers.get("X-Token")
    if not verify_token(token):
        return jsonify({"error": "Unauthorized"}), 401
    force = request.args.get("force", "false").lower() in ("1", "true", "yes")
    threading.Thread(target=sync_db, kwargs={"force": force}, daemon=True).start()
    return jsonify({"status": "sync started", "force": force})



def _arr_queue_hashes() -> set[str]:
    """Les téléchargements que Sonarr et Radarr suivent encore dans leur file. Lève si une file ne répond pas."""
    en_file: set[str] = set()
    for nom, url, cle in _assist_arrs():
        inconnus = "includeUnknownMovieItems" if nom == "radarr" else "includeUnknownSeriesItems"
        for r in _queue_pages(url, cle, {inconnus: "true"}):
            if r.get("downloadId"):
                en_file.add(r["downloadId"].lower())
    return en_file


def _trace_ancien(sim: dict[str, Any] | None, n: int, dry_run: bool) -> None:
    """En simulation : ce que l'ancien /cleanup a fait, à côté de la décision de la corbeille globale."""
    if not sim or not sim.get("id"):
        return
    with _db_lock:
        with get_db() as conn:
            conn.execute("UPDATE corbeille_log SET ancien = ? WHERE id = ?", (n, sim["id"]))
    log.info("  [SIMULATION] Corbeille globale (cleanup) : l'ancien /cleanup %s %d torrent(s)",
             "aurait déplacé" if dry_run else "a déplacé", n)


def _cleanup_refus() -> str:
    """
    La raison de refuser /cleanup d'après la dernière sync réussie, ou "" : trop
    vieille (plus de deux SYNC_INTERVAL), ou qui a fait perdre plus de la moitié de
    l'index, jusqu'à le vider. Sans sync réussie, rien n'est refusé : /cleanup en
    lance une avant de juger.
    """
    with _db_lock:
        with get_db() as conn:
            dernier = conn.execute(
                "SELECT finished_at, rows_before, rows_after FROM sync_log "
                "WHERE status = 'success' ORDER BY id DESC LIMIT 1"
            ).fetchone()
    if not dernier:
        return ""
    age, max_age = int(time.time()) - (dernier["finished_at"] or 0), SYNC_INTERVAL * 2
    if age > max_age:
        return f"dernière sync réussie il y a {age // 3600} h (max {max_age // 3600} h). Lancez /sync d'abord"
    avant, apres = dernier["rows_before"] or 0, dernier["rows_after"] or 0
    if avant and apres < avant * 0.5:
        return f"la dernière sync a perdu {(1 - apres / avant) * 100:.0f} % des lignes ({avant} → {apres}). Vérifiez la base"
    return ""


@app.route("/cleanup", methods=["POST"])
def cleanup_orphans():
    """
    Recherche et déplace les torrents orphelins (upgrades ratés passés). Avec la
    corbeille globale active, c'est elle qui juge ; en simulation, elle trace ce
    qu'elle ferait, avant l'ancien balayage.
    """
    token = request.args.get("token") or request.headers.get("X-Token")
    if not verify_token(token):
        return jsonify({"error": "Unauthorized"}), 401

    dry_run = request.args.get("dry_run", "false").lower() in ("1", "true", "yes")

    def run_cleanup():
        prefix = "[DRY RUN] " if dry_run else ""
        log.info("=== %sDébut du nettoyage des orphelins ===", prefix)

        # ── Garde-fou : vérifier que la dernière sync est récente et OK ───────
        refus = _cleanup_refus()
        if refus:
            log.warning("⚠️ Cleanup refusé : %s", refus)
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
            statut = sync_db()
            if statut != "success":
                # Un index refusé ou une sync ratée ne disent pas l'état réel des liens.
                log.warning("⚠️ Cleanup interrompu : la sync préalable n'a pas réussi (%s)", statut)
                return

        # Un téléchargement qu'un *arr suit encore dans sa file n'est jamais orphelin,
        # même bloqué depuis des jours : on attend qu'il soit importé ou retiré.
        try:
            en_file = _arr_queue_hashes()
        except Exception as e:
            log.error("⚠️ Cleanup interrompu : file des *arr illisible (%s)", e)
            return

        # La corbeille globale juge au repos. Active, elle remplace l'ancien balayage ;
        # en simulation, elle trace ce qu'elle ferait, avant lui.
        if CORBEILLE_GLOBALE == "active":
            rest_trash(dry_run, en_file)
            log.info("=== %sNettoyage terminé (corbeille globale) ===", prefix)
            return
        sim = rest_trash(True, en_file) if CORBEILLE_GLOBALE == "simulation" else None

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

        # ── Étape 4 bis : file des *arr ──────────────────────────────────────
        dans_file = [t for t in torrents if t["torrent_hash"].lower() in en_file]
        if dans_file:
            torrents = [t for t in torrents if t["torrent_hash"].lower() not in en_file]
            log.info("Étape 4 bis (file des *arr) : %d torrent(s) épargnés, encore suivis par un *arr",
                     len(dans_file))
            for t in dans_file[:10]:
                log.info("    ⏳ %s", t["torrent_name"])

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
            _trace_ancien(sim, 0, dry_run)
            return

        for t in torrents:
            log.info("  %sOrphan [%s] %s → %s",
                     "[DRY RUN] " if dry_run else "✓ ",
                     t["category"], t["torrent_name"], TARGET_CAT)

        if dry_run:
            log.info("=== [DRY RUN] %d torrents seraient déplacés ===", len(torrents))
            _trace_ancien(sim, len(torrents), dry_run)
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
            _trace_ancien(sim, len(torrents), dry_run)
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

    Un torrent de corbeille (tag sans-torrent) est retiré sans ses fichiers. Un vrai
    torrent revient dans sa catégorie d'origine, même vide.
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

                # Regroupe par catégorie d'origine. Un torrent de corbeille se reconnaît à son
                # tag, jamais à une catégorie d'origine vide : un vrai torrent noté sans
                # catégorie revient sans catégorie, il n'est pas retiré de qBittorrent.
                by_cat: dict[str | None, list[dict[str, str]]] = {}
                for h, (name, cat) in hash_to_cat.items():
                    if h not in in_target:
                        continue
                    autre = partages.get(h.lower())
                    if autre:
                        log.info("  ⏭ [%s] %s non restauré (fichiers partagés avec [%s] %s)",
                                 TARGET_CAT, name, autre.get("category", "?"), autre.get("name", "?"))
                        continue
                    cle = None if _CORBEILLE_TAG in _tags(in_target[h]) else (cat or "")
                    by_cat.setdefault(cle, []).append({"hash": h, "name": name})

                if not by_cat:
                    log.info("Aucun torrent à restaurer.")
                    return

                restored: list[dict[str, str]] = []
                for cat, cat_torrents in by_cat.items():
                    hashes = "|".join(t["hash"] for t in cat_torrents)
                    if cat is None:
                        # Torrent de corbeille de la corbeille globale : le retirer sans
                        # ses fichiers laisse leurs noms en place, comme avant le geste.
                        try:
                            rep = s.post(f"{QBIT_URL}/api/v2/torrents/delete", timeout=10,
                                         data={"hashes": hashes, "deleteFiles": "false"})
                            rep.raise_for_status()
                        except requests.RequestException as e:
                            log.error("  ✗ Retrait des torrents de corbeille impossible : %s", e)
                            continue
                        for t in cat_torrents:
                            log.info("  ↩ Retiré [%s] %s, fichiers gardés", TARGET_CAT, t["name"])
                            restored.append(t)
                        continue
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
                        log.info("  ↩ Restauré [%s] %s → %s", TARGET_CAT, t["name"], cat or "(sans catégorie)")
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


@app.route("/corbeille/history")
def corbeille_history():
    """Décisions de la corbeille globale, simulées ou réelles, les plus récentes d'abord."""
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
                "SELECT id, source, motif, chemin, dry_run, resume, decision, ancien, "
                "datetime(at, 'unixepoch') AS at FROM corbeille_log ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
    return jsonify([{**dict(r), "decision": json.loads(r["decision"]) if r["decision"] else None} for r in rows])


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
    log.info("Corbeille globale : %s", {"off": "désactivée", "simulation": "en simulation",
                                       "active": "ACTIVE"}[CORBEILLE_GLOBALE])
    threading.Thread(target=sync_loop, daemon=True).start()
    threading.Thread(target=import_assist_loop, daemon=True).start()
    return app


if __name__ == "__main__":
    init_db()
    threading.Thread(target=sync_loop, daemon=True).start()
    threading.Thread(target=import_assist_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=5000)
