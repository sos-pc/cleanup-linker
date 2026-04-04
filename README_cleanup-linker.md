# cleanup-linker

Service Python/Flask qui écoute les webhooks Sonarr et Radarr pour déplacer automatiquement vers la catégorie `cleanuparr-unlinked` tous les torrents qBittorrent liés à un contenu supprimé ou upgradé — original **et** cross-seeds.

---

## Prérequis

| Dépendance | Rôle |
|---|---|
| **qBittorrent** | Client torrent (API v2) |
| **Sonarr** | Gestionnaire de séries (webhooks) |
| **Radarr** | Gestionnaire de films (webhooks) |
| **cross-seed** | Fournit sa DB SQLite pour le mapping des cross-seeds |
| **cleanuparr** | Traite la catégorie `cleanuparr-unlinked` pour supprimer les fichiers |
| **Docker** | Environnement d'exécution |
| **Réseau Docker `mediastack`** | Permet la communication entre containers |

---

## Fonctionnement

### Vue d'ensemble

```
Sonarr/Radarr
     │  webhook DELETE/UPGRADE
     ▼
cleanup-linker
     │  1. cherche inode via DB locale
     │  2. cherche torrents liés via DB cross-seed
     ▼
qBittorrent API
     │  setCategory → "cleanuparr-unlinked"
     ▼
cleanuparr
     │  supprime quand l'espace manque
     ▼
Espace disque libéré
```

### Les deux bases de données

**DB locale (`db.sqlite`)** — maintenue par cleanup-linker :

Mappe chaque fichier vidéo sur le disque à son inode et au hash du torrent qBit correspondant. Couvre trois zones :
- `/data/Media/` — hardlinks créés par Sonarr/Radarr
- `/data/Multimedia/<catégories qBit>/` — fichiers sources des torrents
- `/data/Multimedia/cross-seeds/` — hardlinks créés par cross-seed

```
inode  │ path                                         │ torrent_hash
───────┼──────────────────────────────────────────────┼──────────────
295671 │ /data/Media/Films/MonFilm/MonFilm.mkv        │ abc123...
295671 │ /data/Multimedia/Films/MonFilm.mkv           │ abc123...
295671 │ /data/Multimedia/cross-seeds/C411/MonFilm.mkv│ def456...
```

**DB cross-seed (`cross-seed.db`)** — maintenue par cross-seed :

Contient dans `client_searchee` tous les torrents présents dans qBit avec leur nom, hash et catégorie. Permet de retrouver tous les torrents qui partagent le même nom (donc le même contenu) que le torrent original.

### Flux de suppression détaillé

```
1. Radarr/Sonarr supprime ou upgrade un fichier
        │
        ▼
2. Webhook → cleanup-linker reçoit le path du fichier
   ex: /data/Media/Films/MonFilm (2024)/MonFilm.mkv
        │
        ▼
3. DB locale : cherche ce path → trouve l'inode
   Si pas trouvé : recherche partielle sur le dossier parent
        │
        ▼
4. DB locale : inode → torrent_hash(es) original(aux)
        │
        ▼
5. DB cross-seed : pour chaque hash original,
   cherche tous les torrents avec le même nom dans client_searchee
   → trouve les cross-seeds (même nom, hash différent)
        │
        ▼
6. qBit API : setCategory(tous les hashes, "cleanuparr-unlinked")
        │
        ▼
7. DB locale : DELETE WHERE inode = ?
        │
        ▼
8. cleanuparr prend le relais selon sa propre logique
```

### Sync automatique

Toutes les 12h (configurable), le scanner reconstruit entièrement la DB locale :

1. Récupère les catégories qBit dynamiquement via l'API → save_paths
2. Scanne chaque dossier de catégorie pour indexer les fichiers vidéo
3. Scanne `/data/Multimedia/cross-seeds/` pour les hardlinks cross-seed
4. Scanne `/data/Media/` pour les hardlinks Sonarr/Radarr
5. Croise les inodes avec la liste des torrents qBit → peuple la DB

La sync couvre automatiquement les torrents ajoutés directement dans qBit sans passer par Sonarr/Radarr.

---

## Installation

### Structure des fichiers

```
Mediastack/
├── docker-compose-cleanup-linker.yaml
└── config/
    └── cleanup-linker/
        ├── app.py
        ├── requirements.txt
        ├── Dockerfile
        └── db.sqlite          ← créé automatiquement
```

### docker-compose

```yaml
services:
  cleanup-linker:
    build:
      context: ./config/cleanup-linker
    container_name: cleanup-linker
    restart: unless-stopped
    ports:
      - "5001:5000"
    volumes:
      - ./config/cleanup-linker:/config
      - ./config/CrossSeed:/config/crossseed:ro
      - /data/Multimedia:/data/Multimedia:ro
      - /data/Media:/data/Media:ro
    environment:
      - QBIT_URL=http://192.168.1.111:8080
      - QBIT_USER=admin
      - QBIT_PASS=VOTRE_MOT_DE_PASSE
      - TARGET_CATEGORY=cleanuparr-unlinked
      - WEBHOOK_TOKEN=VOTRE_TOKEN_SECRET
      - DB_PATH=/config/db.sqlite
      - CROSSSEED_DB=/config/crossseed/cross-seed.db
      - SYNC_INTERVAL_HOURS=12

networks:
  default:
    name: mediastack
    external: true
```

### Variables d'environnement

| Variable | Défaut | Description |
|---|---|---|
| `QBIT_URL` | `http://192.168.1.111:8080` | URL de l'interface Web qBittorrent |
| `QBIT_USER` | `admin` | Identifiant qBittorrent |
| `QBIT_PASS` | — | Mot de passe qBittorrent |
| `TARGET_CATEGORY` | `cleanuparr-unlinked` | Catégorie de destination dans qBit |
| `WEBHOOK_TOKEN` | — | Token de sécurité pour les webhooks |
| `DB_PATH` | `/config/db.sqlite` | Chemin de la DB locale |
| `CROSSSEED_DB` | `/config/crossseed/cross-seed.db` | Chemin de la DB cross-seed |
| `SYNC_INTERVAL_HOURS` | `12` | Fréquence de resync en heures |

---

## Configuration Sonarr / Radarr

### Sonarr — Settings → Connect → + → Webhook

```
Name    : cleanup-linker
URL     : http://cleanup-linker:5000/webhook?token=VOTRE_TOKEN
Method  : POST
Triggers: ✅ On File Delete
          ✅ On Episode File Delete For Upgrade
          ✅ On Series Delete
          ✅ On Import Complete
```

### Radarr — Settings → Connect → + → Webhook

```
Name    : cleanup-linker
URL     : http://cleanup-linker:5000/webhook?token=VOTRE_TOKEN
Method  : POST
Triggers: ✅ On File Import
          ✅ On File Upgrade
          ✅ On Movie Delete
          ✅ On Movie File Delete
          ✅ On Movie File Delete For Upgrade
```

---

## Événements gérés

### Sonarr

| Event | Déclencheur | Action |
|---|---|---|
| `EpisodeFileDelete` | Suppression manuelle d'un épisode | Move vers cleanuparr-unlinked |
| `EpisodeFileDeleteForUpgrade` | Remplacement par meilleure qualité | Move vers cleanuparr-unlinked |
| `SeriesDelete` | Suppression d'une série entière | Move vers cleanuparr-unlinked |
| `Download` isUpgrade=false | Import normal | Ignoré |
| `Test` | Test webhook | Répond OK |

### Radarr

| Event | Déclencheur | Action |
|---|---|---|
| `MovieFileDelete` | Suppression manuelle d'un film | Move vers cleanuparr-unlinked |
| `MovieFileDeleteForUpgrade` | Remplacement par meilleure qualité | Move vers cleanuparr-unlinked |
| `MovieDelete` | Suppression d'un film entier | Move vers cleanuparr-unlinked |
| `Download` isUpgrade=false | Import normal | Ignoré |
| `Test` | Test webhook | Répond OK |

---

## Endpoints API

| Endpoint | Méthode | Auth | Description |
|---|---|---|---|
| `/webhook?token=...` | POST | Token | Reçoit les events Sonarr/Radarr |
| `/sync?token=...` | POST | Token | Force une resync manuelle immédiate |
| `/stats?token=...` | GET | Token | Statistiques de la DB |
| `/health` | GET | Non | Healthcheck |

### Exemple d'utilisation

```bash
# Forcer une resync manuelle
curl -XPOST 'http://192.168.1.111:5001/sync?token=VOTRE_TOKEN'

# Consulter les stats de la DB
curl -s 'http://192.168.1.111:5001/stats?token=VOTRE_TOKEN' | python3 -m json.tool
# Retourne :
# {
#     "linked_to_torrent": 13964,
#     "total_paths": 58000,
#     "unique_inodes": 11494
# }
```

---

## Limites connues

**Chemin non trouvé en DB** — Si Sonarr/Radarr supprime un fichier importé très récemment (entre deux syncs), le chemin peut ne pas être encore dans la DB locale. La prochaine sync résoudra ce cas. Force une resync manuelle si nécessaire.

**Torrents multi-saisons** — Certains torrents qBit regroupent plusieurs saisons sous un même nom de dossier. L'association inode peut être incomplète dans ces cas.

**Extensions vidéo** — Seuls les fichiers avec les extensions suivantes sont indexés : `.mkv`, `.mp4`, `.avi`, `.ts`, `.m2ts`, `.mov`, `.wmv`, `.flv`, `.iso`.
