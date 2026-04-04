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

La DB contient aussi une table `arr_managed` qui mémorise tous les hashes de torrents ayant un jour été importés par Sonarr ou Radarr. C'est ce qui permet de distinguer un torrent géré par les *arrs d'un ajout manuel.

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

Le webhook marque également le `downloadId` de chaque event dans la table `arr_managed` — ce qui garantit que les futurs `/cleanup` ne toucheront jamais les torrents ajoutés manuellement.

### Sync automatique

Toutes les 12h (configurable), le scanner reconstruit entièrement la DB locale :

1. Récupère les catégories qBit dynamiquement via l'API → save_paths
2. Scanne chaque dossier de catégorie pour indexer les fichiers vidéo
3. Scanne `/data/Multimedia/cross-seeds/` pour les hardlinks cross-seed
4. Scanne `/data/Media/` pour les hardlinks Sonarr/Radarr
5. Croise les inodes avec la liste des torrents qBit → peuple la DB

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
    image: ghcr.io/sos-pc/cleanup-linker:latest
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
      - SONARR_URL=http://sonarr:8989
      - SONARR_API_KEY=VOTRE_CLE_API_SONARR
      - RADARR_URL=http://radarr:7878
      - RADARR_API_KEY=VOTRE_CLE_API_RADARR

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
| `SONARR_URL` | — | URL Sonarr (requis pour `/bootstrap`) |
| `SONARR_API_KEY` | — | Clé API Sonarr (Settings → General) |
| `RADARR_URL` | — | URL Radarr (requis pour `/bootstrap`) |
| `RADARR_API_KEY` | — | Clé API Radarr (Settings → General) |

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
| `Download` isUpgrade=false | Import normal | Marqué arr_managed, ignoré sinon |
| `Test` | Test webhook | Répond OK |

### Radarr

| Event | Déclencheur | Action |
|---|---|---|
| `MovieFileDelete` | Suppression manuelle d'un film | Move vers cleanuparr-unlinked |
| `MovieFileDeleteForUpgrade` | Remplacement par meilleure qualité | Move vers cleanuparr-unlinked |
| `MovieDelete` | Suppression d'un film entier | Move vers cleanuparr-unlinked |
| `Download` isUpgrade=false | Import normal | Marqué arr_managed, ignoré sinon |
| `Test` | Test webhook | Répond OK |

---

## Endpoints API

| Endpoint | Méthode | Auth | Description |
|---|---|---|---|
| `/webhook?token=...` | POST | Token | Reçoit les events Sonarr/Radarr |
| `/sync?token=...` | POST | Token | Force une resync manuelle immédiate |
| `/bootstrap?token=...` | POST | Token | Peuple arr_managed depuis l'historique *arr |
| `/cleanup?token=...` | POST | Token | Déplace les torrents *arr orphelins |
| `/restore?token=...` | POST | Token | Restaure les torrents après un cleanup raté |
| `/stats?token=...` | GET | Token | Statistiques de la DB |
| `/health` | GET | Non | Healthcheck |

---

## Workflow premier démarrage

Le `/cleanup` repose sur la table `arr_managed` pour ne jamais toucher les torrents ajoutés manuellement. Après une première installation (ou si le service était absent pendant une longue période), il faut peupler cette table depuis l'historique Sonarr/Radarr.

**Étape 1 — Bootstrap (une seule fois)**

Teste d'abord en dry run pour vérifier combien d'entrées seraient importées :

```bash
curl -XPOST 'http://192.168.1.111:5001/bootstrap?token=VOTRE_TOKEN&dry_run=true'
```

Puis applique réellement :

```bash
curl -XPOST 'http://192.168.1.111:5001/bootstrap?token=VOTRE_TOKEN'
```

Surveille les logs :

```bash
docker logs cleanup-linker -f
# Sonarr: 1243 entrées marquées
# Radarr: 587 entrées marquées
# === Bootstrap terminé : 1830 hashes au total ===
```

**Étape 2 — Cleanup (dry run)**

Vérifie quels torrents seraient déplacés sans rien toucher :

```bash
curl -XPOST 'http://192.168.1.111:5001/cleanup?token=VOTRE_TOKEN&dry_run=true'
```

Les logs afficheront la liste des candidats avec `[DRY RUN]`. Vérifie que seuls des torrents gérés par *arr apparaissent, pas tes ajouts manuels.

**Étape 3 — Cleanup réel**

Si le dry run est correct :

```bash
curl -XPOST 'http://192.168.1.111:5001/cleanup?token=VOTRE_TOKEN'
```

---

## En cas de cleanup raté

Si des torrents ont été déplacés à tort vers `cleanuparr-unlinked`, l'endpoint `/restore` permet de les remettre en place **tant que la DB n'a pas été resyncée** (fenêtre de ~12h) :

```bash
curl -XPOST 'http://192.168.1.111:5001/restore?token=VOTRE_TOKEN'
```

Si la DB a déjà été resyncée, utilise le script `restore.py` avec les logs du cleanup :

```bash
# Sauvegarde les logs du cleanup dans un fichier
docker logs cleanup-linker > /tmp/cleanup.log

# Lance la restauration basée sur les logs
python3 restore.py /tmp/cleanup.log
```

---

## Utilisation courante

```bash
# Forcer une resync manuelle
curl -XPOST 'http://192.168.1.111:5001/sync?token=VOTRE_TOKEN'

# Consulter les stats de la DB
curl -s 'http://192.168.1.111:5001/stats?token=VOTRE_TOKEN' | python3 -m json.tool
# {
#     "total_paths": 91359,
#     "linked_to_torrent": 88201,
#     "unique_inodes": 14832,
#     "arr_managed_torrents": 1830
# }
```

---

## Limites connues

**Chemin non trouvé en DB** — Si Sonarr/Radarr supprime un fichier importé très récemment (entre deux syncs), le chemin peut ne pas être encore dans la DB locale. La prochaine sync résoudra ce cas. Force une resync manuelle si nécessaire.

**Torrents multi-saisons** — Certains torrents qBit regroupent plusieurs saisons sous un même nom de dossier. L'association inode peut être incomplète dans ces cas.

**Extensions vidéo** — Seuls les fichiers avec les extensions suivantes sont indexés : `.mkv`, `.mp4`, `.avi`, `.ts`, `.m2ts`, `.mov`, `.wmv`, `.flv`, `.iso`.
