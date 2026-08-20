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
      - JELLYFIN_URL=http://jellyfin:8096
      - JELLYFIN_API_KEY=VOTRE_CLE_API_JELLYFIN
      - JELLYFIN_PATH_MAP=/Multimedia:/data/Multimedia

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
| `VIDEO_EXTENSIONS` | voir ci-dessous | Extensions indexées, séparées par des virgules. Le point initial et la casse sont optionnels |
| `SONARR_URL` | — | URL Sonarr (requis pour `/bootstrap`) |
| `SONARR_API_KEY` | — | Clé API Sonarr (Settings → General) |
| `RADARR_URL` | — | URL Radarr (requis pour `/bootstrap`) |
| `RADARR_API_KEY` | — | Clé API Radarr (Settings → General) |
| `JELLYFIN_URL` | — | URL Jellyfin. Vide = suppressions Jellyfin désactivées |
| `JELLYFIN_API_KEY` | — | Clé API Jellyfin (Tableau de bord → Clés API) |
| `JELLYFIN_PATH_MAP` | `/Multimedia:/data/Multimedia` | Traduction des chemins vus par Jellyfin. Format `préfixe_jf:préfixe_local`, séparés par des virgules |
| `JELLYFIN_FALLBACK_MATCH` | `true` | Autorise la résolution par métadonnées quand l'item n'est pas encore indexé |
| `JELLYFIN_FALLBACK_MAX` | `10` | Nombre max de chemins que le fallback peut retenir pour un film/épisode |
| `JELLYFIN_FALLBACK_MAX_CONTAINER` | `500` | Idem pour une série/saison, qui compte légitimement beaucoup de fichiers |
| `JELLYFIN_DELEGATE_TO_ARR` | `false` | Traduit la suppression en action Sonarr/Radarr au lieu de déplacer les torrents directement |

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

## Configuration Jellyfin

Supprimer un média depuis Jellyfin déclenche la même mécanique que depuis Sonarr/Radarr : le hardlink de la bibliothèque disparaît, cleanup-linker retrouve l'inode, et déplace le torrent original **et** ses cross-seeds.

### Pourquoi une indexation est nécessaire

Contrairement aux webhooks Sonarr/Radarr, le payload `ItemDeleted` de Jellyfin **ne contient pas le chemin du fichier** — seulement un `ItemId` interne. Et l'event part *après* la suppression : l'API Jellyfin ne peut plus répondre, l'item n'existe plus.

La correspondance doit donc être établie **avant**. À chaque sync, cleanup-linker interroge Jellyfin (`GET /Items?Recursive=true&Fields=Path`, un seul appel paginé, aucun scan disque) et tague les lignes de `files` avec la colonne `jellyfin_item_id` :

```
files
inode  │ path                                   │ torrent_hash │ jellyfin_item_id │ jellyfin_series_id
───────┼────────────────────────────────────────┼──────────────┼──────────────────┼───────────────────
12345  │ /data/Media/Séries/…/S01E14.mkv        │ 222d582cec   │ a1b2c3d4…        │ e896855b…
12345  │ /data/Media/Séries/…/S01E14.mkv        │ bd4a11bf5b   │ a1b2c3d4…        │ e896855b…
12345  │ /data/Multimedia/Séries/…/S01E14.mkv   │ 222d582cec   │ NULL             │ NULL
12345  │ /data/Multimedia/cross-seeds/…/S01E14  │ 40056fb95f   │ NULL             │ NULL
```

Seul le chemin vu par Jellyfin est tagué — l'inode se charge du reste. À la suppression : `jellyfin_item_id` → inode → tous les hashes → cross-seeds → qBit.

### Plugin Webhook

Tableau de bord → Extensions → Webhook → **Add Generic Destination**

```
Webhook Name  : cleanup-linker
Webhook Url   : http://192.168.1.111:5001/jellyfin?token=VOTRE_TOKEN
Notification Type : ✅ Item Deleted
Item Type     : ✅ Movies   ✅ Episodes   ✅ Series   ✅ Seasons
Send All Properties : ✅ obligatoire
```

`Send All Properties` envoie le JSON brut ; sans cette case il faut fournir un template Handlebars et la route ne recevra pas `ItemId`.

Si Jellyfin n'est pas sur le réseau Docker `mediastack`, utilise l'IP de l'hôte (`http://192.168.1.111:5001`) plutôt que le nom de container.

### Les deux garde-fous

`ItemDeleted` ne se déclenche pas seulement sur une suppression volontaire : il part dès qu'un item quitte la base Jellyfin — refresh de métadonnées, réidentification, **retrait d'une bibliothèque**. Dans ces cas les fichiers sont toujours sur le disque.

1. **Le hardlink a-t-il vraiment disparu ?** Un `os.stat()` sur le chemin tagué. S'il répond, l'event est ignoré. C'est ce qui empêche un retrait de bibliothèque de balancer toute la collection vers `cleanuparr-unlinked`.
2. **Le torrent sert-il encore ?** Garde-fou existant, partagé avec Sonarr/Radarr : un pack de saison dont d'autres épisodes sont encore hardlinkés n'est pas déplacé.

### Séries et saisons

Supprimer une série entière depuis Jellyfin **n'émet qu'un seul event**, sur le conteneur — jamais un event par épisode (vérifié en production : une suppression de 14 épisodes n'a produit qu'un `ItemDeleted` de type `Series`).

Une `Series` ou une `Season` étant un dossier, elle n'a pas de ligne dans `files`. On remonte donc à ses fichiers par les colonnes `jellyfin_series_id` / `jellyfin_season_id`, renseignées sur chaque épisode pendant l'indexation (l'API Jellyfin renvoie nativement `SeriesId` et `SeasonId`).

Les chemins retenus sont ensuite ramenés à leur **dossier commun**, traité en une seule passe par la branche préfixe de `move_torrents_for_path` — une seule session qBittorrent au lieu d'une par épisode. Si ce dossier commun remonte trop haut (moins de 4 segments, ce qui engloberait une racine entière), il est rejeté et chaque chemin est traité séparément.

C'est aussi ici que le garde-fou compte le plus : retirer une bibliothèque de Jellyfin émet le même event `Series`, mais les fichiers restent sur le disque — aucun n'est retenu, aucun torrent n'est déplacé.

### Média non indexé

Un média ajouté *puis* supprimé entre deux syncs n'a jamais été tagué. Le fallback (`JELLYFIN_FALLBACK_MATCH`) restreint alors les candidats par métadonnées (`SeriesName` + saison, ou titre + année) et **ne retient que les chemins réellement absents du disque**. Au-delà de `JELLYFIN_FALLBACK_MAX` chemins il abandonne : un titre trop générique ne doit pas déclencher un déplacement de masse.

Pour combler l'écart sans attendre la sync :

```bash
curl -XPOST 'http://192.168.1.111:5001/jellyfin/index?token=VOTRE_TOKEN'
```

### Bibliothèques couvertes

Seules les bibliothèques dont les fichiers sont indexés par cleanup-linker sont résolvables : celles sous `/data/Media/` et sous les dossiers de catégories qBit. Une bibliothèque pointant ailleurs (musique, YouTube…) produira un `unresolved` dans les logs, sans effet de bord.

### Déléguer à Sonarr / Radarr

Supprimer depuis Jellyfin détruit le hardlink, mais **rien n'en informe Sonarr ou Radarr**. Ils continuent de croire qu'ils détiennent le fichier — et comme le média reste *monitored*, ils le **retéléchargent** au prochain rescan. Le torrent a été déplacé, l'espace n'est pas récupéré pour autant.

`JELLYFIN_DELEGATE_TO_ARR=true` change l'approche : au lieu d'agir sur qBittorrent, cleanup-linker traduit l'event en action *arr.

```
Jellyfin ItemDeleted
    ↓  résolution ItemId → chemins (identique)
    ↓  identification de la série/du film par préfixe de chemin
Sonarr/Radarr : dé-monitorer, puis supprimer l'enregistrement du fichier
    ↓  l'*arr émet son propre webhook
/webhook  →  move_torrents_for_path()   ← le chemin de code existant
qBit → cleanuparr-unlinked
```

L'identification se fait par **préfixe de chemin**, jamais par titre : le dossier d'une série Sonarr est toujours un ancêtre du chemin de ses épisodes, ce qui donne une correspondance exacte. Le préfixe le plus long gagne si des racines sont imbriquées.

L'ordre compte : on dé-monitore **avant** de supprimer, sinon l'*arr peut relancer une recherche entre les deux appels et retélécharger ce qu'on vient de retirer.

| Event Jellyfin | Action *arr |
|---|---|
| `Episode` | Dé-monitore l'épisode, supprime son `episodefile` |
| `Season` | Dé-monitore les épisodes de la saison, supprime leurs fichiers |
| `Series` | Idem + dé-monitore la série elle-même |
| `Movie` | Dé-monitore le film, supprime son `moviefile` |

L'entrée reste dans Sonarr/Radarr avec son historique et ses réglages — seul le monitoring bascule.

**Repli** : si le média n'est géré par aucun *arr (typiquement une bibliothèque Jellyfin pointant directement sur un dossier de téléchargement), on retombe sur le déplacement direct des torrents. Idem si l'*arr est injoignable — une erreur d'API n'interrompt jamais le traitement.

### Tester sans rien déplacer

```bash
# Ajoute &dry_run=true à l'URL du webhook, ou rejoue un payload à la main
curl -XPOST 'http://192.168.1.111:5001/jellyfin?token=VOTRE_TOKEN&dry_run=true' \
  -H 'Content-Type: application/json' \
  -d '{"NotificationType":"ItemDeleted","ItemType":"Episode","ItemId":"..."}'
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

### Jellyfin

Route dédiée `/jellyfin` — le payload Jellyfin n'a pas le même format que celui des *arrs.

| Event | ItemType | Action |
|---|---|---|
| `ItemDeleted` | `Movie`, `Episode`, `Video` | Résout `ItemId` → chemin → inode, puis move vers cleanuparr-unlinked |
| `ItemDeleted` | `Series`, `Season` | Résout via `jellyfin_series_id` / `jellyfin_season_id` → dossier commun → move en une passe |
| Autre | — | Ignoré |

Les moves sont tracés dans `cleanup_log` avec `source = "jellyfin"`, donc visibles dans `/history` et réversibles via `/restore`.

---

## Endpoints API

| Endpoint | Méthode | Auth | Description |
|---|---|---|---|
| `/webhook?token=...` | POST | Token | Reçoit les events Sonarr/Radarr |
| `/sync?token=...` | POST | Token | Force une resync manuelle immédiate |
| `/bootstrap?token=...` | POST | Token | Peuple arr_managed depuis l'historique *arr |
| `/cleanup?token=...` | POST | Token | Déplace les torrents *arr orphelins |
| `/restore?token=...` | POST | Token | Restaure les torrents après un cleanup raté |
| `/history?token=...` | GET | Token | Historique des moves (param `limit`, défaut 50) |
| `/stats?token=...` | GET | Token | Statistiques de la DB |
| `/jellyfin?token=...` | POST | Token | Reçoit les events Jellyfin (param `dry_run`) |
| `/jellyfin/index?token=...` | POST | Token | Force la réindexation Jellyfin |
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

## Historique des moves

`/cleanup` étend son résultat aux **cross-seeds partageant les mêmes inodes**. Sans ça il ne déplacerait que le hash importé par l'*arr : les cross-seeds ne sont jamais dans `arr_managed` (c'est cross-seed qui les ajoute, pas Sonarr/Radarr), ils resteraient derrière à hardlinker le même fichier, et aucun espace ne serait libéré.

L'extension part de deux sources : les orphelins trouvés à l'instant, **et** les torrents déjà dans `cleanuparr-unlinked` — leurs cross-seeds ont pu rester derrière lors d'un move précédent, ou être ajoutés par cross-seed après coup. `/cleanup` rattrape donc les moves incomplets du passé.

Elle est filtrée par les hashes ayant encore un fichier dans `/data/Media` : un pack de saison cross-seedé avec un épisode isolé partage un inode avec l'orphelin mais garde d'autres épisodes en bibliothèque — il n'est pas déplacé.

Chaque torrent déplacé vers `cleanuparr-unlinked` est enregistré dans la table `cleanup_log` de la DB SQLite (persistée dans `/config/db.sqlite`). Cette table survit aux resyncs et recreations du container.

```bash
# Voir les 50 derniers moves
curl -s 'http://192.168.1.111:5001/history?token=VOTRE_TOKEN' | python3 -m json.tool

# Voir les 200 derniers
curl -s 'http://192.168.1.111:5001/history?token=VOTRE_TOKEN&limit=200' | python3 -m json.tool
```

Chaque entrée contient : `torrent_hash`, `torrent_name`, `original_cat`, `source` (webhook ou cleanup), `moved_at`.

---

## En cas de cleanup raté

Si des torrents ont été déplacés à tort vers `cleanuparr-unlinked`, l'endpoint `/restore` permet de les remettre en place. Il utilise la table `cleanup_log` comme source principale — **il fonctionne donc même après une resync** :

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

**Extensions vidéo** — Par défaut : `.mkv`, `.mp4`, `.avi`, `.ts`, `.m2ts`, `.mov`, `.wmv`, `.flv`, `.iso`, `.m4v`.

Un fichier dont l'extension ne figure pas dans cette liste est **invisible pour tout le service** : son torrent ne sera déplacé ni par un webhook, ni par `/cleanup`, sans message d'erreur explicite — seulement un `Chemin non trouvé en DB` ou un `unresolved`. Si tu constates ça sur un média qui existe bien, vérifie son extension avant tout le reste.

La liste s'ajuste sans rebuild via `VIDEO_EXTENSIONS` :

```yaml
- VIDEO_EXTENSIONS=.mkv,.mp4,.avi,.ts,.m2ts,.mov,.wmv,.flv,.iso,.m4v,.webm
```
