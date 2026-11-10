# cleanup-linker

Service Python/Flask qui entretient une médiathèque bâtie sur qBittorrent, Sonarr, Radarr et Jellyfin. Il a deux missions :

1. **Mettre en corbeille les torrents qui ne servent plus.** Quand un fichier de la bibliothèque est supprimé ou remplacé, que ce soit depuis Sonarr, Radarr ou Jellyfin, son torrent et ses cross-seeds passent dans la catégorie qBittorrent `cleanuparr-unlinked`. Ils y continuent de seeder, jusqu'à ce qu'un outil comme cleanuparr libère la place.
2. **Débloquer les imports**, avec l'assistant d'import. Il règle les téléchargements que Sonarr et Radarr laissent en plan dans leur file :
   - déjà importé, mais resté coincé ;
   - torrent mal nommé ;
   - pack de saison incomplet ;
   - saison demandée sous un autre numéro ;
   - téléchargement mort ;
   - téléchargement devenu inutile.

Trois principes valent partout :
- **cleanup-linker ne supprime aucun fichier de lui-même.** Il change des catégories, renomme un torrent, retire une ligne d'une file *arr, lance une recherche ou un import. La seule suppression est celle demandée depuis Jellyfin, quand on choisit de la répercuter dans Sonarr ou Radarr.
- **Tout est tracé.** Chaque mise en corbeille est journalisée et peut être défaite (`/restore`). Chaque verdict de l'assistant l'est aussi (`/assist/history`).
- **Chaque règle de l'assistant démarre en simulation.** Elle journalise ce qu'elle ferait, jusqu'à ce qu'on l'active.

---

## Ce que fait le service

| Fonction | Déclencheur | Automatique |
|---|---|---|
| Index des fichiers par inode | Au démarrage, puis toutes les `SYNC_INTERVAL_HOURS` h. Chaque import est indexé tout de suite | Oui |
| Index Jellyfin | Après chaque sync, ou par `/jellyfin/index` | Oui |
| Sauvegarde de la base | Avant chaque sync | Oui |
| Corbeille après une suppression ou un upgrade dans Sonarr/Radarr | Webhook Sonarr/Radarr | Oui |
| Corbeille après une suppression dans Jellyfin, déléguée à Sonarr/Radarr si l'option est active | Webhook Jellyfin | Oui |
| Assistant d'import (règles 1, 2, 4, 5, 6, 7) | Event « Manual Interaction Required », « On Grab » pour la règle 4, passage de renfort toutes les 6 h | Oui |
| Balayage des orphelins (`/cleanup`) | À la main | Non |
| Restauration, bootstrap, sync forcée | À la main | Non |

---

## Installation

### Prérequis

| Dépendance | Rôle |
|---|---|
| **qBittorrent** | Client torrent, via l'API v2 |
| **Sonarr**, **Radarr** | Webhooks et API. Requis pour l'assistant d'import et `/bootstrap` |
| **cross-seed** | Sa base SQLite relie un torrent à ses cross-seeds |
| **Jellyfin** | Facultatif : suppressions depuis Jellyfin, par le plugin Webhook |
| **Prowlarr** | Facultatif : recherche des packs complets (règle 4) |
| **Seerr** | Facultatif : demandes à accorder avec les saisons Sonarr (règle 5) |
| **cleanuparr** | Facultatif : vide la catégorie `cleanuparr-unlinked`, ce que cleanup-linker ne fait jamais |
| **Docker** et le réseau `mediastack` | Exécution, et dialogue entre conteneurs |

### Image et fichiers

GitHub Actions construit l'image `ghcr.io/sos-pc/cleanup-linker:latest` à chaque push sur `main`. Elle porte le commit dans le label `org.opencontainers.image.revision`. Le conteneur exécute `app.py` sous gunicorn, avec 2 workers et un délai de 120 s par requête.

Le dossier monté sur `/config` contient :

```
config/cleanup-linker/
├── db.sqlite               ← base locale, créée au premier démarrage
├── db.sqlite.assist.lock   ← verrou : un seul passage de l'assistant à la fois
└── backups/                ← une copie de la base avant chaque sync
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
      - WEBHOOK_TOKEN=VOTRE_TOKEN_SECRET
      - TARGET_CATEGORY=cleanuparr-unlinked
      - SONARR_URL=http://sonarr:8989
      - SONARR_API_KEY=VOTRE_CLE_API_SONARR
      - RADARR_URL=http://radarr:7878
      - RADARR_API_KEY=VOTRE_CLE_API_RADARR
      - PROWLARR_URL=http://prowlarr:9696
      - PROWLARR_API_KEY=VOTRE_CLE_API_PROWLARR
      - SEERR_URL=http://seerr:5055
      - SEERR_API_KEY=VOTRE_CLE_API_SEERR
      - JELLYFIN_URL=http://jellyfin:8096
      - JELLYFIN_API_KEY=VOTRE_CLE_API_JELLYFIN
      - JELLYFIN_PATH_MAP=/Multimedia:/data/Multimedia
      - JELLYFIN_DELEGATE_TO_ARR=true
      # Assistant d'import : chaque règle reste en simulation tant qu'on ne la passe pas à false
      - IMPORT_ASSIST_DRY_RUN=false          # règle 1 · débloquer la file
      - IMPORT_ASSIST_NOM_DRY_RUN=false      # règle 2 · torrent mal nommé
      - IMPORT_ASSIST_PACK_DRY_RUN=false     # règle 4 · pack incomplet
      - IMPORT_ASSIST_ACCORD_DRY_RUN=false   # règle 5 · bonne saison
      - IMPORT_ASSIST_MORT_DRY_RUN=false     # règle 6 · téléchargement mort
      - IMPORT_ASSIST_INUTILE_DRY_RUN=false  # règle 7 · téléchargement inutile

networks:
  default:
    name: mediastack
    external: true
```

Les montages de `/data` sont en lecture seule : cleanup-linker lit le disque, et ne passe que par les API pour agir.

### Variables d'environnement

Les variables de simulation de l'assistant (`IMPORT_ASSIST_*_DRY_RUN`) ne font agir leur règle que sur `0`, `false` ou `no` : une valeur mal saisie laisse la simulation. Les options Jellyfin (`JELLYFIN_FALLBACK_MATCH`, `JELLYFIN_DELEGATE_TO_ARR`, `JELLYFIN_ARR_DELETE_CONTAINERS`) ne s'activent, elles, que sur `1`, `true` ou `yes`.

**Obligatoires** : le service ne démarre pas sans elles.

| Variable | Description |
|---|---|
| `QBIT_URL` | URL de l'interface Web qBittorrent |
| `QBIT_USER` | Identifiant qBittorrent |
| `QBIT_PASS` | Mot de passe qBittorrent |
| `WEBHOOK_TOKEN` | Token exigé par toutes les routes, sauf `/health` |

**Socle**

| Variable | Défaut | Description |
|---|---|---|
| `TARGET_CATEGORY` | `cleanuparr-unlinked` | Catégorie qBittorrent de la corbeille |
| `DB_PATH` | `/config/db.sqlite` | Base locale |
| `CROSSSEED_DB` | `/config/crossseed/cross-seed.db` | Base de cross-seed |
| `SYNC_INTERVAL_HOURS` | `12` | Intervalle entre deux syncs |
| `BACKUP_DIR` | `/config/backups` | Dossier des sauvegardes de la base |
| `BACKUP_RETENTION` | `5` | Nombre de sauvegardes gardées |
| `CLEANUP_GRACE_HOURS` | `24` | Délai avant qu'un torrent terminé, ou un fichier dont les liens ont changé, puisse être vu comme orphelin par `/cleanup`, ou comme inutile par le filet de la règle 7. `0` désactive ce délai |
| `CORBEILLE_GLOBALE` | `simulation` | Corbeille globale, voir plus bas. `off` : ancien geste seul. `simulation` : ancien geste, plus le journal de ce que ferait le nouveau, aux suppressions, aux upgrades et à `/cleanup`. `active` : le nouveau geste remplace l'ancien, `/cleanup` compris. Une valeur inconnue vaut `simulation` |
| `VIDEO_EXTENSIONS` | `.mkv,.mp4,.avi,.ts,.m2ts,.mov,.wmv,.flv,.iso,.m4v` | Extensions indexées, séparées par des virgules. Le point initial et la casse sont facultatifs. Un fichier d'une autre extension est invisible pour tout le service |

**Sonarr et Radarr**

| Variable | Défaut | Description |
|---|---|---|
| `SONARR_URL` | — | URL Sonarr, requise pour `/bootstrap` et l'assistant d'import |
| `SONARR_API_KEY` | — | Clé API Sonarr (Settings → General) |
| `RADARR_URL` | — | URL Radarr, requise pour `/bootstrap` et l'assistant d'import |
| `RADARR_API_KEY` | — | Clé API Radarr (Settings → General) |

**Jellyfin**

| Variable | Défaut | Description |
|---|---|---|
| `JELLYFIN_URL` | — | URL Jellyfin. Vide = pas d'index Jellyfin, et la route `/jellyfin` répond 503 |
| `JELLYFIN_API_KEY` | — | Clé API Jellyfin (Tableau de bord → Clés API) |
| `JELLYFIN_PATH_MAP` | `/Multimedia:/data/Multimedia` | Traduction des chemins vus par Jellyfin. Format `préfixe_jf:préfixe_local`, séparés par des virgules ; les plus longs passent d'abord |
| `JELLYFIN_FALLBACK_MATCH` | `true` | Résolution par métadonnées d'un item pas encore indexé |
| `JELLYFIN_FALLBACK_MAX` | `10` | Nombre maximal de chemins que cette résolution peut retenir pour un film ou un épisode |
| `JELLYFIN_FALLBACK_MAX_CONTAINER` | `500` | Idem pour une série ou une saison, qui compte légitimement beaucoup de fichiers |
| `JELLYFIN_DELEGATE_TO_ARR` | `false` | Traduit une suppression en action Sonarr/Radarr, au lieu de déplacer les torrents directement |
| `JELLYFIN_ARR_DELETE_CONTAINERS` | `false` | La suppression d'une série ou d'un film retire aussi l'entrée de Sonarr/Radarr, au lieu de la dé-monitorer |

**Assistant d'import**

| Variable | Défaut | Description |
|---|---|---|
| `IMPORT_ASSIST_INTERVAL_MINUTES` | `360` | Intervalle du passage de renfort, qui rattrape les events manqués. `0` le désactive, et `/assist` reste utilisable |
| `IMPORT_ASSIST_DRY_RUN` | `true` | Simulation de la règle 1 · débloquer la file |
| `IMPORT_ASSIST_NOM_DRY_RUN` | `true` | Simulation de la règle 2 · torrent mal nommé |
| `IMPORT_ASSIST_PACK_DRY_RUN` | `true` | Simulation de la règle 4 · pack incomplet |
| `IMPORT_ASSIST_PACK_MIN_SCORE` | `50` | Score de formats minimal d'un pack complet de remplacement, sans le bonus de pack. Il peut être plus bas que celui du pack incomplet : mieux vaut une saison sans trou. En dessous, un pack d'une autre langue ne sert qu'à combler les trous, fichier par fichier |
| `PROWLARR_URL` | — | URL Prowlarr, où la règle 4 cherche les packs complets. Vide = elle signale les packs partiels sans rien remplacer |
| `PROWLARR_API_KEY` | — | Clé API Prowlarr (Settings → General) |
| `IMPORT_ASSIST_ACCORD_DRY_RUN` | `true` | Simulation de la règle 5 · bonne saison |
| `SEERR_URL` | — | URL Seerr, d'où la règle 5 lit les demandes. Vide = règle 5 désactivée |
| `SEERR_API_KEY` | — | Clé API Seerr (Paramètres → Général) |
| `IMPORT_ASSIST_MORT_DRY_RUN` | `true` | Simulation de la règle 6 · téléchargement mort |
| `IMPORT_ASSIST_MORT_JOURS` | `3` | Jours sans rien recevoir, sans source connectée, au bout desquels un téléchargement est jugé mort. C'est aussi l'écart minimal entre deux recherches d'une autre version |
| `IMPORT_ASSIST_MORT_ESSAIS` | `5` | Nombre maximal d'autres versions essayées pour un même téléchargement mort |
| `IMPORT_ASSIST_INUTILE_DRY_RUN` | `true` | Simulation de la règle 7 · téléchargement inutile |

### Webhooks Sonarr et Radarr

Dans le réseau Docker, l'URL est `http://cleanup-linker:5000/webhook?token=VOTRE_TOKEN`. Depuis l'hôte, c'est le port publié, `5001`. Le token peut aussi passer dans l'en-tête `X-Token`.

**Sonarr** — Settings → Connect → + → Webhook

```
Name    : cleanup-linker
URL     : http://cleanup-linker:5000/webhook?token=VOTRE_TOKEN
Method  : POST
Triggers: ✅ On Grab              ← règle 4 : vérifie chaque pack de saison dès son grab
          ✅ On File Import       ← indispensable : indexe chaque import sans attendre la sync
          ✅ On File Upgrade      ← signale le fichier remplacé, une fois le nouveau en place
          ✅ On Series Delete
          ✅ On Episode File Delete
          ❌ On Episode File Delete For Upgrade   ← laisser décoché (voir Limites connues)
          ❌ On Import Complete   ← inutile : doublon de On File Import
          ✅ On Manual Interaction Required   ← déclenche l'assistant d'import
```

Dans Sonarr, « On File Upgrade » n'envoie rien si « On File Import » n'est pas coché, car c'est un filtre de cet event. Et « On Import Complete » ne dit pas quels fichiers un upgrade a remplacés.

**Radarr** — Settings → Connect → + → Webhook

```
Name    : cleanup-linker
URL     : http://cleanup-linker:5000/webhook?token=VOTRE_TOKEN
Method  : POST
Triggers: ✅ On Grab              ← facultatif : marque le torrent comme pris par Radarr dès son grab
          ✅ On File Import       ← indispensable : indexe chaque import sans attendre la sync
          ✅ On File Upgrade      ← signale le fichier remplacé, une fois le nouveau en place
          ✅ On Movie Delete
          ✅ On Movie File Delete
          ❌ On Movie File Delete For Upgrade   ← laisser décoché (voir Limites connues)
          ✅ On Manual Interaction Required   ← déclenche l'assistant d'import
```

Chaque event qui porte un `downloadId` marque le torrent comme pris par un *arr (table `arr_managed`). C'est ce qui le rend visible pour `/cleanup`.

### Webhook Jellyfin

Tableau de bord → Extensions → Webhook → **Add Generic Destination**

```
Webhook Name  : cleanup-linker
Webhook Url   : http://192.168.1.111:5001/jellyfin?token=VOTRE_TOKEN
Notification Type : ✅ Item Deleted
Item Type     : ✅ Movies   ✅ Episodes   ✅ Series   ✅ Seasons
Send All Properties : ✅ obligatoire
```

`Send All Properties` envoie le JSON brut. Sans cette case, il faut fournir un template Handlebars, et la route ne recevra pas `ItemId`.

Si Jellyfin n'est pas sur le réseau Docker `mediastack`, utilise l'IP de l'hôte (`http://192.168.1.111:5001`) plutôt que le nom du conteneur.

### Premier démarrage

`/cleanup` repose sur la table `arr_managed` pour ne jamais toucher les torrents ajoutés à la main. En régime établi, elle s'entretient seule : chaque webhook enregistre son `downloadId`, et chaque sync marque les torrents hardlinkés dans `/data/Media/`. `/bootstrap` ne sert donc qu'une fois, après une première installation ou une longue absence du service. Il récupère dans l'historique de Sonarr et de Radarr les imports antérieurs, que la base locale ne peut pas reconstituer.

1. **Bootstrap**, en simulation d'abord, puis pour de bon :

   ```bash
   curl -XPOST 'http://192.168.1.111:5001/bootstrap?token=VOTRE_TOKEN&dry_run=true'
   curl -XPOST 'http://192.168.1.111:5001/bootstrap?token=VOTRE_TOKEN'
   ```

   Les logs donnent le compte :

   ```
   Sonarr: 1243 entrées marquées
   Radarr: 587 entrées marquées
   === Bootstrap terminé : 1830 hashes au total ===
   ```

2. **Cleanup en simulation** : les candidats apparaissent dans les logs avec `[DRY RUN]`. Vérifie que seuls des torrents gérés par un *arr y figurent, et pas tes ajouts manuels.

   ```bash
   curl -XPOST 'http://192.168.1.111:5001/cleanup?token=VOTRE_TOKEN&dry_run=true'
   ```

3. **Cleanup réel**, si la simulation est juste :

   ```bash
   curl -XPOST 'http://192.168.1.111:5001/cleanup?token=VOTRE_TOKEN'
   ```

---

## La corbeille des torrents qui ne servent plus

```
Sonarr / Radarr / Jellyfin
     │  suppression ou upgrade d'un fichier de la bibliothèque
     ▼
cleanup-linker
     │  1. chemin → inode, par la base locale
     │  2. inode → torrent d'origine, puis ses cross-seeds
     │  3. garde-fous : fichier toujours là, torrent qui sert encore, dossier partagé
     ▼
qBittorrent : setCategory → « cleanuparr-unlinked »
     │  le torrent continue de seeder
     ▼
cleanuparr (facultatif) : supprime quand la place manque
```

### L'index

**La base locale (`db.sqlite`)**, maintenue par cleanup-linker, relie chaque fichier vidéo du disque à son inode et au hash de son torrent. Elle couvre trois zones :
- `/data/Media/`, les hardlinks créés par Sonarr et Radarr ;
- `/data/Multimedia/<catégories qBit>/`, les fichiers sources des torrents ;
- `/data/Multimedia/cross-seeds/`, les hardlinks créés par cross-seed.

```
inode  │ path                                         │ torrent_hash
───────┼──────────────────────────────────────────────┼──────────────
295671 │ /data/Media/Films/MonFilm/MonFilm.mkv        │ abc123...
295671 │ /data/Multimedia/Films/MonFilm.mkv           │ abc123...
295671 │ /data/Multimedia/cross-seeds/C411/MonFilm.mkv│ def456...
```

La table `arr_managed` liste les torrents pris par un *arr. Sont marqués :
- les `downloadId` des webhooks : grab, import, upgrade… ;
- les torrents hardlinkés dans `/data/Media/`, à chaque sync ;
- l'historique des *arr, par `/bootstrap`.

Un torrent qui n'est plus dans qBittorrent en sort. C'est elle qui distingue un torrent géré par les *arr d'un ajout manuel. Elle ne dit pas tout : sans « On Grab », cas de Radarr dans la configuration conseillée, un torrent pris mais jamais importé n'y figure pas. La file de l'*arr, que lit l'assistant, reste la référence.

**La base de cross-seed (`cross-seed.db`)**, maintenue par cross-seed, liste dans `client_searchee` tous les torrents de qBittorrent, avec leur nom, leur hash et leur catégorie. Elle permet de retrouver les torrents qui portent le même nom, donc le même contenu, que le torrent d'origine.

**La sync** reconstruit entièrement la base locale au démarrage, puis toutes les 12 h (`SYNC_INTERVAL_HOURS`). Elle commence par sauvegarder la base (`BACKUP_DIR`, les `BACKUP_RETENTION` dernières), puis :
1. récupère les catégories qBittorrent et leurs dossiers via l'API ;
2. scanne chaque dossier de catégorie pour indexer les vidéos ;
3. scanne `/data/Multimedia/cross-seeds/` pour les hardlinks de cross-seed ;
4. scanne `/data/Media/` pour les hardlinks de Sonarr et Radarr ;
5. croise les inodes avec la liste des torrents qBittorrent ;
6. marque `arr_managed` tout torrent hardlinké dans `/data/Media/`, et en retire ceux qui ne sont plus dans qBittorrent ;
7. indexe Jellyfin, si configuré.

L'étape 6 est un filet contre les webhooks ratés : service arrêté pendant un import, event non coché, livraison échouée. Un hardlink dans une racine *arr prouve l'import, indépendamment de tout event reçu. Le critère ne porte que sur `/data/Media/` : un torrent présent seulement sous `/data/Multimedia/`, un ajout manuel, n'est jamais marqué, donc jamais candidat au `/cleanup`.

La purge se fonde sur la **liste des torrents qBittorrent**, pas sur la table `files`. Un torrent présent dans le client mais dont les fichiers ont disparu du disque n'a aucune ligne dans `files`, et serait retiré à tort. Sans cette purge, une release re-téléchargée plus tard reprendrait le même hash, et serait d'emblée considérée comme gérée par un *arr, même ajoutée à la main.

**L'indexation à l'import.** L'event `Download` (« On File Import ») fait indexer le fichier importé tout de suite. Il est rattaché au torrent dont il est un hardlink, à partir de son inode et du `downloadId`. Une suppression qui suit avant la prochaine sync le retrouve donc.

### Suppression ou upgrade dans Sonarr / Radarr

```
1. Radarr/Sonarr supprime ou remplace un fichier
        │
        ▼
2. Webhook → cleanup-linker reçoit le chemin du fichier
   ex. : /data/Media/Films/MonFilm (2024)/MonFilm.mkv
        │
        ▼
3. Base locale : ce chemin → son inode
   S'il n'est pas trouvé : recherche partielle sur le dossier parent
        │
        ▼
4. Base locale : inode → hash(es) du ou des torrents d'origine
        │
        ▼
5. Base cross-seed : pour chaque hash d'origine, les torrents de même nom
   dans client_searchee → ses cross-seeds
        │
        ▼
6. qBittorrent : écarte tout torrent dont l'emplacement sert aussi
   à un torrent hors du lot
        │
        ▼
7. qBittorrent : setCategory(tous les hashes, « cleanuparr-unlinked »)
        │
        ▼
8. Base locale : DELETE WHERE inode = ?
```

Un torrent dont d'autres fichiers ont encore un hardlink dans la bibliothèque n'est pas déplacé : un pack de saison dont on supprime un seul épisode reste en place. Une suppression de série ou de film entier (`SeriesDelete`, `MovieDelete`) déplace tous les torrents du dossier, sans cette vérification.

**Un fichier toujours sur le disque n'a pas été supprimé.** Un `EpisodeFileDelete` ou un `MovieFileDelete` dont le fichier existe encore est ignoré. C'est le cas d'une réaffectation à un autre épisode, comme celles de la règle 5.

**Les torrents jumeaux** (étape 6). Une même release prise sur deux trackers atterrit au même endroit : le second torrent réutilise les fichiers du premier. Comme la corbeille a son propre dossier, y passer l'un fait déplacer ces fichiers par ATM, et l'autre reste en seed sur du vide. C'est arrivé sur un upgrade Radarr vers la même release : la suppression de l'ancien fichier a envoyé l'ancien torrent en corbeille, avec le fichier que le nouveau allait importer. Le jumeau peut être plus récent que la dernière sync. Le contrôle lit donc la liste qBittorrent au moment du move, et compare les `content_path`. Un torrent écarté reste en place, et le log l'indique (`⏭ … fichiers partagés avec …`). `/cleanup`, `/restore` et l'assistant d'import appliquent le même contrôle.

**Les upgrades.** « On File Upgrade » envoie un event `Download` avec `isUpgrade`. Le nouveau fichier est d'abord indexé, puis chaque fichier remplacé (`deletedFiles`) est traité comme une suppression. Le torrent de l'ancien fichier part en corbeille dès l'import du nouveau.

### Suppression depuis Jellyfin

Supprimer un média depuis Jellyfin déclenche la même mécanique : le hardlink de la bibliothèque disparaît, cleanup-linker retrouve l'inode, puis déplace le torrent d'origine **et** ses cross-seeds.

#### Pourquoi une indexation est nécessaire

Contrairement aux webhooks Sonarr/Radarr, le payload `ItemDeleted` de Jellyfin **ne contient pas le chemin du fichier**, seulement un `ItemId` interne. Et l'event part *après* la suppression : l'API Jellyfin ne peut plus répondre, l'item n'existe plus.

La correspondance doit donc être établie **avant**. À chaque sync, cleanup-linker interroge Jellyfin (`GET /Items?Recursive=true&Fields=Path`, un seul appel paginé, aucun scan disque). Il tague les lignes de `files` avec la colonne `jellyfin_item_id` :

```
files
inode  │ path                                   │ torrent_hash │ jellyfin_item_id │ jellyfin_series_id
───────┼────────────────────────────────────────┼──────────────┼──────────────────┼───────────────────
12345  │ /data/Media/Séries/…/S01E14.mkv        │ 222d582cec   │ a1b2c3d4…        │ e896855b…
12345  │ /data/Media/Séries/…/S01E14.mkv        │ bd4a11bf5b   │ a1b2c3d4…        │ e896855b…
12345  │ /data/Multimedia/Séries/…/S01E14.mkv   │ 222d582cec   │ NULL             │ NULL
12345  │ /data/Multimedia/cross-seeds/…/S01E14  │ 40056fb95f   │ NULL             │ NULL
```

Seul le chemin vu par Jellyfin est tagué : l'inode se charge du reste. À la suppression, la chaîne est la suivante : `jellyfin_item_id` → inode → tous les hashes → cross-seeds → qBittorrent.

La requête passe `CollapseBoxSetItems=false`. Sans ce paramètre, Jellyfin remplace les films rangés dans une collection par la collection elle-même, et ces films échappent à l'index. Sur l'installation de référence, il en manquait 189 sur 1654, et les supprimer depuis Jellyfin restait sans effet.

#### Les trois garde-fous

`ItemDeleted` ne se déclenche pas seulement sur une suppression volontaire. Il part dès qu'un item quitte la base Jellyfin : refresh de métadonnées, réidentification, **retrait d'une bibliothèque**, **changement de bibliothèque**. Dans ces cas, le fichier existe toujours, parfois sous un autre nom.

1. **Le hardlink a-t-il vraiment disparu ?** Un `os.stat()` sur le chemin tagué : s'il répond, l'event est ignoré. C'est ce qui empêche un retrait de bibliothèque d'envoyer toute la collection dans `cleanuparr-unlinked`.
2. **Le fichier a-t-il seulement changé de nom ?**
   - Déplacer un titre vers une autre bibliothèque fait disparaître l'ancien chemin, exactement comme une suppression. C'est vrai qu'on ait changé son dossier racine dans Sonarr/Radarr ou sa catégorie dans qBittorrent. L'index ne connaîtra le nouveau chemin qu'à la sync suivante.
   - C'est le compteur de liens de l'inode qui tranche : une suppression le fait baisser, un déplacement le laisse intact. Si `st_nlink` dépasse le nombre de liens connus encore présents, le fichier vit ailleurs, et le chemin est ignoré.
   - Quand plus aucun lien connu ne subsiste, parce que le torrent et le hardlink ont été déplacés tous les deux, l'inode est cherché à l'emplacement actuel des torrents. Si qBittorrent ne répond pas, rien n'est fait.
   - Sans ce garde-fou, avec la délégation activée, une série changée de bibliothèque était supprimée de Sonarr.
3. **Le torrent sert-il encore ?** C'est le garde-fou partagé avec Sonarr/Radarr : un pack de saison dont d'autres épisodes sont encore hardlinkés n'est pas déplacé.

#### Séries et saisons

Supprimer une série entière depuis Jellyfin **n'émet qu'un seul event**, sur le conteneur, et jamais un event par épisode. C'est vérifié en production : une suppression de 14 épisodes n'a produit qu'un `ItemDeleted` de type `Series`.

Une `Series` ou une `Season` étant un dossier, elle n'a pas de ligne dans `files`. On remonte donc à ses fichiers par les colonnes `jellyfin_series_id` et `jellyfin_season_id`, renseignées sur chaque épisode pendant l'indexation : l'API Jellyfin renvoie nativement `SeriesId` et `SeasonId`.

Les chemins retenus sont ensuite ramenés à leur **dossier commun**, traité en une seule passe. Il ne faut donc qu'une seule session qBittorrent, au lieu d'une par épisode. Si ce dossier commun remonte trop haut, à moins de 5 segments, c'est une bibliothèque entière comme `/data/Media/Séries` : il est rejeté, et chaque chemin est traité séparément. C'est le cas quand Jellyfin réunit deux dossiers d'une même bibliothèque sous une seule série : la passe sur le dossier commun ratisserait alors toute la bibliothèque.

C'est aussi ici que le premier garde-fou compte le plus. Retirer une bibliothèque de Jellyfin émet le même event `Series`, mais les fichiers restent sur le disque : aucun n'est retenu, aucun torrent n'est déplacé.

#### Média non indexé

Un média ajouté *puis* supprimé entre deux syncs n'a jamais été tagué. Le repli (`JELLYFIN_FALLBACK_MATCH`) restreint alors les candidats par métadonnées : `SeriesName` et saison, ou titre et année. Il **ne retient que les chemins réellement absents du disque**. Au-delà de `JELLYFIN_FALLBACK_MAX` chemins (`JELLYFIN_FALLBACK_MAX_CONTAINER` pour une série ou une saison), il abandonne : un titre trop générique ne doit pas déclencher un déplacement de masse.

Pour combler l'écart sans attendre la sync :

```bash
curl -XPOST 'http://192.168.1.111:5001/jellyfin/index?token=VOTRE_TOKEN'
```

Seules les bibliothèques dont les fichiers sont indexés par cleanup-linker sont résolvables : celles sous `/data/Media/` et sous les dossiers de catégories qBittorrent. Une bibliothèque qui pointe ailleurs (musique, YouTube…) produit un `unresolved` dans les logs, sans effet de bord.

#### Déléguer à Sonarr / Radarr

Supprimer depuis Jellyfin détruit le hardlink, mais **rien n'en informe Sonarr ou Radarr**. Ils continuent de croire qu'ils détiennent le fichier. Comme le média reste *monitored*, ils le **retéléchargent** au prochain rescan. Le torrent a été déplacé, mais l'espace n'est pas récupéré pour autant.

`JELLYFIN_DELEGATE_TO_ARR=true` change l'approche : au lieu d'agir sur qBittorrent, cleanup-linker traduit l'event en action *arr.

```
Jellyfin ItemDeleted
    ↓  résolution ItemId → chemins (identique)
    ↓  identification de la série ou du film par préfixe de chemin
Sonarr/Radarr : dé-monitorer, puis supprimer l'enregistrement du fichier
    ↓  l'*arr émet son propre webhook
/webhook  →  même circuit qu'une suppression dans Sonarr/Radarr
qBittorrent → cleanuparr-unlinked
```

L'identification se fait par **préfixe de chemin**, jamais par titre. Le dossier d'une série Sonarr est toujours un ancêtre du chemin de ses épisodes, ce qui donne une correspondance exacte. Si des racines sont imbriquées, le préfixe le plus long gagne.

**Bibliothèque posée sur le dossier de téléchargement.** Quand une bibliothèque Jellyfin pointe directement sur un dossier de catégorie qBittorrent, `Films` sur `/data/Multimedia/Films` par exemple, Jellyfin et Radarr désignent le même fichier par deux chemins différents :

```
Jellyfin : /data/Multimedia/Films/Cypher.2002.Multi.Bluray.Remux....mkv
Radarr   : /data/Media/Films/Cypher (2002) [tmdbid-10133]/Cypher (2002) ... .mkv
           └─ même inode : le hardlink créé par Radarr à l'import
```

Comparer le seul chemin Jellyfin aux dossiers Radarr ne peut pas aboutir. Avant le correctif, la suppression retombait sur le déplacement direct des torrents, et Radarr gardait sa copie, devenue invisible mais toujours `hasFile=true`. Sur l'installation de référence, 4 films sont restés ainsi, soit 90 Go, et aucune suppression de film depuis Jellyfin n'avait jamais été déléguée.

Les chemins supprimés sont donc d'abord complétés par **tous les autres chemins du même inode** connus de la base. Ils y sont encore au moment de l'event, car la sync ne purge un chemin disparu qu'au passage suivant. L'identification par préfixe trouve alors le film par son chemin `/data/Media`.

Un lien n'est retenu que si **son inode sur le disque est toujours celui de la base**. Un fichier remplacé depuis la dernière sync, par un upgrade par exemple, est un autre fichier : une suppression Jellyfin ne doit pas l'emporter. Et quand Radarr détient une autre release que celle supprimée, aucun chemin `/data/Media` ne partage l'inode du fichier supprimé : rien n'est délégué, et la release de Radarr reste en place.

L'ordre compte : on dé-monitore **avant** de supprimer, sinon l'*arr peut relancer une recherche entre les deux appels et retélécharger ce qu'on vient de retirer. Le dé-monitorage passe par les endpoints à charge minimale, `episode/monitor`, `series/editor` et `movie/editor`. Jamais par un `PUT` sur la ressource, qui exigerait de renvoyer l'objet complet, et risquerait d'abîmer une fiche sur un champ mal repris.

| Event Jellyfin | Action *arr |
|---|---|
| `Episode` | Dé-monitore l'épisode, supprime son `episodefile` |
| `Season` | Dé-monitore les épisodes de la saison, supprime leurs fichiers |
| `Series` | Idem, et dé-monitore la série elle-même |
| `Movie` | Dé-monitore le film, supprime son `moviefile` |

L'entrée reste dans Sonarr/Radarr avec son historique et ses réglages : seul le monitoring bascule.

**Supprimer l'entrée plutôt que la dé-monitorer.** Une série vidée mais conservée reste indéfiniment dans Sonarr, à 0 fichier, en affichant ses épisodes manquants. `JELLYFIN_ARR_DELETE_CONTAINERS=true` retire l'entrée, pour les **conteneurs uniquement** :

| Event Jellyfin | Action |
|---|---|
| `Series` | `DELETE /series/{id}?deleteFiles=true` |
| `Movie` | `DELETE /movie/{id}?deleteFiles=true` |
| `Season`, `Episode` | Inchangé : supprimer une saison ne doit pas retirer la série |

Effet secondaire appréciable : Sonarr n'émet plus qu'un seul `SeriesDelete`, résolu par préfixe de dossier, au lieu d'un `EpisodeFileDelete` par épisode qui déplaçait le même torrent autant de fois.

C'est désactivé par défaut, parce que c'est irréversible côté *arr : on perd l'historique, le profil de qualité, les tags et le monitoring. **À vérifier avant d'activer** : avec des listes d'import (Trakt, IMDb…), la série sera ré-ajoutée au prochain sync, sauf à gérer les exclusions.

**Repli** : si le média n'est géré par aucun *arr, typiquement une bibliothèque Jellyfin posée sur un dossier de téléchargement, on retombe sur le déplacement direct des torrents. Idem si l'*arr est injoignable : une erreur d'API n'interrompt jamais le traitement.

#### Tester sans rien déplacer

```bash
# Ajoute &dry_run=true à l'URL du webhook, ou rejoue un payload à la main
curl -XPOST 'http://192.168.1.111:5001/jellyfin?token=VOTRE_TOKEN&dry_run=true' \
  -H 'Content-Type: application/json' \
  -d '{"NotificationType":"ItemDeleted","ItemType":"Episode","ItemId":"..."}'
```

### `/cleanup` : les orphelins

Un torrent peut rester hors de la corbeille alors que plus rien ne s'en sert : webhook raté, suppression avant indexation, ou téléchargement jamais importé. `/cleanup` le rattrape. Il **n'est lancé par aucune tâche** : on l'appelle à la main, de préférence en simulation d'abord (`&dry_run=true`).

Il commence par refuser de tourner si la dernière sync réussie date de plus de deux intervalles, ou si elle a perdu plus de la moitié des lignes. Il relance une sync si la dernière a plus de 2 minutes. Il lit ensuite les files de Sonarr et de Radarr, éléments inconnus compris : si l'une ne répond pas, il s'arrête sans rien déplacer. Puis :
1. il retient les torrents `arr_managed` qui n'ont aucun fichier dans `/data/Media/` et ne sont pas déjà dans la corbeille ;
2. il y ajoute leurs **cross-seeds**, qui partagent leurs inodes. Il fait de même pour les cross-seeds des torrents déjà en corbeille, qu'un move précédent a pu laisser derrière. Un voisin qui garde d'autres fichiers dans `/data/Media` reste en place. Sans cette étape, les cross-seeds ne partiraient jamais : ils ne sont pas dans `arr_managed`, puisque c'est cross-seed qui les ajoute ;
3. il épargne les torrents terminés depuis moins de `CLEANUP_GRACE_HOURS` (24 h), dont l'import est probablement en attente ;
4. il épargne aussi les téléchargements encore dans la file d'un *arr, même bloqués depuis des jours : on attend qu'ils soient importés ou retirés ;
5. il écarte les dossiers partagés avec un torrent hors du lot, comme le webhook ;
6. il passe le reste en corbeille.

Avec la corbeille globale, `/cleanup` juge aussi avec sa règle, au repos : voir [La corbeille globale](#la-corbeille-globale). En simulation, il trace ce qu'elle ferait avant l'ancien balayage, qui agit seul ; active, elle le remplace.

### Historique et restauration

Chaque torrent passé en corbeille est enregistré dans la table `cleanup_log`, avec son hash, son nom, sa catégorie d'origine, sa date et sa source :

| Source | Origine du move |
|---|---|
| `webhook` | Suppression ou upgrade dans Sonarr/Radarr |
| `jellyfin` | Suppression dans Jellyfin, sans délégation |
| `cleanup` | Balayage `/cleanup` |
| `assist` | Assistant d'import : règles 4, 6 et 7 |

Cette table survit aux syncs et à la recréation du conteneur.

```bash
# Les 50 derniers moves (limit : 500 au plus)
curl -s 'http://192.168.1.111:5001/history?token=VOTRE_TOKEN&limit=50' | python3 -m json.tool
```

`/restore` remet les torrents dans leur catégorie d'origine, en s'appuyant sur `cleanup_log`. Il fonctionne donc même après une sync. Par défaut, il ne touche qu'aux torrents du **dernier `/cleanup`** :

```bash
curl -XPOST 'http://192.168.1.111:5001/restore?token=VOTRE_TOKEN'
```

Pour défaire aussi les moves des webhooks, de Jellyfin ou de l'assistant, `since` (timestamp Unix) restaure tout ce qui a été tracé depuis cette date :

```bash
curl -XPOST "http://192.168.1.111:5001/restore?token=VOTRE_TOKEN&since=$(date -d '2 hours ago' +%s)"
```

Sans ce bornage, tout ce qui est dans la corbeille et figure au journal repartirait : des mois de suppressions légitimes, plus les torrents qu'on y laisse seeder pour honorer les trackers.

### Après la corbeille

cleanup-linker ne vide jamais `cleanuparr-unlinked`. C'est le rôle de cleanuparr, qui supprime selon sa propre logique, par exemple quand la place manque. Sans lui, la corbeille ne fait que grossir : ses torrents seedent, et leurs fichiers occupent toujours le disque.

### La corbeille globale

**Le problème.** Le geste habituel ne passe que par les torrents. Or un fichier de la bibliothèque peut avoir gardé, côté téléchargement, un nom que plus aucun torrent ne liste : le torrent a été retiré de qBittorrent, le fichier est resté. Personne ne retire ce nom, ni l'*arr qui ne connaît que celui de `/data/Media`, ni la corbeille. Supprimer le titre ne libère alors rien. Sur l'installation de référence, 2 580 noms sous fiche sont dans ce cas, soit 3,8 To de données.

**Le geste.** À une vraie suppression ou à un upgrade, chaque fichier touché est jugé d'après ses autres noms :

| Ses autres noms | Décision |
|---|---|
| Un nom ailleurs dans `/data/Media` : une autre fiche ou bibliothèque | rien ne part pour ce fichier |
| Un lien que l'index ne connaît pas : collection, dossier non scanné | rien ne part pour ce fichier |
| Sinon, un nom tenu par un torrent | le torrent part, si aucune de ses autres vidéos ne sert encore |
| Sinon, un nom qu'aucun torrent ne tient | il entre dans un torrent de corbeille |

Une autre vidéo d'un torrent sert encore si elle a un nom dans `/data/Media`, un lien inconnu, ou un nom lu par Jellyfin. Un petit fichier lu seulement dans un dossier de téléchargement, de moins d'un quart du plus gros du torrent, est un bonus de la même release : il ne retient pas le torrent. Un torrent sans catégorie n'est jamais touché.

« Tenu par un torrent » se lit dans la liste des fichiers du torrent, jamais dans son dossier : un dossier peut contenir des fichiers qui n'en font pas partie. Les cross-seeds suivent parce qu'ils partagent les mêmes fichiers. Ils ne sont plus cherchés par leur nom, car plusieurs torrents s'appellent `Season 2` sans rien partager.

**Le torrent de corbeille.** Les noms sans torrent d'un même dossier de release entrent dans un torrent ajouté à qBittorrent :
- il s'appelle `[sans torrent] <dossier>`, porte le tag `sans-torrent`, et va dans la catégorie de la corbeille. Si le dossier garde d'autres vidéos, son nom le dit : `[sans torrent] Scissor Seven (24 vidéo(s) sur 58)`. Les 34 autres restent ;
- il liste exactement ces fichiers, plus les petits fichiers du dossier (.nfo, .jpg, .srt) quand le dossier ne garde aucune autre vidéo. Une vidéo qui part avec son torrent, ou qui est déjà en corbeille, ne retient pas le dossier ;
- il y en a un par dossier. Un upgrade arrive épisode par épisode : à chaque fois, le nouveau torrent de corbeille reprend le contenu du précédent, qui est retiré sans ses fichiers. Au dernier épisode, le dossier part entier. Un fichier qu'un autre torrent liste n'est jamais repris ;
- ses empreintes de pièces sont nulles : il est ajouté sans vérification, donc sans relire les données, même pour un pack de 300 Go. Il est privé, sans tracker, et jamais démarré : il ne parle à personne. Une revérification forcée l'afficherait à 0 %, sans toucher aux fichiers ;
- s'il est dans un dossier lu par Jellyfin, il est déplacé dans le dossier de la corbeille, comme ATM le fait pour un torrent : le titre disparaît de la bibliothèque. Ce n'est pas le cas si un autre torrent occupe la même place.

Il se vide comme les autres, depuis qBittorrent, torrent et fichiers. Le retirer sans ses fichiers annule le geste, et c'est ce que fait `/restore`. Le service ne supprime toujours aucun fichier.

**Quand.**

| Geste | Décision |
|---|---|
| Suppression d'une série ou d'un film, case des fichiers cochée | geste complet |
| Même suppression, case décochée (`deletedFiles` à `false`) | rien : les fichiers restent, leurs torrents aussi |
| Suppression d'un épisode ou d'un fichier, fichier disparu signalé par l'*arr | geste sur ce fichier |
| Upgrade | geste sur l'ancien fichier |
| Suppression depuis Jellyfin d'un titre sans fiche | geste sur les chemins disparus |
| Fichier toujours sur le disque (réaffectation) | rien, comme avant |
| `/cleanup` | passage au repos, ci-dessous |

**Au repos, avec `/cleanup`.** Sans geste, le passage cherche ce qu'un geste passé a laissé, par exemple un upgrade fait avant la corbeille globale. Un fichier ne sert plus s'il n'a :
- aucun nom dans `/data/Media` ;
- aucun lien que l'index ne connaît pas, comme une collection ;
- aucun nom lu par Jellyfin ;
- et s'il n'a pas bougé depuis `CLEANUP_GRACE_HOURS`.

Alors :
- un torrent part si aucune de ses vidéos ne le retient, comme au geste. Seuls sont regardés ceux qu'un *arr a pris (`arr_managed`) et les cross-seeds, jamais un ajout à la main ;
- un torrent attend s'il est encore dans la file d'un *arr, s'il a fini depuis moins de 24 h, ou si son téléchargement n'est pas fini (cross-seeds exceptés) ;
- les noms sans torrent d'un fichier qui ne sert plus entrent dans un torrent de corbeille ;
- sauf si un torrent qui reste tient encore ce fichier : rien ne se libérerait. Ces noms attendent que ce torrent parte, comme les OAV d'un pack dont les épisodes servent.

Si Jellyfin ne répond pas, le passage ne jette rien : on ne saurait pas ce qu'il montre. Les moves sont tracés avec la source `cleanup`, et `/restore` les défait comme les autres.

```
INFO   [SIMULATION] 🗑 Corbeille globale (cleanup) : 59 fichier(s) ne servent plus, 1 torrent(s) en corbeille, 4 torrent(s) de corbeille pour 49 nom(s) sans torrent, 4 en attente ; 215.8 Go libérés au vidage, sur 215.8 Go
INFO       → torrent en corbeille : [cross-seed-link] Dark Season 01
INFO       → torrent de corbeille : [sans torrent] Scissor Seven (24 vidéo(s) sur 58), 24 vidéo(s)
INFO       → attend : Ghosts.S06E01.NORDiC.1080p.VIAP.WEB-DL.DDP5.1.H.264-ADDICTION.mkv : encore dans la file d'un *arr
INFO       → attend : 24 nom(s) sans torrent : un torrent qui sert encore tient leurs fichiers : rien ne se libérerait
INFO   [SIMULATION] Corbeille globale (cleanup) : l'ancien /cleanup aurait déplacé 0 torrent(s)
```

**Simulation d'abord.** Par défaut, `CORBEILLE_GLOBALE=simulation` : l'ancien geste agit comme avant, et le nouveau journalise ce qu'il ferait, avant lui, puisque l'ancien geste retire de l'index les fichiers qu'il traite. Chaque décision est tracée dans la table `corbeille_log`, avec le nombre de torrents que l'ancien geste a déplacés, et `/corbeille/history` la relit.

```
INFO   [SIMULATION] 🗑 Corbeille globale (Sonarr SeriesDelete) : 7 fichier(s) touché(s), 6 torrent(s) en corbeille, 1 torrent(s) de corbeille pour 3 nom(s) sans torrent ; 4.4 Go libérés au vidage, sur 4.4 Go
INFO       → torrent en corbeille : [cross-seed-link] Le College Noir Complainte D'auvergne.S01.TRUEFRENCH.1080p.WEB.x264-FTMVHD
INFO       → torrent de corbeille : [sans torrent] Le College Noir Complainte D'auvergne.S01.TRUEFRENCH.1080p.WEB.x264-FTMVHD, 3 vidéo(s)
INFO   [SIMULATION] Corbeille globale (Sonarr SeriesDelete) : l'ancien geste a déplacé 6 torrent(s)
```

```bash
# Les 20 dernières décisions de la corbeille globale
curl -s 'http://192.168.1.111:5001/corbeille/history?token=VOTRE_TOKEN&limit=20' | python3 -m json.tool
```

Le filet de la règle 7 et les règles 4, 6 et 7 de l'assistant gardent pour l'instant leur propre geste.

---

## L'assistant d'import

Un téléchargement terminé que Sonarr ou Radarr n'arrive pas à importer reste dans leur file. L'état dépend du nombre de vidéos : `importBlocked` en général, mais `importPending` avec un avertissement quand une vidéo unique est refusée ou qu'il n'y a rien à importer. L'*arr attend une intervention. Tant que le téléchargement y reste, il refuse les autres releases des mêmes épisodes. Les téléchargements morts et les packs incomplets bloquent aussi des demandes, sans que l'*arr s'en plaigne. L'assistant examine ces téléchargements et règle ceux qui peuvent l'être sans risque.

Il lit les files avec les lignes sans média. Quand le média d'un téléchargement change, après un autre import par exemple, l'*arr relit le seul nom du torrent. S'il ne le reconnaît pas, il le détache de son film ou de sa série. L'assistant reprend ces lignes, à condition que l'historique de cet *arr montre leur grab : le média revient de ce grab. Les lignes sans média de l'autre *arr, qui voit les téléchargements d'une catégorie partagée, n'ont pas de grab ici : elles restent écartées.

### Déclenchement

- **L'event « On Manual Interaction Required »**, à cocher dans le webhook de Sonarr et de Radarr. L'*arr l'émet au moment où il bloque un import, dans trois cas : titre non reconnu, grab rattaché par ID, ou épisodes attendus manquants. L'assistant passe deux minutes plus tard, le temps que la file affiche l'état `importBlocked`. Les events reçus entre-temps se regroupent dans ce passage.
- **Un passage de renfort**, toutes les `IMPORT_ASSIST_INTERVAL_MINUTES` (6 h par défaut, `0` le désactive), et deux minutes après chaque démarrage. L'*arr n'émet l'event qu'une fois par téléchargement, et l'oublie quand il redémarre : ce passage rattrape les events manqués. Un téléchargement mort ne déclenche aucun event, pas plus qu'une vidéo unique refusée : c'est ce passage, ou celui d'un autre event, qui les juge (règles 6 et 7). Avant chaque passage de renfort, l'assistant relance aussi un torrent dont la vérification de la règle 4 a été interrompue.
- **L'event « On Grab »** de Sonarr lance tout de suite la vérification d'un pack de saison (règle 4), en arrière-plan, sans attendre de passage.
- **`POST /assist?token=…`** lance un passage tout de suite et renvoie les verdicts :
  - sans paramètre, chaque règle suit son interrupteur ;
  - `&dry_run=true` est un aperçu : toutes les règles simulent, même actives ;
  - `&dry_run=false` fait agir la règle 1 même si elle est en simulation, et les autres règles suivent toujours leur interrupteur ;
  - seules les valeurs `0`, `false` et `no` font agir : une valeur mal saisie reste un aperçu.

Un seul passage tourne à la fois, quel que soit le process gunicorn qui le lance. Pendant un passage, `/assist` répond `409`. Un passage programmé par un event qui trouve le verrou pris est reprogrammé, au lieu d'être perdu : une recherche de pack complet ou d'autre version peut tenir le verrou plusieurs minutes.

### Les règles

**Chaque règle a son interrupteur**, en simulation par défaut. En simulation, elle journalise ce qu'elle ferait (`[SIMULATION] ✓ …`), sans rien changer. Aucune ne dépend de l'interrupteur d'une autre. Le numéro 3 n'est pas attribué.

| Règle | Interrupteur | Ce qu'elle fait |
|-------|--------------|-----------------|
| 1 · débloquer la file | `IMPORT_ASSIST_DRY_RUN=false` | Sort de la file un téléchargement déjà importé, et cherche ses épisodes manquants |
| 2 · torrent mal nommé | `IMPORT_ASSIST_NOM_DRY_RUN=false` | Renomme avec le titre de sa release un torrent que l'*arr ne reconnaît pas, pour qu'il l'importe lui-même |
| 4 · pack incomplet | `IMPORT_ASSIST_PACK_DRY_RUN=false` | Remplace un pack de saison partiel par un pack complet, ou comble ses trous |
| 5 · bonne saison | `IMPORT_ASSIST_ACCORD_DRY_RUN=false` | Range dans la bonne saison TVDB ce que Seerr a demandé sous un autre numéro |
| 6 · téléchargement mort | `IMPORT_ASSIST_MORT_DRY_RUN=false` | Lance une autre version en parallèle d'un téléchargement qui ne reçoit plus rien ; la première finie gagne |
| 7 · téléchargement inutile | `IMPORT_ASSIST_INUTILE_DRY_RUN=false` | Sort de la file, sans blacklist, un téléchargement bloqué qui n'apporterait rien, et le met en corbeille, où il continue de seeder |

Principes communs :
- **pas de blacklist sans raison**, car une release écartée à tort ne revient plus. Seule la règle 4 blackliste le pack partiel qu'elle remplace ;
- **la corbeille continue de seeder.** Un torrent privé déjà entamé n'est jamais arrêté, à cause du hit-and-run ;
- **un dossier partagé avec un autre torrent reste dans sa catégorie** ;
- **un téléchargement en erreur n'empêche pas de traiter les suivants** ;
- **un même verdict n'est journalisé qu'une fois par jour**, et une action réelle n'est jamais rejouée dans ce délai.

**Lire les logs.** Chaque ligne de l'assistant porte le nom de sa règle, et `[SIMULATION]` quand elle n'a rien changé. Au démarrage, une ligne résume ce que fait chaque règle :

```
INFO   Assistant d'import : règle 1 · débloquer la file ACTIVE, règle 2 · torrent mal nommé ACTIVE, règle 4 · pack incomplet ACTIVE, règle 5 · bonne saison ACTIVE, règle 6 · téléchargement mort ACTIVE, règle 7 · téléchargement inutile ACTIVE
INFO     ✓ [règle 7 · téléchargement inutile] [sonarr] Veronica.Mars.iNTEGRALE.MULTi.WEBRiP.1080p.x265-SN2P : Sonarr refuse les 72 vidéo(s) : la bibliothèque a déjà mieux pour chacune → retiré de la file sans blacklist, en corbeille, où il continue de seeder
INFO     ⏭ [règle 2 · torrent mal nommé] [sonarr] Ghosts.S06E01.NORDiC.1080p.VIAP.WEB-DL.DDP5.1.H.264-ADDICTION : le nom du torrent est déjà le titre de la release : seul l'identifiant de l'indexeur relie ce téléchargement au média
INFO     [SIMULATION] ✓ [règle 4 · pack incomplet] [sonarr] Kengan.Ashura.S01...-SQUEEZE : pack partiel : 12/24 épisodes attendus de la saison 1 → remplacé par Kengan.Ashura.S01...-T3KASHi (C411, 15 sources, score 195)
INFO   === Assistant d'import (passage de renfort) : 3 téléchargement(s) bloqué(s), 0 mort(s), 2 nouveau(x) verdict(s) ===
```

`✓` : la règle agit, ou agirait en simulation. `⏭` : elle n'a rien à faire, et dit pourquoi. `⚠` : une erreur, ou un cas à voir à la main. Un passage sans nouveau verdict n'écrit rien. `/assist/history` relit les verdicts, et `/assist/history?rule=7` ceux d'une seule règle.

### Règle 1 · débloquer la file : déjà importé, mais resté coincé

Chaque vidéo du torrent a déjà un hardlink dans le dossier du média du grab. La comparaison se fait par inode, sans aucun nom de fichier. Les échantillons ne comptent pas : « sample » dans le nom, et moins de 300 Mo. Cas typiques :
- un pack « S01 » qui n'apporte qu'une partie de la saison TVDB, comme une partie Netflix d'un anime. L'*arr a importé ce qu'il contenait, mais attend les épisodes manquants et garde le téléchargement ;
- un import fait à la main, sans rattacher le téléchargement.

L'assistant :
1. passe le torrent dans la catégorie après import de son client, celle que l'*arr aurait posée lui-même. Le torrent sort de la file et continue de seeder ;
2. pour Sonarr, lance la recherche des épisodes que la release devait apporter et qui manquent encore, s'ils sont surveillés et déjà diffusés. Au-delà de 25 épisodes, la recherche est laissée à la main : Sonarr interroge les indexeurs épisode par épisode.

Il ne fait rien si :
- une vidéo au moins manque dans la bibliothèque, ou n'y figure que sous un autre média. Si aucune n'y est, la règle 7 (téléchargement inutile) ou la règle 2 (torrent mal nommé) peuvent prendre le relais ;
- le torrent est incomplet, absent de qBittorrent, ou si ses données sont introuvables sur le disque ;
- le client n'a pas de catégorie après import, ou si c'est déjà la catégorie actuelle, car le torrent ne sortirait pas de la file. De même si cette catégorie a un autre dossier : la règle ne déplace jamais rien sur le disque ;
- il a déjà agi sur ce téléchargement dans les dernières 24 h.

Rien n'est importé ni supprimé. Si Sonarr refuse la recherche après le changement de catégorie, le verdict `search-failed` le signale : la recherche est alors à lancer depuis Sonarr. Verdicts : `seed`, `search-failed` et `ignored:…`.

```
INFO     [SIMULATION] ✓ [règle 1 · débloquer la file] [sonarr] Kengan.Ashura.S01.MULTI.VFF.1080p.WEBRip.EAC3.2.0.x265-SQUEEZE : 12/12 vidéo(s) déjà dans la bibliothèque → [seed] Séries animation, recherche de 12 épisode(s) manquant(s)
INFO     ⏭ [règle 1 · débloquer la file] [radarr] Un.Film.2026.1080p.WEB-DL-GRP.mkv : 0/1 vidéo(s) dans la bibliothèque
```

### Règle 2 · torrent mal nommé : la release est bonne, pas le nom du torrent

Au bout du téléchargement, Sonarr et Radarr reconnaissent le média d'après **le nom du torrent dans qBittorrent**, relu à chaque rafraîchissement, et non d'après le titre de la release. Ce nom est souvent celui du fichier, choisi par l'uploader : `Resident.Evil.2026.2026.1080p.AMZN…-KyoGo.mkv` pour la release `Resident.Evil.2026.VOSTFR.1080p.WEB…-KyoGo`.

Avec l'année en double, Radarr y lit le titre « Resident Evil 2026 », un film qui n'existe pas. Il retrouve le film par l'historique du grab, mais celui-ci l'avait rattaché par l'identifiant de l'indexeur. Radarr bloque alors l'import : « matched to movie by ID. Manual Import required ». Sans ce blocage, un identifiant faux suffirait à importer un autre film.

La règle renomme le torrent avec **le titre de la release**. L'*arr relit ce nom à son rafraîchissement suivant, reconnaît le média et importe lui-même, avec ses contrôles habituels (upgrade, échantillon, place libre). Seul le nom affiché change : aucun fichier ne bouge, et le torrent continue de seeder. Le fichier importé garde le titre de la release comme nom de scène : ses formats, VOSTFR par exemple, sont reconnus.

Elle n'agit que sur un téléchargement bloqué « matched … by ID », complet, dont aucune vidéo n'est dans la bibliothèque, et seulement si :
- **le titre de la release désigne à lui seul le même média** pour l'*arr (`/parse`) : le même film, ou la même série avec au moins les épisodes de la file. C'est la preuve qui manquait à l'*arr ;
- le nom actuel du torrent ne désigne aucun autre média ;
- la saison n'est pas une saison demandée sous un autre numéro (règle 5) ;
- **le torrent ne contient aucun fichier suspect**, coché ou non. Sont suspects :
  - les exécutables (`.exe`, `.bat`, `.msi`…) ;
  - les scripts (`.ps1`, `.vbs`, `.js`…) ;
  - les raccourcis (`.lnk`, `.url`) ;
  - les archives (`.zip`, `.rar`…).

  Une fausse release les glisse à côté d'une vidéo leurre. Or Sonarr et Radarr ne les signalent que si rien d'autre n'est importé : une fois renommé, le leurre serait importé. Un tel torrent reste bloqué, avec un avertissement `rename-suspect`.

Un titre de release identique au nom du torrent ne prouve rien de plus. C'est le cas de `Ghosts.S06E01.NORDiC…` : « Ghosts » désigne six séries sur TVDB, et seul l'identifiant de l'indexeur relie ce téléchargement à Ghosts (US). Il reste bloqué.

Si l'*arr bloque toujours le torrent 10 minutes après le renommage, l'avertissement `rename-blocked` le signale, et l'import reste à faire à la main. Le torrent n'est jamais renommé deux fois.

**Si un autre import est passé devant entre-temps**, parce que l'*arr a trouvé et importé mieux pendant que celui-ci restait bloqué, le renommer ne servirait à rien : l'*arr le refuserait. La règle 7 le retire alors de la file. C'est arrivé à Resident Evil (2026) le 2026-11-02 : une version MULTi est arrivée deux heures après la KyoGo.

**Un téléchargement détaché de son média** (voir plus haut) ne peut plus être réglé par un renommage. L'*arr importe avec le média de son suivi, qu'il a perdu, et partirait donc sans film ni série. L'avertissement `rename-orphan` demande un import manuel, sauf si la règle 7 constate que le téléchargement est inutile. L'import manuel n'est pas automatisé : par l'API, il ne contrôle pas l'upgrade, et pourrait remplacer un meilleur fichier.

Verdicts : `rename` (torrent renommé), `rename-suspect`, `rename-blocked`, `rename-orphan` et `rename-failed` (à voir à la main), et `ignored:…` (rien à faire, avec la raison).

```
INFO     ✓ [règle 2 · torrent mal nommé] [radarr] Resident.Evil.2026.2026.1080p.AMZN.WEB-DL.DDP5.1.H.264-KyoGo.mkv : le titre de la release désigne bien Resident Evil (2026) : torrent renommé Resident.Evil.2026.VOSTFR.1080p.WEB.EAC3.5.1.H264-KyoGo → Radarr l'importera seul
INFO     ⏭ [règle 2 · torrent mal nommé] [sonarr] Ghosts.S06E01.NORDiC.1080p.VIAP.WEB-DL.DDP5.1.H.264-ADDICTION.mkv : le nom du torrent est déjà le titre de la release : seul l'identifiant de l'indexeur relie ce téléchargement au média
```

### Règle 4 · pack incomplet : pack de saison partiel

Sonarr prend tout pack « S01 » pour la saison entière. Un pack qui n'en contient qu'une partie passe donc pour complet : une moitié de saison TVDB, comme une partie Netflix d'un anime, ou une « Part 1 ». Un vrai pack complet est ensuite refusé à chaque recherche, parce qu'il n'améliore pas les épisodes déjà là.

**Au grab.** Dès que Sonarr prend un pack de saison, l'assistant lit sa liste de fichiers dans qBittorrent, qui a le `.torrent` dès le grab. Chaque vidéo passe par le parseur de Sonarr, celui qui servira à l'import. S'il manque au pack des épisodes diffusés, surveillés et sans fichier, l'assistant :
1. arrête le torrent, avant qu'il ait eu le temps de télécharger ;
2. cherche un pack complet de la saison dans Prowlarr ;
3. s'il en trouve un, l'ajoute dans qBittorrent, dans la catégorie du client Sonarr et sous le titre de sa release : Sonarr le reconnaît par ce titre et l'importe comme les siens ;
4. blackliste ensuite le pack partiel depuis la file de Sonarr, sans le supprimer du client ni relancer de recherche. Il le met ensuite en corbeille, arrêté : à peine entamé, il n'a rien à seeder.

**Sans pack complet en français, les trous sont comblés en VO.** Le seul pack complet peut être dans une autre langue, sous le seuil de 50, mais encore accepté par Sonarr, dont le score minimal du profil est de -20. Le pack partiel est alors gardé, et repart.
- Le pack en VO est ajouté à l'arrêt. Ses fichiers passent en « ne pas télécharger », sauf ceux des épisodes manquants, puis il démarre : seuls ces épisodes sont téléchargés.
- Sonarr importe ce qu'il trouve. Une fois ces épisodes importés, l'assistant lance aussitôt leur recherche en français.
- Les upgrades remplaceront ensuite la VO, dès qu'une version française existera à l'unité.

Sans aucun pack complet, le pack partiel repart : mieux vaut une demi-saison que rien.

Un pack complet doit :
- apporter tous les épisodes manquants, d'après la liste des fichiers de son `.torrent` ;
- être un pack de la même saison, ni multi-saisons, ni « Part » ;
- porter le même titre de série que le pack partiel. Une série d'anthologie rassemble sous une fiche TVDB des histoires que leurs releases nomment chacune « S01 ». Ainsi, « Monster: The Lizzie Borden Story » est la saison 4 de « Monster » pour TVDB, mais Sonarr la range en saison 1, celle de Dahmer, qui a 10 épisodes au lieu de 8. Le « S01 complet » serait une autre histoire que celle demandée ;
- avoir une qualité autorisée par le profil de la série, au moins aussi bonne que celle du pack partiel ;
- avoir un score de formats d'au moins 50 (`IMPORT_ASSIST_PACK_MIN_SCORE`), même s'il est moins bien noté que le pack partiel : mieux vaut une saison sans trou. Si le pack partiel est lui-même sous 50, son propre score sert de seuil. Le bonus que le profil donne aux packs ne compte pas, puisque le partiel et ses remplaçants en ont tous. Avec le profil actuel :
  - une release qui porte du français fait au moins 55 : MULTi 55, VOSTFR 80, VF 110, MULTi VFF 195 ;
  - une release sans français part de -20, et gagne 10 par format technique (Atmos, Dolby Vision, HDR…). Un pack anglais reste donc sous le seuil ;
- avoir au moins 3 sources et un lien `.torrent`. Un magnet n'a pas de liste de fichiers à lire avant d'être ajouté.

Les mieux classés passent d'abord : score, puis qualité, puis sources. Seuls les `.torrent` des 4 premiers sont lus, car chaque lecture passe par un tracker.

Garde-fous :
- **Hit-and-run :** un torrent privé déjà téléchargé à plus de 2 % n'est jamais arrêté. On le laisse finir, et la règle reprend après l'import.
- **Pas de boucle :** au plus 2 remplacements par saison et par semaine. Au-delà, la saison revient à la main.
- **Dossier partagé :** un pack complet publié sous le même nom de dossier que le partiel en partage les fichiers. Le partiel reste alors arrêté dans sa catégorie, parce que la corbeille a son propre dossier : ATM y emporterait les fichiers de l'autre.
- **Vérification interrompue :** si le service redémarre pendant la recherche, le passage de renfort relance le torrent arrêté, passé 30 minutes.

**Après l'import, en filet.** Un pack partiel peut quand même être importé : pris pendant un arrêt du service, magnet sans liste de fichiers au grab, ou torrent privé trop entamé. La règle 1 le trouve alors bloqué dans la file. Au lieu de lancer la recherche des épisodes manquants un à un, l'assistant ajoute un pack complet, choisi de la même façon.
- Une fois ce pack téléchargé, Sonarr importe seul les épisodes manquants, mais refuse de remplacer ceux du pack partiel, faute de les améliorer.
- L'assistant les importe alors à la main, en hardlink, à la place des fichiers du pack partiel, et de ceux-là seulement. Un épisode remplacé entre-temps par une autre release n'est pas touché.
- Le pack partiel, qui n'a plus de fichier dans la bibliothèque, part en corbeille par l'event d'upgrade.
- Si le pack complet est moins bien noté que le partiel, il ne fait que combler les trous : les épisodes du partiel, meilleurs, restent en place. Un pack en VO ne télécharge que les fichiers de ces trous.

**Fin du remplacement.** Une fois chaque épisode prévu pourvu, le pack complet sort de la file de Sonarr en passant dans la catégorie après import, comme avec la règle 1. Ses éventuelles autres vidéos, pour des épisodes déjà mieux pourvus, restent en seed.

Conditions :
- Seul le filet après import dépend aussi de la règle 1 : il remplace la recherche d'épisodes de celle-ci, et n'agit donc que si elle agit.
- Elle demande « On Grab » coché dans le webhook de Sonarr.
- Sans `PROWLARR_URL` et `PROWLARR_API_KEY`, elle signale les packs partiels sans y toucher.
- La saison 0 (épisodes spéciaux) n'est pas traitée.

Verdicts :
- `pack-ok`, `pack-partial` (pack gardé), `pack-replace` ;
- `pack-fill` (pack gardé, trous comblés en VO) ;
- `pack-reimport` (import à la main des épisodes du partiel) ;
- `pack-done` (remplacement terminé, pack complet sorti de la file) ;
- `pack-skipped` et `pack-error` ;
- `pack-stopped` et `pack-resumed`, qui tracent l'arrêt d'un torrent et sa relance.

Les remplacements sont dans la table `pack_replace`.

```
INFO     [SIMULATION] ✓ [règle 4 · pack incomplet] [sonarr] Kengan.Ashura.S01.MULTI.VFF.1080p.WEBRip.EAC3.2.0.x265-SQUEEZE : pack partiel : 12/24 épisodes attendus de la saison 1 → remplacé par Kengan.Ashura.S01.MULTi.VFF.1080p.WEBRiP.EAC3.2.0.x265-T3KASHi (C411, 15 sources, score 195)
INFO     [SIMULATION] ⏭ [règle 4 · pack incomplet] [sonarr] Ranma ep 15-21 : pas un pack de saison
```

### Règle 5 · bonne saison : accorder la demande Seerr et la saison Sonarr

Seerr demande des saisons numérotées comme TMDB, Sonarr les range comme TVDB, et les deux ne s'accordent pas toujours. « Monster: The Lizzie Borden Story » est une mini-série d'une saison pour TMDB, mais la saison 4 de « Monster » pour TVDB. Seerr a demandé la saison 1, et Sonarr y a rangé les 8 épisodes de Lizzie Borden, sous les titres de Dahmer, qui en compte 10.

C'est la demande qui tranche. À chaque passage, avant les autres règles, l'assistant relit les demandes de séries Seerr des 30 derniers jours, approuvées ou terminées. Pour chaque saison demandée, il compare ses épisodes TMDB avec ceux de la saison TVDB de même numéro : nombre, dates de diffusion, titres.
- Si elles collent, rien à faire.
- Sinon, il cherche la seule autre saison TVDB qui colle : toutes les dates identiques, ou au moins 80 % des titres, car les épisodes à venir n'ont pas de date. Pour Lizzie Borden, c'est la S04, avec 8/8 dates et 8/8 titres, alors que la S03 a aussi 8 épisodes mais aucun en commun.
- Si aucune saison ne colle, ou si plusieurs collent, il le signale et ne touche à rien.

Quand une saison est trouvée, l'assistant :
1. surveille la saison TVDB trouvée ;
2. reprend les fichiers de la demande rangés dans la saison demandée, ceux dont la release porte le titre de la fiche demandée :
   - il les réaffecte aux épisodes de même numéro de la bonne saison, par un import manuel sur place, puis les range par un renommage. Rien n'est copié ni supprimé, et le torrent reste lié ;
   - c'est tout ou rien : si un épisode cible a déjà un fichier, aucun fichier ne bouge ;
3. cesse de surveiller la saison demandée, sauf si une autre demande la vise ou si elle garde des fichiers d'une autre release.

La règle 1 ne cherche plus les épisodes manquants de la saison écartée, même en simulation. La règle 4 n'y cherche pas non plus de pack complet, et la règle 2 n'y renomme rien.

Elle demande `SEERR_URL` et `SEERR_API_KEY`. Verdicts : `accord-fix` et `accord-unknown`, journalisés sous `seerr-<demande>-s<saison>`.

Limites :
- Seerr peut repasser la demande en « partiellement disponible » à son prochain scan, une fois la saison demandée vidée.
- Une saison TMDB qui couvre une partie d'une saison TVDB, comme les parties d'un anime, n'est pas encore accordée.
- La règle n'aide pas Sonarr à trouver une release d'anthologie nommée « S01 » pour une saison 4 : elle range ce qu'il a déjà pris.

```
INFO     [SIMULATION] ✓ [règle 5 · bonne saison] [sonarr] Monster (2022) S01 (demande Seerr 912, Hind) : saison 1 demandée = S04 TVDB (8/8 dates et 8/8 titres identiques) → S04 surveillée, S01 plus surveillée, 8 fichier(s) réaffecté(s) de S01 à S04
INFO     [SIMULATION] ✓ [règle 1 · débloquer la file] [sonarr] Monster.The.Lizzie.Borden.Story.S01...-BYOR : 8/8 vidéo(s) déjà dans la bibliothèque → [seed] Rays_Le _Chat (règle 5 : la saison 1 demandée est une autre saison TVDB, ses épisodes manquants ne sont pas cherchés)
```

### Règle 6 · téléchargement mort : plus aucune source

Un torrent sans source reste dans la file de Sonarr ou de Radarr indéfiniment. Aucun des deux ne le remplace, et la demande attend, parfois des mois.

**Quand un téléchargement est-il mort ?** À chaque passage, l'assistant regarde les téléchargements en cours des files. Un téléchargement est mort quand, depuis `IMPORT_ASSIST_MORT_JOURS` jours (3 par défaut), il n'a rien reçu, sans aucune source connectée et sans débit. Ne comptent pas :
- un torrent fini ;
- un torrent qui attend son tour, se vérifie ou se déplace ;
- un torrent dont les fichiers ont disparu, qui est un autre problème ;
- un torrent sans date d'ajout, qu'on ne peut pas juger.

Un torrent mis en pause à la main compte aussi : s'il ne reçoit plus rien, il bloque la demande comme les autres.

**Rien à remplacer.** Si le fichier en place vaut déjà au moins la release morte, aucune recherche n'a lieu : la règle 7 retire le téléchargement (voir plus bas). Cela ne vaut qu'avant le début d'une course.

**La course.** Sinon, le téléchargement mort n'est **ni blacklisté ni retiré**, car un seeder peut revenir, même après des mois. L'assistant lance une course :
1. **Le choix de l'autre version.**
   - Sonarr ou Radarr cherche les autres versions du même film, ou des mêmes épisodes. L'assistant prend la meilleure dans leur propre ordre de préférence : qualité, score de formats, langue.
   - Elle est prise **quel que soit son nombre de sources**.
   - Les seuls refus passés outre sont « la place est prise dans la file », justement par le téléchargement mort, et « pas assez de sources ».
   - La release déjà prise est écartée. Le nom du torrent ne permet pas de la reconnaître : `Parking.mkv` dans qBittorrent, `Parking (2008) Chung Mong-Hong DVDRip…` chez l'indexeur. L'assistant lit donc dans l'historique de l'*arr ce qui a été noté à la prise : le guid de la release, et son titre sur cet indexeur. La même release sur un autre tracker reste une vraie alternative : c'est un autre torrent, avec d'autres sources.
   - Une version que qBittorrent a déjà est écartée, quand l'indexeur donne son hash.
   - Le grab se fait comme un « télécharger quand même » dans l'*arr, en un seul appel. S'il est refusé, par exemple par qBittorrent pour un torrent qu'il a déjà, un avertissement le signale et la version compte comme un essai. La suivante attend la période suivante.
2. **Le premier téléchargement fini et importé gagne**, l'original compris si son seeder revient. Les autres versions sortent alors de la file, sans blacklist, et partent en corbeille, arrêtées. Un torrent privé déjà entamé reste en route, et une version finie continue de seeder.
3. Si toutes les versions de la course meurent à leur tour, la suivante est prise, une par période de `IMPORT_ASSIST_MORT_JOURS`, jusqu'à `IMPORT_ASSIST_MORT_ESSAIS` versions (5 par défaut), prises refusées comprises. Ensuite, on attend que l'une reparte.
4. Sans autre version, l'original reste seul et en route. Une nouvelle recherche a lieu au plus une fois par période.

Si une version finie n'est pas importée, parce que l'*arr la bloque dans sa file, un avertissement le signale, et l'import reste à faire à la main. Les autres versions continuent.

Pour une série, une autre version doit couvrir tous les épisodes du téléchargement mort : un pack ou un multi-épisodes. La recherche se fait sur le premier de ces épisodes.

Garde-fous : au plus 5 recherches par passage, les téléchargements les plus anciens d'abord. Une course qui a épuisé ses essais ne cherche plus, et ne prend pas de place. Une erreur, par exemple une recherche qui échoue, est journalisée : le nouvel essai attend 24 h. Rien n'est supprimé.

Verdicts :
- `dead-race` : une autre version est prise en plus ;
- `dead-wait` : aucune autre version, on attend ;
- `dead-won` : une version a gagné, les autres sont en corbeille ;
- `dead-blocked` : une version est finie mais pas importée ;
- `dead-refused` : la prise a été refusée, elle compte comme un essai ;
- `dead-error` : une erreur, avec un nouvel essai au bout de 24 h.

Les courses sont dans la table `dead_race`.

```
INFO     [SIMULATION] ✓ [règle 6 · téléchargement mort] [radarr] Féroce.2017.FRENCH.1080p.WEB.H264-ViVENDi : plus rien reçu depuis 14 j → autre version prise en plus, Feroce.2017.MUET.VO.1080p.WEB.AAC.2.0.H264-FABZ (C411 (Prowlarr), 18 source(s), score 101) ; la première finie gagnera
INFO     [SIMULATION] ⏭ [règle 6 · téléchargement mort] [radarr] Parking.mkv : plus rien reçu depuis 23 j, aucune autre version trouvée : on attend, le torrent reste en route
```

### Règle 7 · téléchargement inutile : la bibliothèque a déjà mieux

Un téléchargement fini que l'*arr n'importera jamais, parce que la bibliothèque a déjà mieux, reste bloqué dans sa file indéfiniment. Jellyfin, s'il lit les dossiers de téléchargement, le montre en double. La règle le reconnaît de deux façons, quand aucune de ses vidéos n'est dans la bibliothèque :
1. **l'*arr a refusé chacune de ses vidéos** parce qu'elle n'améliore pas le fichier en place (« Not an upgrade », « Not a Custom Format upgrade », « Not a quality revision upgrade »). La file donne une entrée de statut par fichier, ou une seule au nom du téléchargement pour une vidéo unique, alors laissée en `importPending` avec un avertissement. Les échantillons refusés comme tels ne comptent pas. C'est le cas de Veronica Mars SN2P : un pack à 150, contre 160 à 235 pour les 72 épisodes en place. C'est aussi le cas de la SUPPLY de Resident Evil (2026) : une vidéo à 151, contre 231 en place ;
2. **un téléchargement bloqué « matched by ID »** (règle 2), quand le fichier en place vaut au moins la release. Il faut une qualité de rang égal ou supérieur dans le profil, les qualités d'un groupe partageant son rang, et, à rang égal, un score de formats au moins égal. Pour une série, chaque épisode du téléchargement doit avoir un tel fichier. C'est le cas de la KyoGo de Resident Evil (2026) : WEBDL-1080p à 51, contre la kazui MULTi en place, WEBDL-1080p à 171. Dans le doute, par exemple si une note manque, la règle ne fait rien.

La règle 6 lui confie aussi **un téléchargement mort**, avant le début d'une course, quand le fichier en place vaut au moins la release, selon la même comparaison. C'est le cas de L'invitée de l'hiver : un WEBDL-1080p à 39, mort depuis 101 jours, contre un Bluray-1080p à 60 en place.

Le téléchargement sort alors de la file **sans blacklist** : l'*arr ne le reprendra pas, puisqu'il n'est pas un upgrade. Le torrent part en corbeille, où **il continue de seeder**. Un téléchargement mort y est arrêté, puisqu'il n'a rien à seeder, sauf un torrent privé déjà entamé. Rien n'est supprimé. Verdict : `useless`.

Une vidéo unique refusée pour un autre motif, un exécutable par exemple, ou un dossier sans rien à importer, n'est jamais touchée : l'assistant journalise le motif de l'*arr, une fois par jour, avec le verdict `ignored`.

**Le filet hors file.** La corbeille attend un signal : un event sur un fichier de la bibliothèque, ou une ligne de file. Un signal raté n'est jamais rattrapé, et le torrent reste en seed sans plus servir à rien : un import refusé puis sorti de la file, un upgrade passé avant que l'index connaisse le fichier. À chaque passage, la règle regarde donc aussi l'état réel des torrents hors file. L'un d'eux part en corbeille, où il continue de seeder, s'il remplit toutes ces conditions :
- il est fini depuis plus de `CLEANUP_GRACE_HOURS` (24 h) et rangé dans une catégorie après import d'un client Sonarr ou Radarr ;
- un *arr l'a pris : son historique garde le grab. Un ajout manuel n'est jamais touché ;
- aucune de ses vidéos n'est dans le dossier du média. C'est vérifié sur le disque, inode par inode, car l'index ne connaît un import à la main qu'à la sync suivante ;
- la bibliothèque a déjà chaque épisode, ou le film, par un autre fichier présent sur le disque. Sinon, le torrent est peut-être la dernière copie ;
- il n'est jamais sorti de la corbeille : si c'est le cas, quelqu'un l'a voulu.

Si le dossier du média est vide, par exemple quand un montage manque, la règle ne conclut rien. Elle traite au plus 10 torrents par passage, les plus anciens d'abord. Comme ailleurs dans cette règle, les cross-seeds du torrent restent en place. Le verdict est le même : `useless`.

```
INFO     ✓ [règle 7 · téléchargement inutile] [sonarr] Veronica.Mars.iNTEGRALE.MULTi.WEBRiP.1080p.x265-SN2P : Sonarr refuse les 72 vidéo(s) : la bibliothèque a déjà mieux pour chacune → retiré de la file sans blacklist, en corbeille, où il continue de seeder
INFO     ✓ [règle 7 · téléchargement inutile] [radarr] Resident.Evil.2026.2026.1080p.AMZN.WEB-DL.DDP5.1.H.264-KyoGo.mkv : la bibliothèque a déjà mieux (WEBDL-1080p, score 171) que la release Resident.Evil.2026.VOSTFR.1080p.WEB.EAC3.5.1.H264-KyoGo (WEBDL-1080p, score 51) → retiré de la file sans blacklist, en corbeille, où il continue de seeder
INFO     [SIMULATION] ✓ [règle 7 · téléchargement inutile] [radarr] L.invitee.de.l.hiver.1997.VOSTF.BR.1080P.HEVC.DTS-X-AZAZE.mkv : plus rien reçu depuis 101 j, et la bibliothèque a déjà mieux (Bluray-1080p, score 60) que cette release (WEBDL-1080p, score 39) → retiré de la file sans blacklist, en corbeille et arrêté : il n'a rien à seeder
INFO     [SIMULATION] ✓ [règle 7 · téléchargement inutile] [radarr] Resident.Evil.2026.MULTi.CA.1080p.WEB.H265-SUPPLY : Radarr refuse les 1 vidéo(s) : la bibliothèque a déjà mieux pour chacune → retiré de la file sans blacklist, en corbeille, où il continue de seeder
INFO     [SIMULATION] ✓ [règle 7 · téléchargement inutile] [sonarr] Lanterns.2026.S01E02.DV.HDR.2160p.WEB.H265-CAKES : hors de la file, aucune de ses 1 vidéo(s) n'est dans la bibliothèque, qui a déjà tout par d'autres fichiers → en corbeille, où il continue de seeder
```

---

## Exploitation

```bash
# Forcer une resync
curl -XPOST 'http://192.168.1.111:5001/sync?token=VOTRE_TOKEN'

# État de la base
curl -s 'http://192.168.1.111:5001/stats?token=VOTRE_TOKEN' | python3 -m json.tool

# Aperçu de l'assistant : toutes les règles simulent, rien ne change
curl -XPOST 'http://192.168.1.111:5001/assist?token=VOTRE_TOKEN&dry_run=true'

# Derniers verdicts de l'assistant, d'une seule règle
curl -s 'http://192.168.1.111:5001/assist/history?token=VOTRE_TOKEN&rule=6&limit=20' | python3 -m json.tool

# Suivre les logs
docker logs cleanup-linker -f
```

`/stats` renvoie `total_rows`, `total_paths`, `linked_to_torrent`, `unique_inodes`, `arr_managed_torrents`, `cleanup_log_entries`, `jellyfin_enabled`, `jellyfin_indexed_paths` et `last_sync`.

**Un passage long.** gunicorn interrompt toute requête au bout de 120 s. Or un passage qui cherche une autre version (règle 6) ou un pack complet (règle 4) peut durer plusieurs minutes : une recherche Sonarr d'anime interroge chaque indexeur épisode par épisode. Pour un passage réel unique, on passe alors par le conteneur, en surchargeant au besoin l'interrupteur d'une règle :

```bash
docker exec -e IMPORT_ASSIST_NOM_DRY_RUN=false cleanup-linker \
  python -c "import app; app.run_import_assist(source='passage manuel')"
```

---

## Référence

### Événements gérés

Chaque event Sonarr/Radarr qui porte un `downloadId` marque d'abord le torrent dans `arr_managed`.

**Sonarr**

| Event | Déclencheur | Action |
|---|---|---|
| `EpisodeFileDelete` | Suppression d'un épisode | Corbeille. Ignoré si le fichier est toujours sur le disque : il a été réaffecté (`manualOverride`, règle 5) |
| `EpisodeFileDelete` deleteReason=upgrade | On Episode File Delete For Upgrade, s'il est coché | Corbeille, avant l'import du nouveau fichier |
| `SeriesDelete` | Suppression d'une série entière | Corbeille de tous les torrents du dossier. Avec `CORBEILLE_GLOBALE=active`, corbeille globale, et rien si `deletedFiles` vaut `false` |
| `Download` isUpgrade=false | Import (On File Import) | Fichier importé indexé tout de suite |
| `Download` isUpgrade=true | Upgrade (On File Upgrade) | Nouveau fichier indexé, puis chaque `deletedFiles` traité comme une suppression |
| `Download` avec `episodeFiles` | On Import Complete, s'il est coché | Fichiers importés indexés |
| `Grab` | Release prise (On Grab) | Pour un pack de saison, vérification de la règle 4 en arrière-plan |
| `ManualInteractionRequired` | Import bloqué (On Manual Interaction Required) | Passage de l'assistant d'import sous deux minutes |
| `Test` | Test du webhook | Répond OK |

**Radarr**

| Event | Déclencheur | Action |
|---|---|---|
| `MovieFileDelete` | Suppression d'un film | Corbeille. Ignoré si le fichier est toujours sur le disque |
| `MovieFileDelete` deleteReason=upgrade | On Movie File Delete For Upgrade, s'il est coché | Corbeille, avant l'import du nouveau fichier |
| `MovieDelete` | Suppression d'un film entier | Corbeille de tous les torrents du dossier. Avec `CORBEILLE_GLOBALE=active`, corbeille globale, et rien si `deletedFiles` vaut `false` |
| `Download` isUpgrade=false | Import (On File Import) | Fichier importé indexé tout de suite |
| `Download` isUpgrade=true | Upgrade (On File Upgrade) | Nouveau fichier indexé, puis chaque `deletedFiles` traité comme une suppression |
| `Grab` | Release prise (On Grab) | Seulement le marquage `arr_managed` |
| `ManualInteractionRequired` | Import bloqué (On Manual Interaction Required) | Passage de l'assistant d'import sous deux minutes |
| `Test` | Test du webhook | Répond OK |

**Jellyfin** (route `/jellyfin`, car son payload n'a pas le format de celui des *arr)

| Event | ItemType | Action |
|---|---|---|
| `ItemDeleted` | `Movie`, `Episode`, `Video` | Résout `ItemId` → chemin → inode, puis corbeille ou délégation à l'*arr |
| `ItemDeleted` | `Series`, `Season` | Résout via `jellyfin_series_id` / `jellyfin_season_id` → dossier commun → une seule passe |
| Autre | — | Ignoré |

### API

Toutes les routes, sauf `/health`, demandent le token, en paramètre `token` ou dans l'en-tête `X-Token`. Les routes longues (`/sync`, `/cleanup`, `/restore`, `/bootstrap`, `/jellyfin/index`) répondent tout de suite et travaillent en arrière-plan : le résultat est dans les logs.

| Endpoint | Méthode | Paramètres | Description |
|---|---|---|---|
| `/webhook` | POST | — | Reçoit les events Sonarr/Radarr |
| `/jellyfin` | POST | `dry_run` | Reçoit les events Jellyfin |
| `/sync` | POST | — | Force une resync immédiate |
| `/jellyfin/index` | POST | — | Force la réindexation Jellyfin |
| `/cleanup` | POST | `dry_run` | Met en corbeille les torrents *arr orphelins ; avec la corbeille globale, ce qui ne sert plus au repos |
| `/restore` | POST | `since` (timestamp Unix) | Restaure les torrents du dernier `/cleanup`, ou tout ce qui a été tracé depuis `since` |
| `/bootstrap` | POST | `dry_run` | Peuple `arr_managed` depuis l'historique des *arr |
| `/assist` | POST | `dry_run` | Lance un passage de l'assistant d'import et renvoie ses verdicts. `409` pendant un autre passage |
| `/assist/history` | GET | `limit` (50, 500 au plus), `rule` | Derniers verdicts de l'assistant, avec la règle qui les a rendus |
| `/history` | GET | `limit` (50, 500 au plus) | Derniers moves vers la corbeille |
| `/corbeille/history` | GET | `limit` (50, 500 au plus) | Dernières décisions de la corbeille globale, simulées ou réelles |
| `/stats` | GET | — | Statistiques de la base |
| `/health` | GET | — | Healthcheck, sans token |

### Base de données

| Table | Contenu |
|---|---|
| `files` | Chaque chemin vidéo indexé : inode, hash et nom du torrent, catégorie, identifiants Jellyfin |
| `arr_managed` | Les hashes des torrents pris par un *arr |
| `cleanup_log` | Les moves vers la corbeille : hash, nom, catégorie d'origine, source, date |
| `sync_log` | Les syncs : début, fin, statut, lignes avant et après, torrents, durée |
| `assist_log` | Les verdicts de l'assistant : téléchargement, *arr, titre, décision, détails, simulation ou non, règle, date |
| `pack_replace` | Les remplacements de packs de la règle 4 |
| `dead_race` | Les courses de versions de la règle 6 : original, versions essayées, torrents de la course, dernière tentative |
| `corbeille_log` | Les décisions de la corbeille globale : source, geste, chemins, simulation ou non, résumé, détail, torrents déplacés par l'ancien geste, date |

---

## Limites connues

**Chemin non trouvé en base.** La sync ne passe que toutes les 12 h. Pour qu'un fichier supprimé peu après son import soit retrouvé, l'event `Download` de Sonarr/Radarr l'indexe tout de suite. Le message reste possible dans trois cas :
- **import non signalé :** le webhook ne coche pas « On File Import » ;
- **fichier hors import :** copie manuelle, ou copie au lieu d'un hardlink ;
- **qBittorrent injoignable** au moment de l'import.

La suppression ne déplace alors rien, et le torrent devenu orphelin attend le prochain `/cleanup`. Côté Jellyfin, un média ajouté depuis la dernière sync reste `unresolved` : son `ItemId` n'est connu qu'à l'indexation suivante, que `/jellyfin/index` force.

**Torrents multi-saisons.** Certains torrents regroupent plusieurs saisons sous un même nom de dossier. L'association par inode peut être incomplète dans ces cas.

**Upgrade vers la même release.** Pendant un upgrade, Sonarr et Radarr suppriment l'ancien fichier, puis créent le hardlink du nouveau. « Delete For Upgrade » part entre les deux.
- Une même release prise sur un autre tracker partage les fichiers de l'ancien torrent. Si cross-seed l'a déjà enregistrée, elle porte le même nom : elle entre dans le lot, les deux torrents partent en corbeille avec les fichiers, et l'import échoue.
- « On File Upgrade » part, lui, après le hardlink. Le nouveau fichier est alors indexé d'abord, puis la vérification des hardlinks garde les deux torrents.
- C'est pourquoi la configuration ci-dessus laisse « Delete For Upgrade » décoché. Dans Sonarr, cela suppose « On File Import » coché : sans lui, plus rien ne signalerait les fichiers remplacés, et l'ancien torrent resterait hors de la corbeille.

**Lien apparu depuis la sync.** Un hardlink créé depuis la dernière sync, comme un cross-seed injecté entre-temps, fait passer une vraie suppression Jellyfin pour un déplacement. Elle est ignorée et journalisée comme telle (`⏭ Ignoré : … déplacés`). Il faut alors finir la suppression côté Sonarr/Radarr. C'est le sens prudent : rien n'est supprimé à tort.

**Emplacement d'un torrent.** Le champ `name` de l'API qBittorrent est un nom d'affichage. Il diffère du dossier réel dès que le `.torrent` porte un titre différent de son contenu, ou qu'un torrent a été renommé (règle 2). C'est `content_path` qui fait foi. Sur l'installation de référence, 308 torrents sur 3269 sont dans ce cas : reconstruire le chemin avec `save_path` et `name` les rendait introuvables, donc non indexés.

**L'ancien `/cleanup` ne juge pas l'utilité d'un téléchargement.** Tout torrent pris par un *arr, sans lien vers la bibliothèque depuis 24 h et absent des files des *arr, part en corbeille, même si Jellyfin le lit. La corbeille globale au repos juge, elle, chaque vidéo. Aucune tâche ne lance `/cleanup`.

**La corbeille n'est vidée que par cleanuparr.** Sans lui, elle grossit indéfiniment.

**Corbeille globale : ce qu'elle ne couvre pas.**
- Un nom caché dans `/data/Media`, sans fiche, comme celui d'un film rangé dans `Media/Films` mais lu par Jellyfin dans `Multimedia/Films` : qBittorrent ne voit que `/data/Multimedia`, et un torrent de corbeille ne peut pas le lister. Le fichier est gardé, et seul un import dans Radarr règle le cas.
- Un fichier qui n'a que son nom dans `/data/Media` n'a rien à mettre en corbeille : l'*arr ou Jellyfin le suppriment directement.
- Un upgrade d'épisode ne libère rien tant que son pack sert encore aux autres épisodes : un torrent ne se vide pas à moitié.
- Si Jellyfin ne répond pas, chaque vidéo d'un dossier de téléchargement compte comme visible. Seul un petit bonus ne retient alors pas son torrent, et un torrent de corbeille n'est pas déplacé.
- Un lien créé depuis la dernière sync, un cross-seed injecté entre-temps par exemple, est inconnu de l'index : son fichier est gardé.

**Passage d'assistant perdu.** Le passage programmé par un event vit dans le worker gunicorn qui a reçu l'event. Si ce worker redémarre dans les deux minutes, le passage est perdu, et le téléchargement attend le passage de renfort suivant.

**`/assist` limité à 120 s.** gunicorn interrompt la requête au-delà, avec le passage qu'elle portait. Pour un passage long, voir [Exploitation](#exploitation).

**Téléchargement détaché de son média.** Quand l'*arr a perdu le média d'un téléchargement bloqué « matched by ID », et que celui-ci serait encore utile, l'assistant le signale (`rename-orphan`). L'import manuel reste à faire.
