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
6. qBit API : écarte tout torrent dont l'emplacement
   sert aussi à un torrent hors du lot
        │
        ▼
7. qBit API : setCategory(tous les hashes, "cleanuparr-unlinked")
        │
        ▼
8. DB locale : DELETE WHERE inode = ?
        │
        ▼
9. cleanuparr prend le relais selon sa propre logique
```

Le webhook marque également le `downloadId` de chaque event dans la table `arr_managed` — ce qui garantit que les futurs `/cleanup` ne toucheront jamais les torrents ajoutés manuellement.

L'étape 6 protège les **torrents jumeaux**. Une même release prise sur deux trackers atterrit au même endroit, et le second torrent réutilise les fichiers du premier. La corbeille a son propre dossier : y passer l'un fait déplacer ces fichiers par ATM, et l'autre reste en seed sur du vide. C'est arrivé sur un upgrade Radarr vers la même release : la suppression de l'ancien fichier a envoyé l'ancien torrent en corbeille, avec le fichier que le nouveau allait importer.

Le jumeau peut être plus récent que la dernière sync. Le contrôle lit donc la liste qBittorrent au moment du move, et compare les `content_path`. Un torrent écarté reste en place et le log l'indique (`⏭ … fichiers partagés avec …`). Les deux partiront ensemble au `/cleanup` suivant, une fois indexés. `/cleanup` et `/restore` appliquent le même contrôle.

### Sync automatique

Toutes les 12h (configurable), le scanner reconstruit entièrement la DB locale :

1. Récupère les catégories qBit dynamiquement via l'API → save_paths
2. Scanne chaque dossier de catégorie pour indexer les fichiers vidéo
3. Scanne `/data/Multimedia/cross-seeds/` pour les hardlinks cross-seed
4. Scanne `/data/Media/` pour les hardlinks Sonarr/Radarr
5. Croise les inodes avec la liste des torrents qBit → peuple la DB
6. Marque `arr_managed` tout torrent hardlinké dans `/data/Media/`, et en retire ceux dont le torrent n'est plus dans qBittorrent

L'étape 6 est un filet contre les webhooks ratés — service arrêté pendant un import, type d'event non coché dans un *arr, livraison échouée. Un hardlink dans une racine *arr prouve l'import indépendamment de tout event reçu.

Le critère ne porte que sur `/data/Media/` : un torrent vivant uniquement sous `/data/Multimedia/` (ajout manuel) n'est jamais marqué, donc jamais candidat au `/cleanup`.

La purge, elle, se fonde sur la **liste des torrents qBittorrent**, pas sur la table `files` : un torrent bien présent dans le client mais dont les fichiers ont disparu du disque n'a aucune ligne dans `files` et serait retiré à tort. Sans cette purge, une release re-téléchargée plus tard reprendrait le même hash et serait considérée gérée par un *arr d'emblée, même ajoutée à la main.

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
| `CLEANUP_GRACE_HOURS` | `24` | Délai avant qu'un torrent terminé puisse être vu comme orphelin par `/cleanup`. `0` désactive |
| `VIDEO_EXTENSIONS` | voir ci-dessous | Extensions indexées, séparées par des virgules. Le point initial et la casse sont optionnels |
| `SONARR_URL` | — | URL Sonarr (requis pour `/bootstrap` et l'assistant d'import) |
| `SONARR_API_KEY` | — | Clé API Sonarr (Settings → General) |
| `RADARR_URL` | — | URL Radarr (requis pour `/bootstrap` et l'assistant d'import) |
| `RADARR_API_KEY` | — | Clé API Radarr (Settings → General) |
| `JELLYFIN_URL` | — | URL Jellyfin. Vide = suppressions Jellyfin désactivées |
| `JELLYFIN_API_KEY` | — | Clé API Jellyfin (Tableau de bord → Clés API) |
| `JELLYFIN_PATH_MAP` | `/Multimedia:/data/Multimedia` | Traduction des chemins vus par Jellyfin. Format `préfixe_jf:préfixe_local`, séparés par des virgules |
| `JELLYFIN_FALLBACK_MATCH` | `true` | Autorise la résolution par métadonnées quand l'item n'est pas encore indexé |
| `JELLYFIN_FALLBACK_MAX` | `10` | Nombre max de chemins que le fallback peut retenir pour un film/épisode |
| `JELLYFIN_FALLBACK_MAX_CONTAINER` | `500` | Idem pour une série/saison, qui compte légitimement beaucoup de fichiers |
| `JELLYFIN_DELEGATE_TO_ARR` | `false` | Traduit la suppression en action Sonarr/Radarr au lieu de déplacer les torrents directement |
| `JELLYFIN_ARR_DELETE_CONTAINERS` | `false` | Supprimer une série ou un film depuis Jellyfin retire l'entrée de Sonarr/Radarr, au lieu de seulement la dé-monitorer |
| `IMPORT_ASSIST_DRY_RUN` | `true` | Assistant d'import en simulation : il journalise ce qu'il ferait, sans agir. Seuls `0`, `false` ou `no` le font agir |
| `IMPORT_ASSIST_INTERVAL_MINUTES` | `360` | Passage de renfort de l'assistant d'import, pour les events manqués. `0` le désactive |
| `IMPORT_ASSIST_PACK_DRY_RUN` | `true` | Règle 4 de l'assistant (packs de saison partiels) en simulation, indépendamment de la règle 1. Seuls `0`, `false` ou `no` la font agir |
| `PROWLARR_URL` | — | URL Prowlarr, où la règle 4 cherche les packs complets. Vide = elle signale les packs partiels sans rien remplacer |
| `PROWLARR_API_KEY` | — | Clé API Prowlarr (Settings → General) |
| `IMPORT_ASSIST_ACCORD_DRY_RUN` | `true` | Règle 5 de l'assistant (accorder la demande Seerr et la saison Sonarr) en simulation. Seuls `0`, `false` ou `no` la font agir |
| `SEERR_URL` | — | URL Seerr, d'où la règle 5 lit les demandes. Vide = règle 5 désactivée |
| `SEERR_API_KEY` | — | Clé API Seerr (Paramètres → Général) |

---

## Configuration Sonarr / Radarr

### Sonarr — Settings → Connect → + → Webhook

```
Name    : cleanup-linker
URL     : http://cleanup-linker:5000/webhook?token=VOTRE_TOKEN
Method  : POST
Triggers: ✅ On File Import       ← indispensable : indexe chaque import sans attendre la sync
          ✅ On File Upgrade      ← signale le fichier remplacé, une fois le nouveau en place
          ✅ On Series Delete
          ✅ On Episode File Delete
          ❌ On Episode File Delete For Upgrade   ← laisser décoché (voir Limites connues)
          ❌ On Import Complete   ← inutile : doublon de On File Import
          ✅ On Manual Interaction Required   ← déclenche l'assistant d'import
          ✅ On Grab              ← règle 4 : vérifie chaque pack de saison dès son grab
```

Dans Sonarr, « On File Upgrade » n'envoie rien si « On File Import » n'est pas coché : c'est un filtre de cet event. Et « On Import Complete » ne dit pas quels fichiers un upgrade a remplacés.

### Radarr — Settings → Connect → + → Webhook

```
Name    : cleanup-linker
URL     : http://cleanup-linker:5000/webhook?token=VOTRE_TOKEN
Method  : POST
Triggers: ✅ On File Import       ← indispensable : indexe chaque import sans attendre la sync
          ✅ On File Upgrade      ← signale le fichier remplacé, une fois le nouveau en place
          ✅ On Movie Delete
          ✅ On Movie File Delete
          ❌ On Movie File Delete For Upgrade   ← laisser décoché (voir Limites connues)
          ✅ On Manual Interaction Required   ← déclenche l'assistant d'import
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

La requête passe `CollapseBoxSetItems=false`. Sans ce paramètre, Jellyfin remplace les films rangés dans une collection par la collection elle-même, et ces films échappent à l'index. Sur l'installation de référence, il en manquait 189 sur 1654, et les supprimer depuis Jellyfin restait sans effet.

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

### Les trois garde-fous

`ItemDeleted` ne se déclenche pas seulement sur une suppression volontaire : il part dès qu'un item quitte la base Jellyfin — refresh de métadonnées, réidentification, **retrait d'une bibliothèque**, **changement de bibliothèque**. Dans ces cas le fichier existe toujours, parfois sous un autre nom.

1. **Le hardlink a-t-il vraiment disparu ?** Un `os.stat()` sur le chemin tagué. S'il répond, l'event est ignoré. C'est ce qui empêche un retrait de bibliothèque de balancer toute la collection vers `cleanuparr-unlinked`.
2. **Le fichier a-t-il seulement changé de nom ?** Déplacer un titre vers une autre bibliothèque fait disparaître l'ancien chemin exactement comme une suppression, qu'on ait changé son dossier racine dans Sonarr/Radarr ou sa catégorie dans qBittorrent. L'index ne connaîtra le nouveau chemin qu'à la sync suivante. C'est le compteur de liens de l'inode qui tranche : une suppression le fait baisser, un déplacement le laisse intact. Si `st_nlink` dépasse le nombre de liens connus encore présents, le fichier vit ailleurs et le chemin est ignoré. Quand plus aucun lien connu ne subsiste (torrent et hardlink déplacés tous les deux), l'inode est cherché à l'emplacement actuel des torrents. Si qBittorrent ne répond pas, rien n'est fait. Sans ce garde-fou, avec la délégation activée, une série changée de bibliothèque était supprimée de Sonarr.
3. **Le torrent sert-il encore ?** Garde-fou existant, partagé avec Sonarr/Radarr : un pack de saison dont d'autres épisodes sont encore hardlinkés n'est pas déplacé.

### Séries et saisons

Supprimer une série entière depuis Jellyfin **n'émet qu'un seul event**, sur le conteneur — jamais un event par épisode (vérifié en production : une suppression de 14 épisodes n'a produit qu'un `ItemDeleted` de type `Series`).

Une `Series` ou une `Season` étant un dossier, elle n'a pas de ligne dans `files`. On remonte donc à ses fichiers par les colonnes `jellyfin_series_id` / `jellyfin_season_id`, renseignées sur chaque épisode pendant l'indexation (l'API Jellyfin renvoie nativement `SeriesId` et `SeasonId`).

Les chemins retenus sont ensuite ramenés à leur **dossier commun**, traité en une seule passe par la branche préfixe de `move_torrents_for_path` — une seule session qBittorrent au lieu d'une par épisode. Si ce dossier commun remonte trop haut (moins de 5 segments, c'est-à-dire une bibliothèque entière comme `/data/Media/Séries`), il est rejeté et chaque chemin est traité séparément. C'est le cas quand Jellyfin réunit deux dossiers d'une même bibliothèque sous une seule série : la branche préfixe ratisserait alors toute la bibliothèque.

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

#### Bibliothèque posée sur le dossier de téléchargement

Quand une bibliothèque Jellyfin pointe directement sur un dossier de catégorie qBittorrent — `Films` sur `/data/Multimedia/Films` par exemple — Jellyfin et Radarr désignent le même fichier par deux chemins différents :

```
Jellyfin : /data/Multimedia/Films/Cypher.2002.Multi.Bluray.Remux....mkv
Radarr   : /data/Media/Films/Cypher (2002) [tmdbid-10133]/Cypher (2002) ... .mkv
           └─ même inode : le hardlink créé par Radarr à l'import
```

Comparer le seul chemin Jellyfin aux dossiers Radarr ne peut pas aboutir. Avant le correctif, la suppression retombait sur le déplacement direct des torrents, et Radarr gardait sa copie, devenue invisible mais toujours `hasFile=true`. Sur l'installation de référence, 4 films sont restés ainsi, soit 90 Go, et aucune suppression de film depuis Jellyfin n'avait jamais été déléguée.

Les chemins supprimés sont donc d'abord complétés par **tous les autres chemins du même inode** connus de la base. Ils y sont encore au moment de l'event, car la sync ne purge un chemin disparu qu'au passage suivant. L'identification par préfixe trouve alors le film par son chemin `/data/Media`.

Un lien n'est retenu que si **son inode sur le disque est toujours celui de la base**. Un fichier remplacé depuis la dernière sync, par un upgrade par exemple, est un autre fichier : une suppression Jellyfin ne doit pas l'emporter. Et quand Radarr détient une autre release que celle supprimée, aucun chemin `/data/Media` ne partage l'inode du fichier supprimé. Rien n'est délégué, et la release de Radarr reste en place.

L'ordre compte : on dé-monitore **avant** de supprimer, sinon l'*arr peut relancer une recherche entre les deux appels et retélécharger ce qu'on vient de retirer.

Le dé-monitorage passe par les endpoints à charge minimale — `episode/monitor`, `series/editor`, `movie/editor` — jamais par un `PUT` sur la ressource, qui exigerait de renvoyer l'objet complet et donc de risquer d'abîmer une fiche sur un champ mal repris.

| Event Jellyfin | Action *arr |
|---|---|
| `Episode` | Dé-monitore l'épisode, supprime son `episodefile` |
| `Season` | Dé-monitore les épisodes de la saison, supprime leurs fichiers |
| `Series` | Idem + dé-monitore la série elle-même |
| `Movie` | Dé-monitore le film, supprime son `moviefile` |

L'entrée reste dans Sonarr/Radarr avec son historique et ses réglages — seul le monitoring bascule.

### Supprimer l'entrée plutôt que la dé-monitorer

Une série vidée mais conservée reste indéfiniment dans Sonarr, à 0 fichier, en affichant ses épisodes manquants. `JELLYFIN_ARR_DELETE_CONTAINERS=true` retire l'entrée pour les **conteneurs uniquement** :

| Event Jellyfin | Action |
|---|---|
| `Series` | `DELETE /series/{id}?deleteFiles=true` |
| `Movie` | `DELETE /movie/{id}?deleteFiles=true` |
| `Season`, `Episode` | inchangé — supprimer une saison ne doit pas retirer la série |

Effet secondaire appréciable : Sonarr n'émet plus qu'un seul `SeriesDelete`, résolu par préfixe de dossier, au lieu d'un `EpisodeFileDelete` par épisode qui déplaçait le même torrent autant de fois.

Désactivé par défaut : c'est irréversible côté *arr — on perd l'historique, le profil de qualité, les tags et le monitoring. **À vérifier avant d'activer** : si tu utilises des listes d'import (Trakt, IMDb…), la série sera ré-ajoutée au prochain sync, sauf à gérer les exclusions.

**Repli** : si le média n'est géré par aucun *arr (typiquement une bibliothèque Jellyfin pointant directement sur un dossier de téléchargement), on retombe sur le déplacement direct des torrents. Idem si l'*arr est injoignable — une erreur d'API n'interrompt jamais le traitement.

### Tester sans rien déplacer

```bash
# Ajoute &dry_run=true à l'URL du webhook, ou rejoue un payload à la main
curl -XPOST 'http://192.168.1.111:5001/jellyfin?token=VOTRE_TOKEN&dry_run=true' \
  -H 'Content-Type: application/json' \
  -d '{"NotificationType":"ItemDeleted","ItemType":"Episode","ItemId":"..."}'
```

---

## Assistant d'import

Un téléchargement terminé que Sonarr ou Radarr n'arrive pas à importer reste dans leur file, à l'état `importBlocked`. L'*arr attend une intervention, et tant que le téléchargement y reste, il refuse les autres releases des mêmes épisodes. Dès que l'*arr signale un blocage, l'assistant examine les téléchargements bloqués, et règle ceux qui peuvent l'être sans risque.

**Simulation par défaut** : il journalise ce qu'il ferait (`[SIMULATION] ✓ …`), sans rien changer. `IMPORT_ASSIST_DRY_RUN=false` le fait agir.

### Règle 1 : déjà importé, mais resté coincé

Chaque vidéo du torrent a déjà un hardlink dans le dossier du média du grab. La comparaison se fait par inode, sans aucun nom de fichier. Les échantillons ne comptent pas : « sample » dans le nom, et moins de 300 Mo. Cas typiques :
- un pack « S01 » qui n'apporte qu'une partie de la saison TVDB, comme une partie Netflix d'un anime. L'*arr a importé ce qu'il contenait, mais attend les épisodes manquants et garde le téléchargement ;
- un import fait à la main, sans rattacher le téléchargement.

L'assistant :
1. passe le torrent dans la catégorie après import de son client, celle que l'*arr aurait posée lui-même. Le torrent sort de la file et continue de seeder ;
2. pour Sonarr, lance la recherche des épisodes que la release devait apporter et qui manquent encore, s'ils sont surveillés et déjà diffusés. Au-delà de 25 épisodes, la recherche est laissée à la main : Sonarr interroge les indexeurs épisode par épisode.

Il ne fait rien si :
- une seule vidéo manque dans la bibliothèque, ou n'y figure que sous un autre média ;
- le torrent est incomplet, absent de qBittorrent, ou si ses données sont introuvables sur le disque ;
- le client n'a pas de catégorie après import, si celle-ci est la catégorie actuelle (le torrent ne sortirait pas de la file), ou si elle a un autre dossier : la règle ne déplace jamais rien sur le disque ;
- il a déjà agi sur ce téléchargement dans les dernières 24 h.

Rien n'est importé ni supprimé. Un téléchargement en erreur n'empêche pas de traiter les suivants. Si Sonarr refuse la recherche après le changement de catégorie, le verdict `search-failed` le signale : la recherche est alors à lancer depuis Sonarr. Chaque verdict est journalisé une fois par jour, et se relit avec `/assist/history` :

```
INFO   [SIMULATION] ✓ [sonarr] Kengan.Ashura.S01.MULTI.VFF.1080p.WEBRip.EAC3.2.0.x265-SQUEEZE : 12/12 vidéo(s) déjà dans la bibliothèque → [seed] Séries animation, recherche de 12 épisode(s) manquant(s)
INFO   ⏭ [SIMULATION] [sonarr] Veronica.Mars.iNTEGRALE.MULTi.WEBRiP.1080p.x265-SN2P : 0/72 vidéo(s) dans la bibliothèque
```

### Règle 4 : pack de saison partiel

Sonarr prend tout pack « S01 » pour la saison entière. Un pack qui n'en contient qu'une partie passe donc pour complet : une moitié de saison TVDB, comme une partie Netflix d'un anime, ou une « Part 1 ». Un vrai pack complet est ensuite refusé à chaque recherche, parce qu'il n'améliore pas les épisodes déjà là.

**Au grab.** Dès que Sonarr prend un pack de saison, l'assistant lit sa liste de fichiers dans qBittorrent, qui a le `.torrent` dès le grab. Chaque vidéo passe par le parseur de Sonarr, celui qui servira à l'import. S'il manque au pack des épisodes diffusés, surveillés et sans fichier, l'assistant :
1. arrête le torrent, avant qu'il ait eu le temps de télécharger ;
2. cherche un pack complet de la saison dans Prowlarr ;
3. s'il en trouve un, l'ajoute dans qBittorrent, dans la catégorie du client Sonarr et sous le titre de sa release : Sonarr le reconnaît par ce titre et l'importe comme les siens ;
4. blackliste ensuite le pack partiel depuis la file de Sonarr, sans le supprimer du client ni relancer de recherche, puis le met en corbeille, arrêté : à peine entamé, il n'a rien à seeder.

Sans pack complet, le pack partiel repart : mieux vaut une demi-saison que rien.

Un pack complet doit :
- apporter tous les épisodes manquants, d'après la liste des fichiers de son `.torrent` ;
- être un pack de la même saison, ni multi-saisons, ni « Part » ;
- porter le même titre de série que le pack partiel. Une série d'anthologie rassemble sous une fiche TVDB des histoires que leurs releases nomment chacune « S01 ». Ainsi, « Monster: The Lizzie Borden Story » est la saison 4 de « Monster » pour TVDB, mais Sonarr la range en saison 1, celle de Dahmer, qui a 10 épisodes au lieu de 8. Le « S01 complet » serait une autre histoire que celle demandée ;
- avoir une qualité autorisée par le profil de la série, au moins aussi bonne que celle du pack partiel ;
- avoir un score de formats au moins égal : une release française n'est jamais remplacée par une release qui ne l'est pas ;
- avoir au moins 3 sources et un lien `.torrent`. Un magnet n'a pas de liste de fichiers à lire avant d'être ajouté.

Les mieux classés passent d'abord : score, puis qualité, puis sources. Seuls les `.torrent` des 4 premiers sont lus, car chaque lecture passe par un tracker.

Garde-fous :
- **Hit-and-run :** un torrent privé déjà téléchargé à plus de 2 % n'est jamais arrêté. On le laisse finir, et la règle reprend après l'import.
- **Pas de boucle :** au plus 2 remplacements par saison et par semaine. Au-delà, la saison revient à la main.
- **Dossier partagé :** un pack complet publié sous le même nom de dossier que le partiel en partage les fichiers. Le partiel reste alors arrêté dans sa catégorie, parce que la corbeille a son propre dossier : ATM y emporterait les fichiers de l'autre.
- **Vérification interrompue :** si le service redémarre pendant la recherche, le passage de renfort relance le torrent arrêté, passé 30 minutes.

**Après l'import, en filet.** Un pack partiel peut quand même être importé : pris pendant un arrêt du service, magnet sans liste de fichiers au grab, ou torrent privé trop entamé. La règle 1 le trouve alors bloqué dans la file. Au lieu de lancer la recherche des épisodes manquants un à un, l'assistant ajoute un pack complet, choisi de la même façon. Une fois ce pack téléchargé, Sonarr importe seul les épisodes manquants, mais refuse de remplacer ceux du pack partiel, faute de les améliorer. L'assistant les importe alors à la main, en hardlink, à la place des fichiers du pack partiel — et de ceux-là seulement : un épisode remplacé entre-temps par une autre release n'est pas touché. Le pack partiel, qui n'a plus de fichier dans la bibliothèque, part en corbeille par l'event d'upgrade.

La règle 4 a **sa propre simulation**, active par défaut et indépendante de la règle 1 : `IMPORT_ASSIST_PACK_DRY_RUN=false` la fait agir. Elle demande « On Grab » coché dans le webhook de Sonarr. Sans `PROWLARR_URL` et `PROWLARR_API_KEY`, elle signale les packs partiels sans y toucher. La saison 0 (épisodes spéciaux) n'est pas traitée.

Verdicts : `pack-ok`, `pack-partial` (pack gardé), `pack-replace`, `pack-reimport` (fin d'un remplacement après import), `pack-skipped` et `pack-error`. `pack-stopped` et `pack-resumed` tracent l'arrêt d'un torrent et sa relance.

```
INFO   [SIMULATION] ✓ [sonarr] Kengan.Ashura.S01.MULTI.VFF.1080p.WEBRip.EAC3.2.0.x265-SQUEEZE : pack partiel : 12/24 épisodes attendus de la saison 1 → remplacé par Kengan.Ashura.S01.MULTi.VFF.1080p.WEBRiP.EAC3.2.0.x265-T3KASHi (C411, 15 sources, score 195)
INFO   [SIMULATION] ⏭ [sonarr] Ranma ep 15-21 : pas un pack de saison
```

### Règle 5 : accorder la demande Seerr et la saison Sonarr

Seerr demande des saisons numérotées comme TMDB, Sonarr les range comme TVDB, et les deux ne s'accordent pas toujours. « Monster: The Lizzie Borden Story » est une mini-série d'une saison pour TMDB, mais la saison 4 de « Monster » pour TVDB. Seerr a demandé la saison 1, et Sonarr y a rangé les 8 épisodes de Lizzie Borden, sous les titres de Dahmer, qui en compte 10.

C'est la demande qui tranche. À chaque passage, avant les autres règles, l'assistant relit les demandes de séries Seerr des 30 derniers jours, approuvées ou terminées. Pour chaque saison demandée, il compare ses épisodes TMDB avec ceux de la saison TVDB de même numéro : nombre, dates de diffusion, titres.
- Si elles collent, rien à faire.
- Sinon, il cherche la seule autre saison TVDB qui colle : toutes les dates identiques, ou au moins 80 % des titres (les épisodes à venir n'ont pas de date). Pour Lizzie Borden, c'est la S04, avec 8/8 dates et 8/8 titres, alors que la S03 a aussi 8 épisodes mais aucun en commun.
- Si aucune saison ne colle, ou si plusieurs collent, il le signale et ne touche à rien.

Quand une saison est trouvée, l'assistant :
1. surveille la saison TVDB trouvée ;
2. reprend les fichiers de la demande rangés dans la saison demandée, ceux dont la release porte le titre de la fiche demandée. Il les réaffecte aux épisodes de même numéro de la bonne saison par un import manuel sur place, puis les range par un renommage. Rien n'est copié ni supprimé, et le torrent reste lié. C'est tout ou rien : si un épisode cible a déjà un fichier, aucun fichier ne bouge ;
3. cesse de surveiller la saison demandée, sauf si une autre demande la vise ou si elle garde des fichiers d'une autre release.

La règle 1 ne cherche plus les épisodes manquants de la saison écartée, même en simulation. La règle 4 n'y cherche pas non plus de pack complet.

Elle a **sa propre simulation**, active par défaut : `IMPORT_ASSIST_ACCORD_DRY_RUN=false` la fait agir. Elle demande `SEERR_URL` et `SEERR_API_KEY`. Verdicts : `accord-fix` et `accord-unknown`, journalisés sous `seerr-<demande>-s<saison>`.

Limites :
- Seerr peut repasser la demande en « partiellement disponible » à son prochain scan, une fois la saison demandée vidée.
- Une saison TMDB qui couvre une partie d'une saison TVDB (les parties d'un anime) n'est pas encore accordée.
- La règle n'aide pas Sonarr à trouver une release d'anthologie nommée « S01 » pour une saison 4 : elle range ce qu'il a déjà pris.

```
INFO   [SIMULATION] ✓ [sonarr] Monster (2022) S01 (demande Seerr 912, Hind) : saison 1 demandée = S04 TVDB (8/8 dates et 8/8 titres identiques) → S04 surveillée, S01 plus surveillée, 8 fichier(s) réaffecté(s) de S01 à S04
INFO   [SIMULATION] ✓ [sonarr] Monster.The.Lizzie.Borden.Story.S01...-BYOR : 8/8 vidéo(s) déjà dans la bibliothèque → [seed] Rays_Le _Chat (règle 5 : la saison 1 demandée est une autre saison TVDB, ses épisodes manquants ne sont pas cherchés)
```

### Déclenchement

- **L'event « On Manual Interaction Required »**, à cocher dans le webhook de Sonarr et de Radarr. L'*arr l'émet au moment où il bloque un import, dans trois cas : titre non reconnu, grab rattaché par ID, ou épisodes attendus manquants. L'assistant passe deux minutes plus tard, le temps que la file affiche l'état `importBlocked`. Les events reçus entre-temps se regroupent dans ce passage.
- **Un passage de renfort**, toutes les `IMPORT_ASSIST_INTERVAL_MINUTES` (6 h par défaut, `0` le désactive), et deux minutes après chaque démarrage. L'*arr n'émet l'event qu'une fois par téléchargement, et l'oublie quand il redémarre : ce passage rattrape les events manqués.
- **`POST /assist?token=…`** lance un passage tout de suite et renvoie les verdicts. `&dry_run=false` agit même si le service est en simulation ; `&dry_run=true` simule même s'il est actif. Seules les valeurs `0`, `false` et `no` font agir : une valeur mal saisie reste une simulation.

Un seul passage tourne à la fois, quel que soit le process gunicorn qui le lance. Pendant un passage, `/assist` répond `409`. Un passage programmé par un event qui trouve le verrou pris est reprogrammé, au lieu d'être perdu : une recherche de pack complet peut tenir le verrou plusieurs minutes.

La règle 4 a son propre déclenchement au grab : l'event « On Grab » de Sonarr lance tout de suite la vérification du pack, en arrière-plan, sans attendre de passage.

Limite : le passage programmé par un event vit dans le worker gunicorn qui a reçu l'event. Si ce worker redémarre dans les deux minutes, le passage est perdu, et le téléchargement attend le passage de renfort suivant.

---

## Événements gérés

### Sonarr

| Event | Déclencheur | Action |
|---|---|---|
| `EpisodeFileDelete` | Suppression manuelle d'un épisode | Move vers cleanuparr-unlinked. Ignoré si le fichier est toujours sur le disque : il a été réaffecté (`manualOverride`, règle 5) |
| `SeriesDelete` | Suppression d'une série entière | Move vers cleanuparr-unlinked |
| `Download` isUpgrade=false | Import normal (On File Import) | Marqué arr_managed, fichier importé indexé tout de suite |
| `Download` isUpgrade=true | Upgrade (On File Upgrade) | Nouveau fichier indexé, puis chaque `deletedFiles` traité comme une suppression |
| `Download` avec `episodeFiles` | On Import Complete, s'il est coché | Fichiers importés indexés |
| `EpisodeFileDelete` deleteReason=upgrade | On Episode File Delete For Upgrade, s'il est coché | Move vers cleanuparr-unlinked, avant l'import du nouveau fichier |
| `ManualInteractionRequired` | Import bloqué (On Manual Interaction Required) | Passage de l'assistant d'import sous deux minutes |
| `Grab` | Release prise (On Grab) | Marqué arr_managed ; pour un pack de saison, vérification de la règle 4 en arrière-plan |
| `Test` | Test webhook | Répond OK |

### Radarr

| Event | Déclencheur | Action |
|---|---|---|
| `MovieFileDelete` | Suppression manuelle d'un film | Move vers cleanuparr-unlinked. Ignoré si le fichier est toujours sur le disque |
| `MovieDelete` | Suppression d'un film entier | Move vers cleanuparr-unlinked |
| `Download` isUpgrade=false | Import normal (On File Import) | Marqué arr_managed, fichier importé indexé tout de suite |
| `Download` isUpgrade=true | Upgrade (On File Upgrade) | Nouveau fichier indexé, puis chaque `deletedFiles` traité comme une suppression |
| `MovieFileDelete` deleteReason=upgrade | On Movie File Delete For Upgrade, s'il est coché | Move vers cleanuparr-unlinked, avant l'import du nouveau fichier |
| `ManualInteractionRequired` | Import bloqué (On Manual Interaction Required) | Passage de l'assistant d'import sous deux minutes |
| `Test` | Test webhook | Répond OK |

### Jellyfin

Route dédiée `/jellyfin` — le payload Jellyfin n'a pas le même format que celui des *arrs.

| Event | ItemType | Action |
|---|---|---|
| `ItemDeleted` | `Movie`, `Episode`, `Video` | Résout `ItemId` → chemin → inode, puis move vers cleanuparr-unlinked |
| `ItemDeleted` | `Series`, `Season` | Résout via `jellyfin_series_id` / `jellyfin_season_id` → dossier commun → move en une passe |
| Autre | — | Ignoré |

Les moves sont tracés dans `cleanup_log` avec `source = "jellyfin"`, donc visibles dans `/history` et réversibles via `/restore?since=…`.

---

## Endpoints API

| Endpoint | Méthode | Auth | Description |
|---|---|---|---|
| `/webhook?token=...` | POST | Token | Reçoit les events Sonarr/Radarr |
| `/sync?token=...` | POST | Token | Force une resync manuelle immédiate |
| `/bootstrap?token=...` | POST | Token | Peuple arr_managed depuis l'historique *arr |
| `/cleanup?token=...` | POST | Token | Déplace les torrents *arr orphelins |
| `/restore?token=...` | POST | Token | Restaure les torrents du dernier `/cleanup` (param `since` pour élargir) |
| `/history?token=...` | GET | Token | Historique des moves (param `limit`, défaut 50) |
| `/stats?token=...` | GET | Token | Statistiques de la DB |
| `/jellyfin?token=...` | POST | Token | Reçoit les events Jellyfin (param `dry_run`) |
| `/assist?token=...` | POST | Token | Lance un passage de l'assistant d'import et renvoie ses verdicts (param `dry_run`) |
| `/assist/history?token=...` | GET | Token | Derniers verdicts de l'assistant d'import (param `limit`, défaut 50) |
| `/jellyfin/index?token=...` | POST | Token | Force la réindexation Jellyfin |
| `/health` | GET | Non | Healthcheck |

---

## Workflow premier démarrage

Le `/cleanup` repose sur la table `arr_managed` pour ne jamais toucher les torrents ajoutés manuellement.

En régime établi elle s'entretient seule : chaque webhook enregistre son `downloadId`, et chaque sync marque les torrents hardlinkés dans `/data/Media/`. `/bootstrap` ne sert donc qu'une fois — après une première installation, ou si le service a été absent longtemps — pour récupérer l'historique Sonarr/Radarr des imports antérieurs, que la DB locale ne peut pas reconstituer.

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

Si des torrents ont été déplacés à tort vers `cleanuparr-unlinked`, l'endpoint `/restore` permet de les remettre en place. Il utilise la table `cleanup_log` comme source principale — **il fonctionne donc même après une resync**.

Par défaut, il ne touche qu'aux torrents du **dernier `/cleanup`** :

```bash
curl -XPOST 'http://192.168.1.111:5001/restore?token=VOTRE_TOKEN'
```

Pour défaire aussi les moves venus des webhooks ou de Jellyfin, `since` (timestamp Unix) restaure tout ce qui a été tracé depuis cette date :

```bash
curl -XPOST "http://192.168.1.111:5001/restore?token=VOTRE_TOKEN&since=$(date -d '2 hours ago' +%s)"
```

Sans ce bornage, tout ce qui est dans la catégorie cible et figure au journal repartirait : des mois de suppressions légitimes, plus les torrents qu'on y laisse seeder pour honorer les trackers.

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

**Chemin non trouvé en DB** — La sync ne passe que toutes les 12 h. Pour qu'un fichier supprimé peu après son import soit retrouvé, l'event `Download` de Sonarr/Radarr l'indexe tout de suite. Cleanup-linker rattache alors le fichier importé au torrent dont il est un hardlink, à partir de son inode et du `downloadId`.

Le message reste possible dans trois cas :
- **Import non signalé :** le webhook ne coche pas « On File Import ».
- **Fichier hors import :** copie manuelle, ou copie au lieu d'un hardlink.
- **qBittorrent injoignable** au moment de l'import.

La suppression ne déplace alors rien. Le torrent devenu orphelin attend le prochain `/cleanup`.

Côté Jellyfin, un média ajouté depuis la dernière sync reste `unresolved` : son `ItemId` n'est connu qu'à l'indexation suivante. `/jellyfin/index` force cette indexation.

**Torrents multi-saisons** — Certains torrents qBit regroupent plusieurs saisons sous un même nom de dossier. L'association inode peut être incomplète dans ces cas.

**Upgrade vers la même release** — Pendant un upgrade, Sonarr et Radarr suppriment l'ancien fichier, puis créent le hardlink du nouveau. « Delete For Upgrade » part entre les deux. Une même release prise sur un autre tracker partage les fichiers de l'ancien torrent. Si cross-seed l'a déjà enregistrée, elle porte le même nom : elle entre dans le lot, les deux torrents partent en corbeille avec les fichiers, et l'import échoue. « On File Upgrade » part après le hardlink. Le nouveau fichier est alors indexé d'abord, puis la vérification des hardlinks garde les deux torrents. C'est pourquoi la configuration ci-dessus laisse « Delete For Upgrade » décoché. Dans Sonarr, cela suppose « On File Import » coché : sans lui, plus rien ne signalerait les fichiers remplacés, et l'ancien torrent resterait hors de la corbeille.

**Lien apparu depuis la sync** — Un hardlink créé depuis la dernière sync, comme un cross-seed injecté entre-temps, fait passer une vraie suppression Jellyfin pour un déplacement. Elle est ignorée et journalisée comme telle (`⏭ Ignoré : … déplacés`), et il faut alors finir la suppression côté Sonarr/Radarr. C'est le sens prudent : rien n'est supprimé à tort.

**Emplacement d'un torrent** — Le champ `name` de l'API qBittorrent est un nom d'affichage, qui diffère du dossier réel dès que le `.torrent` porte un titre différent de son contenu. C'est `content_path` qui fait foi, et c'est lui qu'utilise `torrent_root()`. Sur l'installation de référence, 308 torrents sur 3269 sont dans ce cas : reconstruire le chemin avec `save_path + name` les rendait introuvables, donc non indexés — leurs fichiers apparaissaient alors comme n'appartenant à aucun torrent.

**Extensions vidéo** — Par défaut : `.mkv`, `.mp4`, `.avi`, `.ts`, `.m2ts`, `.mov`, `.wmv`, `.flv`, `.iso`, `.m4v`.

Un fichier dont l'extension ne figure pas dans cette liste est **invisible pour tout le service** : son torrent ne sera déplacé ni par un webhook, ni par `/cleanup`, sans message d'erreur explicite — seulement un `Chemin non trouvé en DB` ou un `unresolved`. Si tu constates ça sur un média qui existe bien, vérifie son extension avant tout le reste.

La liste s'ajuste sans rebuild via `VIDEO_EXTENSIONS` :

```yaml
- VIDEO_EXTENSIONS=.mkv,.mp4,.avi,.ts,.m2ts,.mov,.wmv,.flv,.iso,.m4v,.webm
```
