"""
La porte Jellyfin : une suppression vue par Jellyfin, traduite en action Sonarr/Radarr. Vrais fichiers et vrais
hardlinks dans un dossier jetable, index rempli à la main, route /jellyfin appelée avec un faux Sonarr et un faux
Radarr qui notent chaque appel. Le cas de l'incident Westworld (2026-11-12) en tête.
"""
import os
import time
from contextlib import contextmanager

import pytest

import app


@pytest.fixture
def porte(monkeypatch):
    """Porte Jellyfin active, délégation et suppression des conteneurs comprises ; index vide ; appels *arr notés."""
    with app.get_db() as conn:
        conn.execute("DELETE FROM files")
    appels, gestes = [], []
    etat = {"series": [], "episodefiles": [], "movies": [], "bibliotheques": set()}

    def faux_arr(method, base_url, api_key, endpoint, timeout=30, retries=3, **kwargs):
        appels.append((method, endpoint))
        if method == "GET" and endpoint == "series":
            return etat["series"]
        if method == "GET" and endpoint == "episodefile":
            return etat["episodefiles"]
        if method == "GET" and endpoint == "episode":
            return [{"id": 100 + f["id"], "episodeFileId": f["id"]} for f in etat["episodefiles"]]
        if method == "GET" and endpoint == "movie":
            return etat["movies"]
        return None  # PUT et DELETE : seulement notés

    monkeypatch.setattr(app, "_arr_request", faux_arr)
    monkeypatch.setattr(app, "_jellyfin_libraries", lambda: etat["bibliotheques"])
    monkeypatch.setattr(app, "_delete_gesture", lambda chemins, *a, **k: gestes.append(list(chemins)) or 0)
    for nom, valeur in {"JELLYFIN_URL": "http://jellyfin.invalid", "JELLYFIN_API_KEY": "tests",
                        "JELLYFIN_DELEGATE_TO_ARR": True, "JELLYFIN_ARR_DELETE_CONTAINERS": True,
                        "SONARR_URL": "http://sonarr.invalid", "SONARR_API_KEY": "tests",
                        "RADARR_URL": "http://radarr.invalid", "RADARR_API_KEY": "tests"}.items():
        monkeypatch.setattr(app, nom, valeur)
    return {"appels": appels, "gestes": gestes, "etat": etat}


def fichier(chemin, contenu=b"video"):
    os.makedirs(os.path.dirname(chemin), exist_ok=True)
    with open(chemin, "wb") as f:
        f.write(contenu)
    return chemin


def lien(source, cible):
    os.makedirs(os.path.dirname(cible), exist_ok=True)
    os.link(source, cible)
    return cible


def indexer(chemin, torrent=None, item=None, serie=None):
    with app.get_db() as conn:
        conn.execute("INSERT INTO files (inode, path, torrent_hash, stale, jellyfin_item_id, jellyfin_series_id) "
                     "VALUES (?, ?, ?, 0, ?, ?)", (os.stat(chemin).st_ino, chemin, torrent, item, serie))


def evenement(item_type, item_id, nom="test"):
    client = app.app.test_client()
    rep = client.post("/jellyfin?token=tests", json={"NotificationType": "ItemDeleted", "ItemType": item_type,
                                                     "ItemId": item_id, "Name": nom})
    assert rep.status_code == 200
    return rep.get_json()


def suppressions(appels):
    return [e for m, e in appels if m == "DELETE"]


def westworld(tmp_path, porte):
    """Deux copies d'une série, sur les mêmes inodes, chacune vue par sa bibliothèque Jellyfin ; Sonarr gère « Séries »."""
    torrent = fichier(str(tmp_path / "Multimedia/Séries/Westworld.S01E01.mkv"))
    meuh = lien(torrent, str(tmp_path / "Media/meuhmeuh/Westworld (2016)/Season 01/S01E01.mkv"))
    series = lien(torrent, str(tmp_path / "Media/Séries/Westworld (2016)/Season 01/S01E01.mkv"))
    indexer(torrent, torrent="t1")
    indexer(meuh, item="ep-meuh", serie="serie-meuh")
    indexer(series, item="ep-series", serie="serie-series")
    porte["etat"]["bibliotheques"] = {str(tmp_path / "Media/meuhmeuh"), str(tmp_path / "Media/Séries")}
    porte["etat"]["series"] = [{"id": 217, "title": "Westworld", "path": str(tmp_path / "Media/Séries/Westworld (2016)")}]
    porte["etat"]["episodefiles"] = [{"id": 1, "path": series}]
    os.remove(meuh)  # le doublon est retiré, la copie de « Séries » reste
    return series


def test_westworld_le_doublon_retire_ne_supprime_pas_la_serie(tmp_path, porte):
    restee = westworld(tmp_path, porte)
    rep = evenement("Series", "serie-meuh", "Westworld")
    assert suppressions(porte["appels"]) == []
    assert rep.get("status") != "delegated"
    # Retour au geste direct, sur le seul chemin supprimé : la corbeille globale garde ce qui a un autre nom.
    assert porte["gestes"] == [[str(tmp_path / "Media/meuhmeuh/Westworld (2016)/Season 01/S01E01.mkv")]]
    assert os.path.exists(restee)


def test_westworld_par_episode_aucun_fichier_vivant_supprime(tmp_path, porte, monkeypatch):
    monkeypatch.setattr(app, "JELLYFIN_ARR_DELETE_CONTAINERS", False)
    westworld(tmp_path, porte)
    rep = evenement("Episode", "ep-meuh", "Westworld")
    assert suppressions(porte["appels"]) == []
    assert rep.get("status") != "delegated"


def test_doublon_gere_des_deux_cotes_seule_la_fiche_retiree_part(tmp_path, porte):
    """
    Deux fiches Sonarr sur les mêmes inodes, chacune vue par Jellyfin. La copie retirée est la plus courte : l'ancienne
    porte retenait la fiche au dossier le plus long, celle de la copie restée visible, et la supprimait.
    """
    torrent = fichier(str(tmp_path / "Multimedia/Séries animation/DBK.001.mkv"))
    longue = lien(torrent, str(tmp_path / "Media/Séries animation/Dragon Ball Kai Integrale Collector/DBK.001.mkv"))
    courte = lien(torrent, str(tmp_path / "Media/Séries animation/DBK/DBK.001.mkv"))
    indexer(torrent, torrent="t7")
    indexer(longue, item="ep-longue", serie="serie-longue")
    indexer(courte, item="ep-courte", serie="serie-courte")
    porte["etat"]["bibliotheques"] = {str(tmp_path / "Media/Séries animation")}
    porte["etat"]["series"] = [
        {"id": 152, "title": "Copie visible", "path": str(tmp_path / "Media/Séries animation/Dragon Ball Kai Integrale Collector")},
        {"id": 153, "title": "Copie retirée", "path": str(tmp_path / "Media/Séries animation/DBK")},
    ]
    os.remove(courte)
    rep = evenement("Series", "serie-courte", "DBK")
    assert suppressions(porte["appels"]) == ["series/153"]
    assert rep["status"] == "delegated"
    assert os.path.exists(longue)


def test_film_supprime_dans_films_atteint_toujours_radarr(tmp_path, porte):
    """« Films » lit le dossier du torrent ; la copie de Radarr, invisible pour Jellyfin, est retrouvée par l'inode."""
    torrent = fichier(str(tmp_path / "Multimedia/Films/Un.Film.2020.1080p/Un.Film.2020.1080p.mkv"))
    radarr = lien(torrent, str(tmp_path / "Media/Films/Un Film (2020)/Un Film (2020).mkv"))
    indexer(torrent, torrent="t2", item="film-1")
    indexer(radarr)
    porte["etat"]["bibliotheques"] = {str(tmp_path / "Multimedia/Films")}
    porte["etat"]["movies"] = [{"id": 9, "title": "Un Film", "path": str(tmp_path / "Media/Films/Un Film (2020)"),
                                "movieFile": {"id": 5, "path": radarr}}]
    os.remove(torrent)  # Jellyfin a supprimé le fichier qu'il voyait
    rep = evenement("Movie", "film-1", "Un Film")
    # Le fichier actuel de Radarr est encore là, mais c'est le même fichier, sous un autre nom : la fiche part.
    assert suppressions(porte["appels"]) == ["movie/9"]
    assert rep["status"] == "delegated"


def no_other_land(tmp_path, porte):
    """
    Bibliothèque « meuhmeuh » posée sur /data/Media. Radarr a remplacé Kitsune par la VOSTFR (upgrade), et l'event
    s'est perdu : le torrent de Kitsune est toujours là, et l'index ne connaît pas encore le nouveau nom de Radarr.
    """
    kitsune = fichier(str(tmp_path / "Multimedia/MeuhMeuh/No.Other.Land.Kitsune.mkv"), b"kitsune")
    vostfr = fichier(str(tmp_path / "Multimedia/MeuhMeuh/No.Other.Land.VOSTFR.mkv"), b"vostfr")
    dossier = tmp_path / "Media/meuhmeuh/No Other Land (2024)"
    ancien = lien(kitsune, str(dossier / "No Other Land (2024) - Kitsune.mkv"))
    nouveau = lien(vostfr, str(dossier / "No Other Land (2024) - VOSTFR.mkv"))
    indexer(kitsune, torrent="kitsune")
    indexer(ancien, item="film-kitsune")
    indexer(vostfr, torrent="vostfr")
    porte["etat"]["bibliotheques"] = {str(tmp_path / "Media/meuhmeuh")}
    porte["etat"]["movies"] = [{"id": 714, "title": "No Other Land", "path": str(dossier),
                                "movieFile": {"id": 50, "path": nouveau}}]
    os.remove(ancien)  # l'upgrade : Radarr efface l'ancien fichier, Jellyfin le retire de sa base
    return ancien, nouveau


def test_upgrade_ne_supprime_pas_le_film(tmp_path, porte):
    """No Other Land, 2026-11-22 : l'ItemDeleted de l'ancien fichier supprimait le film, nouvelle version comprise."""
    ancien, nouveau = no_other_land(tmp_path, porte)
    rep = evenement("Movie", "film-kitsune", "No Other Land")
    assert suppressions(porte["appels"]) == []
    assert rep.get("status") != "delegated"
    # Le torrent de l'ancien fichier passe par la corbeille, comme l'event d'upgrade l'aurait fait.
    assert porte["gestes"] == [[ancien]]
    assert os.path.exists(nouveau)


def test_upgrade_d_un_fichier_sans_torrent_ne_supprime_pas_le_film(tmp_path, porte):
    """L'ancien fichier n'avait aucun torrent : son inode disparaît avec lui, la disparition a l'air d'une suppression."""
    dossier = tmp_path / "Media/meuhmeuh/Vieux Film (1990)"
    ancien = fichier(str(dossier / "Vieux Film (1990) - DVD.mkv"), b"dvd")
    nouveau = fichier(str(dossier / "Vieux Film (1990) - Bluray.mkv"), b"bluray")
    indexer(ancien, item="film-dvd")
    porte["etat"]["bibliotheques"] = {str(tmp_path / "Media/meuhmeuh")}
    porte["etat"]["movies"] = [{"id": 715, "title": "Vieux Film", "path": str(dossier),
                                "movieFile": {"id": 52, "relativePath": "Vieux Film (1990) - Bluray.mkv"}}]
    os.remove(ancien)
    rep = evenement("Movie", "film-dvd", "Vieux Film")
    assert suppressions(porte["appels"]) == []
    assert rep.get("status") != "delegated"
    assert os.path.exists(nouveau)


def test_film_supprime_dans_sa_bibliotheque_media_est_delegue(tmp_path, porte):
    """Même bibliothèque, cas ordinaire : le fichier qu'on supprime est le fichier actuel de Radarr."""
    torrent = fichier(str(tmp_path / "Multimedia/MeuhMeuh/Un.Autre.Film.mkv"))
    dossier = tmp_path / "Media/meuhmeuh/Un Autre Film (2020)"
    radarr = lien(torrent, str(dossier / "Un Autre Film (2020).mkv"))
    indexer(torrent, torrent="t8")
    indexer(radarr, item="film-meuh")
    porte["etat"]["bibliotheques"] = {str(tmp_path / "Media/meuhmeuh")}
    porte["etat"]["movies"] = [{"id": 15, "title": "Un Autre Film", "path": str(dossier),
                                "movieFile": {"id": 51, "relativePath": "Un Autre Film (2020).mkv"}}]
    os.remove(radarr)
    rep = evenement("Movie", "film-meuh", "Un Autre Film")
    assert suppressions(porte["appels"]) == ["movie/15"]
    assert rep["status"] == "delegated"


@pytest.fixture
def ajouts(monkeypatch):
    """Les ajouts Jellyfin repartent de zéro, avec un délai qui ne s'écoule jamais pendant le test."""
    monkeypatch.setattr(app, "_JELLYFIN_INDEX_DELAY", 3600)
    monkeypatch.setattr(app, "_jellyfin_index_timer", None)
    monkeypatch.setattr(app, "_jellyfin_index_items", 0)
    yield
    if app._jellyfin_index_timer is not None:
        app._jellyfin_index_timer.cancel()


def ajout(item_type, item_id="x", nom="test"):
    rep = app.app.test_client().post("/jellyfin?token=tests", json={
        "NotificationType": "ItemAdded", "ItemType": item_type, "ItemId": item_id, "Name": nom})
    assert rep.status_code == 200
    return rep.get_json()["status"]


def test_ajouts_regroupes_dans_une_seule_sync(porte, ajouts, monkeypatch):
    syncs = []
    monkeypatch.setattr(app, "sync_db", lambda **k: syncs.append(k) or "success")
    avant = time.time()
    assert ajout("Episode") == "index scheduled"
    assert ajout("Episode") == "index already scheduled"
    assert ajout("Series") == "ignored"  # un conteneur n'a pas de fichier : ses épisodes arrivent à part
    assert syncs == []
    app._jellyfin_index_timer.cancel()
    assert app._run_jellyfin_index() == "success"
    assert len(syncs) == 1
    apres = syncs[0].pop("apres")
    assert syncs == [{"sauvegarde": False, "motif": "2 ajout(s) dans Jellyfin"}]
    assert avant <= apres <= time.time()  # l'instant du premier ajout : une sync commencée depuis la rend inutile
    # La sync passée, un ajout en programme une autre.
    assert ajout("Movie") == "index scheduled"


def test_oppenheimer_supprime_juste_apres_son_ajout(tmp_path, porte, ajouts, monkeypatch):
    """
    Oppenheimer, 2026-11-22 : importé à 17:37, supprimé dans Jellyfin à 17:49, la dernière sync datait de 13:07.
    L'ItemAdded fait connaître l'item tout de suite, et la suppression atteint Radarr.
    """
    with app.get_db() as conn:
        conn.execute("DELETE FROM sync_log")  # aucune sync récente : celle de l'ajout doit avoir lieu
    films = tmp_path / "Multimedia/Films"
    torrent = fichier(str(films / "Oppenheimer.2023.QTZ.mkv"))
    dossier = tmp_path / "Media/Films/Oppenheimer (2023)"
    radarr = lien(torrent, str(dossier / "Oppenheimer (2023).mkv"))

    @contextmanager
    def session():
        yield object()

    sauvegardes = []
    monkeypatch.setattr(app, "qbit_session", session)
    monkeypatch.setattr(app, "qbit_get_categories", lambda s: {"[seed] Films": str(films)})
    monkeypatch.setattr(app, "qbit_get_torrents", lambda s: [{
        "hash": "a" * 40, "name": "Oppenheimer.2023.QTZ.mkv", "category": "[seed] Films",
        "content_path": torrent, "save_path": str(films)}])
    monkeypatch.setattr(app, "backup_db", lambda: sauvegardes.append(True))
    monkeypatch.setattr(app, "refresh_arr_managed_from_links", lambda: 0)
    monkeypatch.setattr(app, "purge_arr_managed_absents", lambda hashes: 0)
    monkeypatch.setattr(app, "_SYNC_MEDIA_ROOT", str(tmp_path / "Media"))
    monkeypatch.setattr(app, "jellyfin_fetch_items", lambda: [{"Id": "oppenheimer", "Path": torrent}])
    porte["etat"]["bibliotheques"] = {str(films)}
    porte["etat"]["movies"] = [{"id": 713, "title": "Oppenheimer", "path": str(dossier),
                                "movieFile": {"id": 70, "path": radarr}}]

    assert app._jellyfin_paths_for_item("oppenheimer") == []  # avant : la suppression restait sans effet
    assert ajout("Movie", "oppenheimer", "Oppenheimer") == "index scheduled"
    app._jellyfin_index_timer.cancel()
    assert app._run_jellyfin_index() == "success"
    assert sauvegardes == []  # pas de copie de la base pour une sync d'ajout

    os.remove(torrent)  # Jellyfin supprime le fichier qu'il voit : celui du torrent
    rep = evenement("Movie", "oppenheimer", "Oppenheimer")
    assert suppressions(porte["appels"]) == ["movie/713"]
    assert rep["status"] == "delegated"


def test_serie_supprimee_dans_sa_bibliotheque_reste_deleguee(tmp_path, porte):
    """Le cas ordinaire : la copie de Sonarr supprimée dans Jellyfin, le torrent invisible pour Jellyfin."""
    torrent = fichier(str(tmp_path / "Multimedia/Séries/Show.S01E01.mkv"))
    sonarr = lien(torrent, str(tmp_path / "Media/Séries/Show (2020)/Season 01/S01E01.mkv"))
    indexer(torrent, torrent="t3")
    indexer(sonarr, item="ep-show", serie="serie-show")
    porte["etat"]["bibliotheques"] = {str(tmp_path / "Media/Séries")}
    porte["etat"]["series"] = [{"id": 31, "title": "Show", "path": str(tmp_path / "Media/Séries/Show (2020)")}]
    porte["etat"]["episodefiles"] = [{"id": 7, "path": sonarr}]
    os.remove(sonarr)
    rep = evenement("Series", "serie-show", "Show")
    assert suppressions(porte["appels"]) == ["series/31"]
    assert rep["status"] == "delegated"


def test_bibliotheques_illisibles_rien_n_est_delegue(tmp_path, porte):
    torrent = fichier(str(tmp_path / "Multimedia/Films/Autre.Film.2021/Autre.Film.2021.mkv"))
    radarr = lien(torrent, str(tmp_path / "Media/Films/Autre Film (2021)/Autre Film (2021).mkv"))
    indexer(torrent, torrent="t4", item="film-2")
    indexer(radarr)
    porte["etat"]["bibliotheques"] = None
    porte["etat"]["movies"] = [{"id": 10, "title": "Autre Film", "path": str(tmp_path / "Media/Films/Autre Film (2021)"),
                                "movieFile": {"id": 6}}]
    os.remove(torrent)
    rep = evenement("Movie", "film-2", "Autre Film")
    # Sans bibliothèques lisibles, la fiche ne peut venir que du chemin supprimé, que Radarr ne connaît pas.
    assert suppressions(porte["appels"]) == []
    assert rep.get("status") != "delegated"


def test_deplacement_reste_ignore(tmp_path, porte):
    """Un dossier déplacé par Sonarr (même inode, nouveau nom inconnu de l'index) : rien n'est supprimé."""
    torrent = fichier(str(tmp_path / "Multimedia/Eggshell/Pokemon.S03E01.mkv"))
    ancien = lien(torrent, str(tmp_path / "Media/Eggshell/Pokémon (1997)/Season 03/S03E01.mkv"))
    indexer(torrent, torrent="t5")
    indexer(ancien, item="ep-poke", serie="serie-poke")
    nouveau = str(tmp_path / "Media/Rays_Le _Chat/Pokémon (1997)/Season 03/S03E01.mkv")
    os.makedirs(os.path.dirname(nouveau))
    os.rename(ancien, nouveau)
    porte["etat"]["bibliotheques"] = {str(tmp_path / "Media/Eggshell"), str(tmp_path / "Media/Rays_Le _Chat")}
    porte["etat"]["series"] = [{"id": 494, "title": "Pokémon", "path": str(tmp_path / "Media/Rays_Le _Chat/Pokémon (1997)")}]
    rep = evenement("Series", "serie-poke", "Pokémon")
    assert suppressions(porte["appels"]) == []
    assert rep["status"] == "ignored" and rep["reason"] == "file moved"


def test_deplacement_connu_de_l_index_reste_protege(tmp_path, porte):
    """Le même déplacement, mais après une sync qui connaît déjà le nouveau nom : l'ancienne porte supprimait la série."""
    torrent = fichier(str(tmp_path / "Multimedia/Eggshell/Pokemon.S03E02.mkv"))
    ancien = lien(torrent, str(tmp_path / "Media/Eggshell/Pokémon (1997)/Season 03/S03E02.mkv"))
    indexer(torrent, torrent="t6")
    indexer(ancien, item="ep-poke2", serie="serie-poke2")
    nouveau = str(tmp_path / "Media/Rays_Le _Chat/Pokémon (1997)/Season 03/S03E02.mkv")
    os.makedirs(os.path.dirname(nouveau))
    os.rename(ancien, nouveau)
    indexer(nouveau, item="ep-poke2-nouveau", serie="serie-poke2-nouveau")
    porte["etat"]["bibliotheques"] = {str(tmp_path / "Media/Eggshell"), str(tmp_path / "Media/Rays_Le _Chat")}
    porte["etat"]["series"] = [{"id": 494, "title": "Pokémon", "path": str(tmp_path / "Media/Rays_Le _Chat/Pokémon (1997)")}]
    porte["etat"]["episodefiles"] = [{"id": 8, "path": nouveau}]
    rep = evenement("Series", "serie-poke2", "Pokémon")
    assert suppressions(porte["appels"]) == []
    assert rep.get("status") != "delegated"
    assert os.path.exists(nouveau)
