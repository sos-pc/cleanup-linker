"""
La porte Jellyfin : une suppression vue par Jellyfin, traduite en action Sonarr/Radarr. Vrais fichiers et vrais
hardlinks dans un dossier jetable, index rempli à la main, route /jellyfin appelée avec un faux Sonarr et un faux
Radarr qui notent chaque appel. Le cas de l'incident Westworld (2026-11-12) en tête.
"""
import os

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
                                "movieFile": {"id": 5}}]
    os.remove(torrent)  # Jellyfin a supprimé le fichier qu'il voyait
    rep = evenement("Movie", "film-1", "Un Film")
    assert suppressions(porte["appels"]) == ["movie/9"]
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
