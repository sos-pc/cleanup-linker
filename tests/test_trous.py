"""
Règle 4, saisons trouées : une saison demandée dans Seerr garde des épisodes sans fichier ni téléchargement, faute
de release à l'unité. Le cas de StuGo, demandé le 20 novembre 2026 : 40 épisodes, 2 fichiers, 8 dans la file, et pour
les 31 autres un seul pack 720p hongrois et anglais. Faux Sonarr, Seerr, Prowlarr et qBittorrent ; vraie base.
"""
import logging
import re
import time
from contextlib import contextmanager

import pytest
from faux import FauxQbit, faux_arr

import app

SERIE = 495
PACK = "StuGo.S01.720p.DSNP.WEB-DL.DDP5.1.H.264.HUN.ENG-donthave"
DANS_LA_FILE = (1, 2, 3, 4, 5, 7, 8, 9)
AVEC_FICHIER = (2, 14)
TROUS = {1000 + n for n in range(1, 41) if n not in DANS_LA_FILE + AVEC_FICHIER}


def il_y_a(secondes: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - secondes))


def parse(kwargs):
    """Le parseur de Sonarr, réduit : « S01E06 » donne l'épisode 1006, « S01 » seul la saison entière."""
    titre = kwargs["params"]["title"]
    serie = "Monster The Lizzie Borden Story" if "Lizzie" in titre else "StuGo"
    m = re.search(r"S01E(\d\d)", titre, re.IGNORECASE)
    if m:
        n = int(m.group(1))
        return {"series": {"id": SERIE}, "episodes": [{"id": 1000 + n, "seasonNumber": 1}],
                "parsedEpisodeInfo": {"seriesTitle": serie, "seasonNumber": 1, "episodeNumbers": [n],
                                      "quality": {"quality": {"id": 3}}},
                "customFormatScore": 110 if "FRENCH" in titre else -10, "customFormats": []}
    return {"series": {"id": SERIE}, "episodes": [],
            "parsedEpisodeInfo": {"seriesTitle": serie, "seasonNumber": 1, "fullSeason": True,
                                  "quality": {"quality": {"id": 5}}},
            "customFormatScore": -10, "customFormats": []}


def torrent_du_pack(nom):
    fichiers = [{"length": 1000, "path": [f"StuGo.S01E{n:02d}.720p.DSNP.WEB-DL.mkv"]} for n in range(1, 41)]
    return app._bencode({"info": {"name": nom, "files": fichiers, "piece length": 16384, "pieces": b"\0" * 20}})


@pytest.fixture
def stugo(monkeypatch, caplog):
    """StuGo dans un faux Sonarr, demandé il y a deux jours ; un pack hongrois et anglais dans Prowlarr."""
    etat = {
        "ajoutee": il_y_a(2 * 86400),
        "demandes": [{"media": {"tvdbId": 454056, "tmdbId": 1}, "createdAt": il_y_a(2 * 86400)}],
        "episodes": [{"id": 1000 + n, "seasonNumber": 1, "episodeNumber": n, "monitored": True,
                      "hasFile": n in AVEC_FICHIER, "airDateUtc": "2025-01-11T00:00:00Z"} for n in range(1, 41)],
        "file": [{"episodeId": 1000 + n, "seriesId": SERIE, "title": f"StuGo.S01E{n:02d}.FRENCH.1080p.WEB.H264-FW"}
                 for n in DANS_LA_FILE],
        "packs": [{"title": PACK, "protocol": "torrent", "downloadUrl": "http://prowlarr.invalid/1", "seeders": 3,
                   "size": 9_400_000_000, "indexer": "nCore"}],
        "ajouts": [], "recherches": [],
    }
    serie = {"id": SERIE, "title": "StuGo", "monitored": True, "tags": [17], "qualityProfileId": 7,
             "seasons": [{"seasonNumber": 0, "monitored": False}, {"seasonNumber": 1, "monitored": True}]}
    clients = [
        {"name": "Eggshell", "enable": True, "implementation": "QBittorrent", "tags": [9], "priority": 1,
         "fields": [{"name": "tvCategory", "value": "Eggshell"}]},
        {"name": "Rays_Le _Chat", "enable": True, "implementation": "QBittorrent", "tags": [17], "priority": 1,
         "fields": [{"name": "tvCategory", "value": "Rays_Le _Chat"}]},
        {"name": "Défaut", "enable": True, "implementation": "QBittorrent", "tags": [], "priority": 1,
         "fields": [{"name": "tvCategory", "value": "Séries"}]},
    ]
    routes = {
        # Une série par demande : StuGo pour son identifiant TVDB, une autre pour les autres.
        ("GET", "series"): lambda kw: [dict(serie, added=etat["ajoutee"],
                                            id=SERIE if kw["params"]["tvdbId"] == 454056 else kw["params"]["tvdbId"])],
        ("GET", "episode"): lambda kw: etat["episodes"],
        ("GET", "queue/details"): lambda kw: etat["file"],
        ("GET", "downloadclient"): clients,
        ("GET", "episodefile"): [{"seasonNumber": 1, "sceneName": "StuGo.S01E14.Truck.Everlasting.1080p.HULU.WEB-DL.DD5.1.H.264-NTb"}],
        ("GET", "parse"): parse,
        ("GET", "qualityprofile/7"): {"items": [{"quality": {"id": 5}, "allowed": True},
                                                {"quality": {"id": 3}, "allowed": True}],
                                      "minFormatScore": -20, "formatItems": []},
    }
    qb = FauxQbit()

    @contextmanager
    def session():
        yield qb

    for nom, valeur in {"SONARR_URL": "http://sonarr.invalid", "SONARR_API_KEY": "tests",
                        "SEERR_URL": "http://seerr.invalid", "SEERR_API_KEY": "tests",
                        "PROWLARR_URL": "http://prowlarr.invalid", "PROWLARR_API_KEY": "tests",
                        "IMPORT_ASSIST_PACK_DRY_RUN": False, "IMPORT_ASSIST_TROUS_DRY_RUN": False}.items():
        monkeypatch.setattr(app, nom, valeur)
    monkeypatch.setattr(app, "_arr_request", faux_arr(routes, []))
    monkeypatch.setattr(app, "_seerr_tv_requests", lambda depuis: etat["demandes"])
    monkeypatch.setattr(app, "_prowlarr_search", lambda requete: etat["recherches"].append(requete) or list(etat["packs"]))
    monkeypatch.setattr(app, "_fetch_torrent", lambda lien: torrent_du_pack(etat["packs"][0]["title"]))
    monkeypatch.setattr(app, "qbit_session", session)
    monkeypatch.setattr(app, "_qbit_add", lambda cand, categorie, ref, seulement=None:
                        etat["ajouts"].append((cand["title"], categorie, ref, seulement)))
    with app.get_db() as conn:
        conn.execute("DELETE FROM pack_replace")
    caplog.set_level(logging.INFO)
    return etat


def contexte(torrents=None, ecartees=None, apercu=False):
    return {"torrents": torrents or {}, "categories": {}, "dry_run": False, "apercu": apercu, "ecartees": ecartees or {}}


def journal():
    with app.get_db() as conn:
        return [dict(r) for r in conn.execute("SELECT download_id, decision, dry_run, details FROM assist_log")]


def remplacements():
    with app.get_db() as conn:
        return [dict(r) for r in conn.execute("SELECT series_id, season, old_hash, new_title, stage, episodes FROM pack_replace")]


def test_stugo_les_31_trous_combles_par_le_pack(stugo):
    assert len(TROUS) == 31
    verdicts, nouveaux = app.run_season_holes(contexte())
    assert nouveaux == 1 and verdicts[0]["decision"] == "pack-hole-fill"
    [(titre, categorie, ref, seulement)] = stugo["ajouts"]
    assert (titre, categorie, ref) == (PACK, "Rays_Le _Chat", {"auto_tmm": True})
    # Seuls les fichiers des trous sont téléchargés : ni les épisodes dans la file, ni ceux qui ont un fichier.
    assert sorted(seulement) == sorted(f"StuGo.S01E{i - 1000:02d}.720p.DSNP.WEB-DL.mkv" for i in TROUS)
    [r] = remplacements()
    assert (r["series_id"], r["season"], r["old_hash"], r["stage"]) == (SERIE, 1, f"trous-{SERIE}-s1", "vo")
    assert set(app.json.loads(r["episodes"])) == TROUS
    assert [(j["decision"], j["dry_run"]) for j in journal()] == [("pack-hole-fill", 0)]


def test_simulation_rien_n_est_ajoute(stugo, monkeypatch):
    monkeypatch.setattr(app, "IMPORT_ASSIST_TROUS_DRY_RUN", True)
    verdicts, _ = app.run_season_holes(contexte())
    assert verdicts[0]["decision"] == "pack-hole-fill"
    assert stugo["ajouts"] == [] and remplacements() == []
    assert [(j["decision"], j["dry_run"]) for j in journal()] == [("pack-hole-fill", 1)]


def test_la_simulation_de_la_regle_4_vaut_aussi(stugo, monkeypatch):
    monkeypatch.setattr(app, "IMPORT_ASSIST_PACK_DRY_RUN", True)
    app.run_season_holes(contexte())
    assert stugo["ajouts"] == []


@pytest.mark.parametrize("quoi", ["série", "demande"])
def test_pas_avant_24_h(stugo, quoi):
    if quoi == "série":
        stugo["ajoutee"] = il_y_a(3600)
    else:
        stugo["demandes"][0]["createdAt"] = il_y_a(3600)
    assert app.run_season_holes(contexte()) == ([], 0)
    assert stugo["recherches"] == []


def test_un_episode_tout_juste_diffuse_n_est_pas_un_trou(stugo):
    for e in stugo["episodes"]:
        e["hasFile"] = True
    stugo["episodes"][39].update(hasFile=False, airDateUtc=il_y_a(3600))
    assert app.run_season_holes(contexte()) == ([], 0)


def test_une_saison_n_est_pas_recherchee_deux_fois_en_3_jours(stugo):
    stugo["packs"] = []
    verdicts, _ = app.run_season_holes(contexte())
    assert verdicts[0]["decision"] == "pack-hole-none"
    assert app.run_season_holes(contexte()) == ([], 0)
    assert len(stugo["recherches"]) == 1


def test_rien_tant_que_le_pack_ajoute_est_en_route(stugo):
    app.run_season_holes(contexte())
    with app.get_db() as conn:
        nouveau = conn.execute("SELECT new_hash FROM pack_replace").fetchone()[0]
        conn.execute("DELETE FROM assist_log")
    assert app.run_season_holes(contexte({nouveau: {"progress": 0.0}})) == ([], 0)
    assert len(stugo["ajouts"]) == 1


def test_un_pack_d_une_autre_histoire_est_ecarte(stugo):
    """Le garde-fou des anthologies : un pack que Sonarr range sous la série, mais qui porte un autre titre."""
    stugo["packs"][0]["title"] = "Monster.The.Lizzie.Borden.Story.S01.720p.WEB-DL"
    verdicts, _ = app.run_season_holes(contexte())
    assert verdicts[0]["decision"] == "pack-hole-none"
    assert "sous un autre titre" in verdicts[0]["reason"]
    assert stugo["ajouts"] == []


def test_la_saison_ecartee_par_la_regle_5_n_est_pas_comblee(stugo):
    assert app.run_season_holes(contexte(ecartees={SERIE: {1}})) == ([], 0)


def test_deux_saisons_au_plus_par_passage(stugo, caplog):
    stugo["demandes"] = [{"media": {"tvdbId": tvdb, "tmdbId": tvdb}, "createdAt": il_y_a(2 * 86400)} for tvdb in (1, 2, 3)]
    stugo["packs"] = []
    verdicts, nouveaux = app.run_season_holes(contexte())
    assert nouveaux == 2
    assert "1 autre(s) saison(s) trouée(s) attendront le passage suivant" in caplog.text


@pytest.mark.parametrize("etiquettes,client", [([17], "Rays_Le _Chat"), ([9], "Eggshell"), ([], "Défaut"), ([42], "Défaut")])
def test_le_client_de_la_serie(etiquettes, client):
    clients = [
        {"name": "Désactivé", "enable": False, "implementation": "QBittorrent", "tags": [17]},
        {"name": "Eggshell", "enable": True, "implementation": "QBittorrent", "tags": [9]},
        {"name": "Rays_Le _Chat", "enable": True, "implementation": "QBittorrent", "tags": [17]},
        {"name": "Défaut", "enable": True, "implementation": "QBittorrent", "tags": []},
    ]
    assert app._series_client(clients, {"tags": etiquettes}) == client


def test_le_passage_de_renfort_a_lieu_toutes_les_heures():
    assert app.IMPORT_ASSIST_INTERVAL == 3600


# ── Saisons tenues par un téléchargement mort, et demandes par saison ────────

def mourir(stugo, dead_wait=True, seeds=0):
    """Les téléchargements de la file deviennent un seul magnet mort-né, comme Pokémon S04."""
    mort = "d" * 40
    for q in stugo["file"]:
        q["downloadId"] = mort.upper()
    if dead_wait:
        app._assist_record(mort, "sonarr", "magnet", "dead-wait", "aucune autre version trouvée", False, 6)
    return {mort: {"hash": mort, "progress": 0, "state": "metaDL", "added_on": time.time() - 20 * 3600,
                   "last_activity": time.time() - 20 * 3600, "num_seeds": seeds, "dlspeed": 0, "downloaded": 0}}


def test_un_mort_sans_autre_version_ne_tient_pas_ses_episodes(stugo):
    app.run_season_holes(contexte(mourir(stugo)))
    [(_, _, _, seulement)] = stugo["ajouts"]
    assert len(seulement) == 38  # les 40, moins les 2 qui ont un fichier


def test_un_mort_encore_en_course_tient_ses_episodes(stugo):
    app.run_season_holes(contexte(mourir(stugo, dead_wait=False)))
    [(_, _, _, seulement)] = stugo["ajouts"]
    assert len(seulement) == 31


def test_un_telechargement_vivant_tient_ses_episodes(stugo):
    app.run_season_holes(contexte(mourir(stugo, seeds=2)))
    [(_, _, _, seulement)] = stugo["ajouts"]
    assert len(seulement) == 31


def demande(createdat, saison):
    return {"media": {"tvdbId": 454056, "tmdbId": 1}, "createdAt": createdat, "seasons": [{"seasonNumber": saison}]}


def test_chaque_saison_compte_depuis_sa_demande(stugo):
    """Pokémon : la saison 1, demandée il y a deux jours, n'attend pas la demande d'une autre saison faite à l'instant."""
    stugo["demandes"] = [demande(il_y_a(2 * 86400), 1), demande(il_y_a(3600), 2)]
    verdicts, _ = app.run_season_holes(contexte())
    assert verdicts[0]["decision"] == "pack-hole-fill"


def test_la_saison_demandee_a_l_instant_attend(stugo):
    stugo["demandes"] = [demande(il_y_a(2 * 86400), 2), demande(il_y_a(3600), 1)]
    assert app.run_season_holes(contexte()) == ([], 0)
