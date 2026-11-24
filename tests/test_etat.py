"""
État du flux : jugements purs, instantané complet contre de faux Seerr, Sonarr, Radarr et qBittorrent,
journal jour par jour, page et route. Les cas viennent de la mesure du 2026-11-24.
"""
import json
import time
from contextlib import contextmanager

import pytest

import app
import etat

MAINTENANT = 1_800_000_000.0  # une date fixe : les âges et les délais ne dépendent pas de l'heure du test
H, J = 3600, 86400


def _iso(instant):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(instant))


def _torrent(h="a" * 40, progres=0.0, ajoute=MAINTENANT - 20 * H, etat_qbit="stalledDL", recu=0, **autres):
    return {"hash": h, "name": autres.pop("name", "Torrent"), "progress": progres, "added_on": ajoute, "state": etat_qbit,
            "downloaded": recu, "completed": recu, "seen_complete": -1, "num_seeds": 0, "dlspeed": 0,
            "last_activity": 0, **autres}


# ── Jugements ───────────────────────────────────────────────────────────────


def test_telechargement_bloque_avec_son_message():
    r = {"trackedDownloadState": "importBlocked",
         "statusMessages": [{"messages": ["Found matching series via grab history, but release was matched by ID"]}]}
    assert etat.etat_telechargement([r], _torrent(progres=1), MAINTENANT) == \
        ("bloqué", "Found matching series via grab history, but release was matched by ID")


def test_telechargement_mort_ne_et_en_cours():
    r = {"trackedDownloadState": "downloading"}
    etat_dl, detail = etat.etat_telechargement([r], _torrent(), MAINTENANT)  # rien reçu en 20 h : mort-né
    assert etat_dl == "mort" and "rien reçu depuis son ajout il y a 20 h" in detail
    assert etat.etat_telechargement([r], _torrent(ajoute=MAINTENANT - 2 * H), MAINTENANT) == ("en cours", "0 %")
    assert etat.etat_telechargement([r], _torrent(progres=1), MAINTENANT) == ("fini", "")
    assert etat.etat_telechargement([r], None, MAINTENANT) == ("absent de qBittorrent", "")


@pytest.mark.parametrize("arr,records,attendu", [
    ("sonarr", [{"episode": {"monitored": True, "hasFile": False}}, {"episode": {"monitored": True, "hasFile": True}}], "attendu"),
    ("sonarr", [{"episode": {"monitored": True, "hasFile": True}}], "upgrade"),
    ("sonarr", [{"episode": {"monitored": False, "hasFile": False}}], "non surveillé"),
    ("radarr", [{"movie": {"monitored": True, "hasFile": False}}], "attendu"),
    ("radarr", [{"movie": {"monitored": True, "hasFile": True}}], "upgrade"),
])
def test_objet(arr, records, attendu):
    assert etat.objet(arr, records) == attendu


@pytest.mark.parametrize("film,attendu", [
    ({"status": "released", "digitalRelease": _iso(MAINTENANT - 80 * J)}, ""),           # La Mémoire des Poissons
    ({"status": "released", "inCinemas": _iso(MAINTENANT - 18 * 365 * J)}, ""),         # Parking : aucune date vidéo
    ({"status": "inCinemas", "inCinemas": _iso(MAINTENANT - 30 * J), "digitalRelease": _iso(MAINTENANT + 14 * J)},
     f"au cinéma depuis le {etat._date(MAINTENANT - 30 * J)}, sortie numérique le {etat._date(MAINTENANT + 14 * J)}"),
    ({"status": "inCinemas", "inCinemas": _iso(MAINTENANT - 10 * J)},
     f"au cinéma depuis le {etat._date(MAINTENANT - 10 * J)}, pas encore de date de sortie en vidéo"),
    ({"status": "announced", "inCinemas": _iso(MAINTENANT + 400 * J)},
     f"au cinéma le {etat._date(MAINTENANT + 400 * J)}, pas encore de date de sortie en vidéo"),         # Elden Ring
    ({"status": "announced"}, "annoncé, pas encore de date de sortie en vidéo"),
])
def test_sortie_film(film, attendu):
    assert etat.sortie_film(film, MAINTENANT) == attendu


SORTI = {"id": 1, "title": "Parking", "monitored": True, "hasFile": False, "status": "released"}


@pytest.mark.parametrize("film,telechargements,age,attendu", [
    (None, [], 5 * J, ("fiche retirée", "plus de fiche Radarr")),
    (dict(SORTI, hasFile=True), [], 5 * J, ("disponible", "")),
    (dict(SORTI, monitored=False), [], 5 * J, ("non suivie", "film non surveillé dans Radarr")),
    (SORTI, [{"titre": "Parking.mkv", "etat": "en cours", "detail": "3 %"},
             {"titre": "Parking.1080p", "etat": "mort", "detail": "plus rien reçu depuis 43 j"}], 43 * J,
     ("téléchargement mort", "Parking.1080p : plus rien reçu depuis 43 j")),
    (SORTI, [{"titre": "A", "etat": "mort", "detail": "x"}, {"titre": "B", "etat": "bloqué", "detail": "matched by ID"}], J,
     ("bloquée à l'import", "B : matched by ID")),
    (SORTI, [{"titre": "A", "etat": "en cours", "detail": "40 %"}], J, ("en cours", "A : 40 %")),
    (dict(SORTI, status="announced"), [], 30 * J, ("pas encore sortie", "annoncé, pas encore de date de sortie en vidéo")),
    (SORTI, [], 2 * H, ("en cours", "recherche en cours")),
    (SORTI, [], 2 * J, ("introuvable", "rien dans la file : aucune release acceptée par Radarr")),
])
def test_etat_film(film, telechargements, age, attendu):
    assert etat.etat_film(film, telechargements, age, MAINTENANT) == attendu


def _ep(id_, fichier, surveille=True, diffuse=True):
    return {"id": id_, "hasFile": fichier, "monitored": surveille,
            "airDateUtc": _iso(MAINTENANT - 10 * J if diffuse else MAINTENANT + 10 * J)}


def test_etat_serie():
    juge = lambda eps, par=None, age=5 * J: etat.etat_serie(eps, par or {}, age, MAINTENANT)  # noqa: E731
    assert juge([])[0] == "introuvable"
    assert juge([_ep(1, False, diffuse=False)]) == ("pas encore sortie", "aucun épisode diffusé")
    assert juge([_ep(1, True), _ep(2, True), _ep(3, False, diffuse=False)]) == ("disponible", "")
    # StuGo : 40/46, 6 épisodes sans rien en file
    stugo = [_ep(i, i <= 40) for i in range(1, 47)]
    assert juge(stugo) == ("introuvable", "40/46 épisodes ; manquants : 6 sans téléchargement")
    assert juge(stugo, age=3 * H)[0] == "en cours"
    # Le pire l'emporte : un import bloqué avant un mort, un mort avant un manque sans téléchargement
    par = {41: [{"etat": "mort"}], 42: [{"etat": "bloqué"}]}
    assert juge(stugo, par) == ("bloquée à l'import", "40/46 épisodes ; manquants : 1 bloqué(s) à l'import, 1 mort(s), 4 sans téléchargement")
    assert juge(stugo, {41: [{"etat": "mort"}]})[0] == "téléchargement mort"
    # Doctor Who S4 : supprimée après coup, épisodes dé-surveillés par Sonarr
    assert juge([_ep(1, False, surveille=False), _ep(2, False, surveille=False)]) == \
        ("non suivie", "0/2 épisodes ; manquants : 2 non surveillé(s) (souvent après une suppression)")
    assert juge([_ep(1, False)], {1: [{"etat": "en cours"}]}) == ("en cours", "0/1 épisodes ; manquants : 1 en cours")


def test_delais():
    assert etat.delai(1000, [500, 990, 4000]) == -10  # l'*arr date l'import une minute avant Seerr au plus
    assert etat.delai(1000, [100, 900]) is None
    s = etat.stats_delais([60, 1800, 7200, 3 * J])
    assert s == {"n": 4, "mediane": 4500, "p90": 3 * J, "moins_1h": 2, "moins_24h": 3}
    assert etat.stats_delais([]) == {"n": 0}


# ── Instantané complet contre de faux services ──────────────────────────────

SONARR, RADARR = "http://sonarr.invalid", "http://radarr.invalid"
DL_MORT, DL_BLOQUE, DL_POKE = "1" * 40, "2" * 40, "3" * 40


@pytest.fixture
def services(monkeypatch, tmp_path):
    monkeypatch.setattr(app, "SONARR_URL", SONARR)
    monkeypatch.setattr(app, "SONARR_API_KEY", "cle")
    monkeypatch.setattr(app, "RADARR_URL", RADARR)
    monkeypatch.setattr(app, "RADARR_API_KEY", "cle")
    monkeypatch.setattr(etat, "FICHIER", str(tmp_path / "etat.json"))
    monkeypatch.setattr(app, "jellyfin_enabled", lambda: False)
    with app.get_db() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS etat_log (id INTEGER PRIMARY KEY AUTOINCREMENT, at INTEGER NOT NULL, resume TEXT NOT NULL)")
        conn.execute("DELETE FROM etat_log")
    demandes = [
        {"type": "movie", "status": 2, "createdAt": _iso(MAINTENANT - 43 * J), "media": {"tmdbId": 10},
         "requestedBy": {"displayName": "pascap"}},                                                     # Parking, mort
        {"type": "movie", "status": 5, "createdAt": _iso(MAINTENANT - 5 * J), "media": {"tmdbId": 11},
         "requestedBy": {"displayName": "meuhmeuh"}},                                                   # disponible en 26 min
        {"type": "movie", "status": 5, "createdAt": _iso(MAINTENANT - 9 * J), "media": {"tmdbId": 12, "mediaAddedAt": _iso(MAINTENANT - 8 * J)},
         "requestedBy": {"displayName": "methammer"}},                                                  # supprimé après coup
        {"type": "tv", "status": 2, "createdAt": _iso(MAINTENANT - 3 * J), "media": {"tvdbId": 76703},
         "seasons": [{"seasonNumber": 1}], "requestedBy": {"displayName": "Eggshell"}},                 # Pokémon S1, bloqué
        {"type": "movie", "status": 3, "createdAt": _iso(MAINTENANT - J), "media": {"tmdbId": 13}},     # refusée : ignorée
        {"type": "movie", "status": 5, "createdAt": _iso(MAINTENANT - 90 * J), "media": {"tmdbId": 11}},  # hors fenêtre
    ]
    routes = {
        (SONARR, "queue"): {"records": [
            {"downloadId": DL_BLOQUE.upper(), "seriesId": 494, "episodeId": 101, "title": "Pokemon.Ligue.Indigo.S01E01.VFQ",
             "trackedDownloadState": "importBlocked", "statusMessages": [{"messages": ["release was matched by ID"]}],
             "series": {"title": "Pokémon"}, "episode": {"monitored": True, "hasFile": False}},
            {"downloadId": DL_POKE, "seriesId": 494, "episodeId": 120, "title": "Pokemon S25E14 NOTAG",
             "trackedDownloadState": "downloading", "series": {"title": "Pokémon"}, "episode": {"monitored": False, "hasFile": False}},
        ], "totalRecords": 2},
        (RADARR, "queue"): {"records": [
            {"downloadId": DL_MORT, "movieId": 1, "title": "Parking.mkv", "trackedDownloadState": "downloading",
             "movie": {"title": "Parking", "monitored": True, "hasFile": False}},
        ], "totalRecords": 1},
        (RADARR, "movie"): [
            {"id": 1, "tmdbId": 10, "title": "Parking", "year": 2008, "monitored": True, "hasFile": False, "status": "released"},
            {"id": 2, "tmdbId": 11, "title": "No Other Land", "year": 2024, "monitored": True, "hasFile": True, "status": "released"},
        ],
        (SONARR, "series"): [{"id": 494, "tvdbId": 76703, "title": "Pokémon"}],
        (SONARR, "episode"): [_ep(101, False), _ep(102, True), _ep(120, False, surveille=False) | {"seasonNumber": 20}],
        (RADARR, "history/since"): [
            {"eventType": "grabbed", "movieId": 2, "downloadId": "ABC", "date": _iso(MAINTENANT - 5 * J + 60)},
            {"eventType": "downloadFolderImported", "movieId": 2, "date": _iso(MAINTENANT - 5 * J + 26 * 60)},
        ],
        (SONARR, "history/since"): [],
    }
    for e in routes[(SONARR, "episode")]:
        e.setdefault("seasonNumber", 1)
    appels = []

    def faux_arr(method, base_url, api_key, endpoint, timeout=30, retries=3, **kwargs):
        appels.append((base_url, endpoint))
        assert method == "GET", "l'état du flux ne fait que lire"
        return routes[(base_url, endpoint)]

    def faux_seerr(route, **params):
        assert route == "request"
        lot = demandes[params["skip"]:params["skip"] + params["take"]]
        return {"results": lot, "pageInfo": {"results": len(demandes)}}

    @contextmanager
    def session():
        yield None

    torrents = [_torrent(DL_MORT, ajoute=MAINTENANT - 43 * J, name="Parking.mkv"),
                _torrent(DL_BLOQUE, progres=1, etat_qbit="stalledUP", ajoute=MAINTENANT - 2 * H),
                _torrent(DL_POKE, ajoute=MAINTENANT - 20 * H),  # mort-né : rien reçu en 20 h
                _torrent("4" * 40, progres=1, etat_qbit="stoppedUP", category=app.TARGET_CAT, size=2e9),
                _torrent("5" * 40, etat_qbit="missingFiles", category="Films", name="High School Musical 3")]
    monkeypatch.setattr(app, "_arr_request", faux_arr)
    monkeypatch.setattr(app, "_seerr_get", faux_seerr)
    monkeypatch.setattr(app, "qbit_session", session)
    monkeypatch.setattr(app, "qbit_get_torrents", lambda s: [dict(t) for t in torrents])
    return {"appels": appels, "routes": routes, "torrents": torrents}


def test_instantane(services):
    inst = etat.calculer(MAINTENANT)
    assert inst["erreurs"] == {}
    d = inst["demandes"]
    assert d["total"] == 4  # la demande refusée et celle d'il y a 90 jours ne comptent pas
    assert d["par_etat"] == {"téléchargement mort": 1, "disponible": 1, "fiche retirée": 1, "bloquée à l'import": 1}
    assert d["delais"]["films"]["n"] == 1 and d["delais"]["films"]["mediane"] == 26 * 60
    assert [(x["titre"], x["etat"]) for x in d["lignes"]] == [
        ("Pokémon S1", "bloquée à l'import"), ("Parking (2008)", "téléchargement mort"), ("film TMDB 12", "fiche retirée")]
    assert "sans doute supprimé après coup" in d["lignes"][2]["detail"]
    assert "Parking.mkv : rien reçu depuis son ajout il y a 43 j" in d["lignes"][1]["detail"]
    f = inst["files"]
    assert f["radarr"]["morts"] == [{"titre": "Parking.mkv", "media": "Parking", "objet": "attendu",
                                     "detail": "rien reçu depuis son ajout il y a 43 j, aucune copie complète jamais vue"}]
    assert [m["objet"] for m in f["sonarr"]["morts"]] == ["non surveillé"]  # la prise S25 de la règle 6
    assert f["sonarr"]["bloques"][0]["detail"] == "release was matched by ID"
    assert f["sonarr"]["medias"] == [{"media": "Pokémon", "total": 2, "recents": 2, "morts": 1, "emballement": False}]
    fin = inst["fin_de_vie"]
    assert fin["corbeille"] == {"torrents": 1, "octets": 2e9, "seedent": 0, "arretes": 1}
    assert fin["erreurs"] == [{"nom": "High School Musical 3", "categorie": "Films", "etat": "missingFiles"}]
    assert inst["resume"]["a_regarder"] == 2 and inst["resume"]["morts_en_file"] == 2


def test_emballement_signale(services):
    """La vague Pokémon du 2026-11-23 : 59 prises en une heure pour une même série."""
    q = services["routes"][(SONARR, "queue")]
    modele = q["records"][1]
    q["records"] = [dict(modele, downloadId=f"{i:040x}") for i in range(12)]
    q["totalRecords"] = 12
    services["torrents"][:] = [_torrent(f"{i:040x}", ajoute=MAINTENANT - (H if i < 10 else 30 * H)) for i in range(12)]
    pokemon = etat.calculer(MAINTENANT)["files"]["sonarr"]["medias"][0]
    assert (pokemon["total"], pokemon["recents"], pokemon["emballement"]) == (12, 10, True)
    services["torrents"][:] = services["torrents"][1:]
    pokemon = etat.calculer(MAINTENANT)["files"]["sonarr"]["medias"][0]
    assert (pokemon["recents"], pokemon["emballement"]) == (9, False)


def test_un_service_en_panne_n_efface_que_sa_partie(services, monkeypatch):
    def seerr_en_panne(route, **params):
        raise RuntimeError("Seerr injoignable, clé 0123456789abcdef0123456789abcdef")
    monkeypatch.setattr(app, "_seerr_get", seerr_en_panne)
    inst = etat.calculer(MAINTENANT)
    assert inst["demandes"] is None and inst["files"]["radarr"]["total"] == 1
    assert inst["erreurs"] == {"demandes": "RuntimeError : Seerr injoignable, clé …"}


def test_ecrire_lire_journal(services):
    premier = etat.calculer(MAINTENANT - 2 * J)
    etat.ecrire(premier)
    etat.ecrire(dict(premier, at=MAINTENANT - 2 * J + H, resume=dict(premier["resume"], a_regarder=7)))
    dernier = etat.calculer(MAINTENANT)
    etat.ecrire(dernier)
    assert etat.lire()["at"] == MAINTENANT
    jours = etat.journal(MAINTENANT)
    assert [j["jour"] for j in jours] == [etat._date(MAINTENANT), etat._date(MAINTENANT - 2 * J)]
    assert jours[1]["a_regarder"] == 7  # le dernier résumé du jour


def test_page(services):
    inst = etat.calculer(MAINTENANT)
    inst["demandes"]["lignes"][0]["titre"] = "<script>alert(1)</script>"
    page = etat.page_html(inst, [dict(inst["resume"], jour="2027-01-15")], "/etat?token=x&refresh=1", MAINTENANT)
    assert "&lt;script&gt;" in page and "<script>" not in page
    for titre in ("Demandes Seerr des 60 derniers jours", "À regarder : bloquées, mortes, introuvables (2)",
                  "Files de Sonarr et de Radarr", "Fin de vie", "Évolution, 1 dernier(s) jour(s)"):
        assert titre in page
    assert "Aucun instantané" in etat.page_html(None, [], "/etat?token=x&refresh=1")


def test_route(services, monkeypatch):
    lances = []
    monkeypatch.setattr(etat, "lancer", lambda: lances.append(1))
    client = app.app.test_client()
    assert client.get("/etat").status_code == 401
    r = client.get("/etat?token=tests")
    assert r.status_code == 200 and "Aucun instantané" in r.get_data(as_text=True) and lances == [1]
    etat.ecrire(etat.calculer(MAINTENANT))
    r = client.get("/etat?token=tests")
    assert "Demandes Seerr" in r.get_data(as_text=True) and lances == [1]
    assert client.get("/etat?token=tests&format=json").get_json()["demandes"]["total"] == 4
    client.get("/etat?token=tests&refresh=1")
    assert lances == [1, 1]
    assert 'href="/etat?token=tests&amp;refresh=1"' in client.get("/etat?token=tests").get_data(as_text=True)


def test_modes_affiches(monkeypatch):
    """Un réglage secondaire suit la simulation de sa règle : la page ne doit pas dire « mort-nés ACTIVE » quand la règle 6 simule."""
    monkeypatch.setattr(app, "IMPORT_ASSIST_MORT_DRY_RUN", True)
    monkeypatch.setattr(app, "IMPORT_ASSIST_MORT_JAMAIS_DRY_RUN", False)
    monkeypatch.setattr(app, "IMPORT_ASSIST_NOM_DRY_RUN", True)
    monkeypatch.setattr(app, "IMPORT_ASSIST_IDENTITE_DRY_RUN", False)
    modes = app._assist_modes()
    assert "mort-nés après 12 h en simulation" in modes and "identité confirmée en simulation" in modes
    monkeypatch.setattr(app, "IMPORT_ASSIST_MORT_DRY_RUN", False)
    monkeypatch.setattr(app, "IMPORT_ASSIST_NOM_DRY_RUN", False)
    modes = app._assist_modes()
    assert "mort-nés après 12 h ACTIVE" in modes and "identité confirmée ACTIVE" in modes


def test_demandes_aidees(services):
    """Une demande aboutie grâce à une règle : une action réelle sur l'un de ses téléchargements, pas un verdict simulé."""
    def action(dry_run, decision="rename"):
        with app.get_db() as conn:
            conn.execute("INSERT INTO assist_log (download_id, arr, title, decision, details, dry_run, rule) "
                         "VALUES ('ABC', 'radarr', 'No Other Land', ?, '', ?, 2)", (decision, dry_run))
    action(1)
    action(0, "ignored:le titre de la release ne désigne pas ce média à lui seul")
    assert etat.calculer(MAINTENANT)["demandes"]["aidees"] == 0
    action(0)
    assert etat.calculer(MAINTENANT)["demandes"]["aidees"] == 1
