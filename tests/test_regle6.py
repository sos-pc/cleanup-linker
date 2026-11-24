"""
Règle 6 : une course ne sert qu'une demande qui attend, un film ou au moins un épisode surveillé et sans fichier.
Cas réel du 2026-11-24 : des épisodes de Pokémon Horizons rangés par un alias en saison 20, non surveillée,
ont lancé 28 courses et autant de prises mortes.
"""
import json
import time

import pytest

import app
from faux import faux_arr

DL = "a" * 40
SERIE, FILM = 494, 714


@pytest.fixture
def regle6(monkeypatch):
    """Faux Sonarr et Radarr par route ; la comparaison de qualité et l'historique des prises sont neutres."""
    with app.get_db() as conn:
        conn.execute("DELETE FROM dead_race")
    etat = {"serie": {"id": SERIE, "monitored": True}, "episodes": [], "film": {"id": FILM, "monitored": True, "hasFile": False},
            "appels": []}
    alternative = {"protocol": "torrent", "guid": "g-alt", "indexer": "C411 (Prowlarr)", "indexerId": 3, "infoHash": "b" * 40,
                   "title": "Autre.Version.1080p", "rejections": [], "seeders": 4, "customFormatScore": 10}
    routes = {
        ("GET", f"series/{SERIE}"): lambda kw: etat["serie"],
        ("GET", "episode"): lambda kw: etat["episodes"],
        ("GET", f"movie/{FILM}"): lambda kw: etat["film"],
        ("GET", "release"): lambda kw: [dict(alternative, mappedEpisodeInfo=[{"id": e["id"]} for e in etat["episodes"]],
                                             mappedMovieId=FILM)],
        ("POST", "release"): {},
    }
    monkeypatch.setattr(app, "_arr_request", faux_arr(routes, etat["appels"]))
    monkeypatch.setattr(app, "_library_better", lambda *a, **k: "")
    monkeypatch.setattr(app, "_race_grabs", lambda url, cle, hashes: (set(), set()))
    monkeypatch.setattr(app, "IMPORT_ASSIST_MORT_ESSAIS", 5)
    return etat


def _torrent():
    return {"hash": DL, "name": "Pokemon S25E14 NOTAG", "progress": 0, "downloaded": 0, "completed": 0,
            "seen_complete": -1, "added_on": time.time() - 20 * 3600, "state": "stalledDL"}


def _essai(arr, episodes=(), course=None, dry=False):
    if arr == "sonarr":
        records = [{"title": "Pokemon S25E14 NOTAG", "seriesId": SERIE, "episodeId": e, "indexer": "TR4KER"} for e in episodes]             or [{"title": "Pokemon S25E14 NOTAG", "seriesId": SERIE, "indexer": "TR4KER"}]
    else:
        records = [{"title": "Parking.mkv", "movieId": FILM, "indexer": "TR4KER"}]
    ctx = {"torrents": {DL: _torrent()}, "apercu": False}
    return app._race_try(arr, "http://arr.invalid", "cle", DL, records, course, 20 * 3600, dry, ctx)[0]


def _cherche(etat):
    return [a for a in etat["appels"] if a[1] == "release"]


def _courses():
    with app.get_db() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM dead_race")]


def _ep(id_, surveille, fichier):
    return {"id": id_, "monitored": surveille, "hasFile": fichier}


@pytest.mark.parametrize("episodes,raison", [
    ([_ep(1, False, False)], "l'épisode n'est pas surveillé"),
    ([_ep(1, True, True)], "l'épisode a déjà un fichier"),
    ([_ep(1, False, False), _ep(2, False, False)], "ses épisodes ne sont pas surveillés"),
    ([_ep(1, True, True), _ep(2, True, True)], "ses épisodes ont déjà un fichier"),
    ([_ep(1, True, True), _ep(2, False, False)], "1 de ses 2 épisodes ont déjà un fichier, et les autres ne sont pas surveillés"),
])
def test_rien_ne_l_attend_rien_n_est_cherche(regle6, episodes, raison):
    regle6["episodes"] = episodes
    verdict = _essai("sonarr", [e["id"] for e in episodes])
    assert verdict["decision"] == "dead-wait"
    assert f"mais {raison} : aucune autre version cherchée" in verdict["reason"]
    assert not _cherche(regle6) and not _courses()


def test_serie_non_surveillee(regle6):
    regle6["serie"] = {"id": SERIE, "monitored": False}
    regle6["episodes"] = [_ep(1, True, False)]
    verdict = _essai("sonarr", [1])
    assert "la série n'est pas surveillée" in verdict["reason"] and not _cherche(regle6)


def test_un_episode_attendu_suffit(regle6):
    """Un pack dont un seul épisode manque sert encore la demande : la course part."""
    regle6["episodes"] = [_ep(1, True, True), _ep(2, True, False)]
    verdict = _essai("sonarr", [1, 2])
    assert verdict["decision"] == "dead-race"
    assert ("POST", "release", None, {"guid": "g-alt", "indexerId": 3}) in regle6["appels"]
    assert [c["original"] for c in _courses()] == [DL]


@pytest.mark.parametrize("film,raison", [
    ({"id": FILM, "monitored": False, "hasFile": False}, "le film n'est pas surveillé"),
    ({"id": FILM, "monitored": True, "hasFile": True}, "le film a déjà un fichier"),
])
def test_film_sans_objet(regle6, film, raison):
    regle6["film"] = film
    verdict = _essai("radarr")
    assert verdict["decision"] == "dead-wait" and f"mais {raison}" in verdict["reason"]
    assert not _cherche(regle6) and not _courses()


def test_film_attendu(regle6):
    verdict = _essai("radarr")
    assert verdict["decision"] == "dead-race" and len(_courses()) == 1


def _course_ouverte():
    with app.get_db() as conn:
        conn.execute("INSERT INTO dead_race (arr, media_id, episodes, original, hashes, tried, last_try) "
                     "VALUES ('sonarr', ?, '[1]', ?, ?, '[\"Essai.1\"]', 0)", (SERIE, DL, json.dumps([DL])))
        return conn.execute("SELECT * FROM dead_race WHERE original = ?", (DL,)).fetchone()


def test_course_sans_objet_close(regle6):
    """L'épisode a reçu un fichier d'ailleurs : la course n'a plus d'objet, elle est close sans rien chercher."""
    course = _course_ouverte()
    regle6["episodes"] = [_ep(1, True, True)]
    verdict = _essai("sonarr", [1], course=course)
    assert verdict["reason"].endswith("aucune autre version cherchée ; la course est close")
    assert not _cherche(regle6) and _courses()[0]["done"] == 1


def test_course_sans_objet_en_simulation(regle6):
    course = _course_ouverte()
    regle6["episodes"] = [_ep(1, False, False)]
    verdict = _essai("sonarr", [1], course=course, dry=True)
    assert verdict["decision"] == "dead-wait" and "course est close" not in verdict["reason"]
    assert _courses()[0]["done"] == 0 and _courses()[0]["last_try"] == 0


def test_course_qui_attend_encore(regle6):
    """Une course dont l'épisode manque toujours continue : la version suivante est prise."""
    course = _course_ouverte()
    regle6["episodes"] = [_ep(1, True, False)]
    verdict = _essai("sonarr", [1], course=course)
    assert verdict["decision"] == "dead-race"
    assert _courses()[0]["done"] == 0 and json.loads(_courses()[0]["tried"]) == ["Essai.1", "Autre.Version.1080p"]


def test_episodes_inconnus(regle6):
    """Sans épisode connu, la règle ne juge pas : comme avant, rien ne couvre l'original, on attend."""
    regle6["episodes"] = [_ep(1, False, False)]
    verdict = _essai("sonarr", [])
    assert verdict["decision"] == "dead-wait" and "aucune autre version trouvée" in verdict["reason"]
    # Sans épisode à juger, ni la série ni ses épisodes (1 300 pour Pokémon) ne sont lus.
    assert not [a for a in regle6["appels"] if a[1] in ("episode", f"series/{SERIE}")]
