"""
L'assistant d'import de bout en bout, avec de faux Sonarr, Radarr et qBittorrent :
règle 2 élargie (identité confirmée) et règle 7 (release piégée), en simulation et en vrai.
"""
import time

import pytest
from faux import faux_arr

import app

DL = "a" * 40
NOM_CELLES = "Celles.Qu.On.N.A.Pas.Eues.1981.TRUEFRENCH.1080p.CNLP.WEB.x264.EAC3-STORiES"
REFAIT_CELLES = "The Ones That Got Away 1981.TRUEFRENCH.1080p.CNLP.WEB.x264.EAC3-STORiES"
NOM_PIEGE = "South Park S29E06 1080p WEB H264-MeGusta"


def il_y_a(secondes: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - secondes))


def contexte(torrents: dict, apercu: bool = False) -> dict:
    return {"torrents": torrents, "categories": {}, "dry_run": False, "apercu": apercu, "ecartees": {}}


def journal(decision: str) -> list:
    with app.get_db() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT decision, dry_run, rule, details FROM assist_log WHERE decision = ?", (decision,))]


@pytest.fixture
def qbit(monkeypatch, tmp_path):
    """Un faux qBittorrent : les renommages et mises en corbeille sont notés, rien n'est appelé."""
    notes = {"post": [], "corbeille": [], "suspects": [], "videos": []}
    video = tmp_path / f"{NOM_CELLES}.mkv"
    video.write_bytes(b"0" * 1024)
    notes["videos"] = [str(video)]
    monkeypatch.setattr(app, "_qbit_post", lambda action, data: notes["post"].append((action, data)))
    monkeypatch.setattr(app, "_assist_trash", lambda t, arreter=True: notes["corbeille"].append((t["hash"], arreter)) or True)
    monkeypatch.setattr(app, "_suspects", lambda dl: list(notes["suspects"]))
    monkeypatch.setattr(app, "_torrent_videos", lambda t: list(notes["videos"]))
    monkeypatch.setattr(app, "scan_video_files", lambda dossier: {})
    monkeypatch.setattr(app, "SEERR_URL", "")
    return notes


# ── Règle 2 élargie : Celles qu'on n'a pas eues ───────────────────────────────

def parse_radarr(kwargs):
    """Radarr ne reconnaît que son propre titre : « N.A » contre « n'a » fait échouer le titre français."""
    titre = kwargs["params"]["title"]
    film = {"id": 704, "title": "The Ones That Got Away", "year": 1981} if titre.startswith("The Ones That Got Away 1981") else None
    return {"movie": film, "parsedMovieInfo": {"quality": {"quality": {"id": 3, "name": "WEBDL-1080p"}}},
            "customFormatScore": 10}


def routes_celles(release: str = NOM_CELLES) -> dict:
    return {
        ("GET", "movie/704"): {"id": 704, "title": "The Ones That Got Away", "originalTitle": "Celles qu'on n'a pas eues",
                               "year": 1981, "runtime": 105, "alternateTitles": [], "qualityProfileId": 1,
                               "path": "/data/Media/Rays_Le _Chat/The Ones That Got Away (1981)"},
        ("GET", "history"): {"records": [{"eventType": "grabbed", "sourceTitle": release, "date": il_y_a(3600), "movieId": 704}]},
        ("GET", "parse"): parse_radarr,
        ("GET", "qualitydefinition"): [{"quality": {"name": "WEBDL-1080p"}, "minSize": 0}],
        ("GET", "moviefile"): [],
        ("GET", "qualityprofile/1"): {"items": []},
    }


def file_radarr(nom: str) -> list:
    return [{"id": 1, "downloadId": DL.upper(), "movieId": 704, "title": nom, "downloadClient": "Rays_Le _Chat",
             "trackedDownloadState": "importBlocked", "trackedDownloadStatus": "warning",
             "statusMessages": [{"title": nom, "messages": [
                 "Found matching movie via grab history, but release was matched to movie by ID. Manual Import required."]}]}]


def lancer(arr, records, torrents, ctx):
    return app._assist_download(arr, "http://arr.invalid", "cle", DL, records, [], {}, ctx)


def test_celles_renomme_pour_que_radarr_importe(monkeypatch, qbit):
    appels = []
    monkeypatch.setattr(app, "_arr_request", faux_arr(routes_celles(), appels))
    monkeypatch.setattr(app, "IMPORT_ASSIST_NOM_DRY_RUN", False)
    monkeypatch.setattr(app, "IMPORT_ASSIST_IDENTITE_DRY_RUN", False)
    t = {"hash": DL, "name": f"{NOM_CELLES}.mkv", "progress": 1, "category": "Rays_Le _Chat", "save_path": "/tmp"}
    verdict, nouveau = lancer("radarr", file_radarr(t["name"]), {DL: t}, contexte({DL: t}))
    assert verdict["decision"] == "rename" and nouveau
    assert qbit["post"] == [("rename", {"hash": DL, "name": REFAIT_CELLES})]
    assert "Celles qu'on n'a pas eues" in verdict["reason"]
    assert [(r["dry_run"], r["rule"]) for r in journal("rename")] == [(0, 2)]


def test_celles_en_simulation_ne_renomme_rien(monkeypatch, qbit):
    monkeypatch.setattr(app, "_arr_request", faux_arr(routes_celles(), []))
    monkeypatch.setattr(app, "IMPORT_ASSIST_NOM_DRY_RUN", False)
    monkeypatch.setattr(app, "IMPORT_ASSIST_IDENTITE_DRY_RUN", True)
    t = {"hash": DL, "name": f"{NOM_CELLES}.mkv", "progress": 1, "category": "Rays_Le _Chat", "save_path": "/tmp"}
    verdict, _ = lancer("radarr", file_radarr(t["name"]), {DL: t}, contexte({DL: t}))
    assert verdict["decision"] == "rename" and verdict["release"] == REFAIT_CELLES
    assert qbit["post"] == []
    assert [r["dry_run"] for r in journal("rename")] == [1]


def test_identite_non_confirmee_rien_ne_bouge(monkeypatch, qbit):
    autre = "Un.Autre.Film.1981.TRUEFRENCH.1080p.WEB.x264-GRP"
    monkeypatch.setattr(app, "_arr_request", faux_arr(routes_celles(release=autre), []))
    monkeypatch.setattr(app, "IMPORT_ASSIST_NOM_DRY_RUN", False)
    monkeypatch.setattr(app, "IMPORT_ASSIST_IDENTITE_DRY_RUN", False)
    t = {"hash": DL, "name": f"{autre}.mkv", "progress": 1, "category": "Rays_Le _Chat", "save_path": "/tmp"}
    verdict, _ = lancer("radarr", file_radarr(t["name"]), {DL: t}, contexte({DL: t}))
    assert verdict["decision"] == "ignored"
    assert "aucun titre connu" in verdict["reason"]
    assert qbit["post"] == []


def test_videos_trop_petites_rien_ne_bouge(monkeypatch, qbit):
    routes = routes_celles()
    routes[("GET", "qualitydefinition")] = [{"quality": {"name": "WEBDL-1080p"}, "minSize": 5}]
    monkeypatch.setattr(app, "_arr_request", faux_arr(routes, []))
    monkeypatch.setattr(app, "IMPORT_ASSIST_NOM_DRY_RUN", False)
    monkeypatch.setattr(app, "IMPORT_ASSIST_IDENTITE_DRY_RUN", False)
    t = {"hash": DL, "name": f"{NOM_CELLES}.mkv", "progress": 1, "category": "Rays_Le _Chat", "save_path": "/tmp"}
    verdict, _ = lancer("radarr", file_radarr(t["name"]), {DL: t}, contexte({DL: t}))
    assert verdict["decision"] == "ignored" and "trop petites" in verdict["reason"]
    assert qbit["post"] == []


# ── Règle 7 : release piégée ─────────────────────────────────────────────────

def routes_piege(vrai_hash: str) -> dict:
    return {
        ("GET", "series/10"): {"id": 10, "title": "South Park", "path": "/data/Media/Séries animation/South Park"},
        ("GET", "history"): {"records": []},
        ("GET", "history/series"): [
            # Le grab du piège : son hash annoncé n'est celui d'aucun torrent.
            {"id": 555, "eventType": "grabbed", "sourceTitle": NOM_PIEGE, "date": il_y_a(2 * 86400),
             "downloadId": "B" * 40, "data": {"indexer": "1337x (Prowlarr)"}},
            # Une vraie release du même nom, plus récente : son torrent existe, elle ne doit pas être visée.
            {"id": 556, "eventType": "grabbed", "sourceTitle": NOM_PIEGE, "date": il_y_a(86400),
             "downloadId": vrai_hash.upper(), "data": {"indexer": "Tracker privé"}},
            # Un vieux grab du même nom, hors délai.
            {"id": 554, "eventType": "grabbed", "sourceTitle": NOM_PIEGE, "date": il_y_a(30 * 86400),
             "downloadId": "D" * 40, "data": {"indexer": "1337x (Prowlarr)"}},
        ],
        ("POST", "history/failed/555"): None,
    }


def file_sonarr() -> list:
    return [{"id": 7, "downloadId": DL.upper(), "seriesId": 10, "episodeId": 100, "title": NOM_PIEGE,
             "downloadClient": "Séries animation", "trackedDownloadState": "importBlocked", "trackedDownloadStatus": "warning",
             "statusMessages": [{"title": NOM_PIEGE, "messages": ["Caution: Found potentially dangerous file with extension: .scr"]}]}]


@pytest.mark.parametrize("simulation", [False, True])
def test_release_piegee(monkeypatch, qbit, simulation):
    vrai = "c" * 40
    appels = []
    monkeypatch.setattr(app, "_arr_request", faux_arr(routes_piege(vrai), appels))
    monkeypatch.setattr(app, "IMPORT_ASSIST_PIEGE_DRY_RUN", simulation)
    qbit["videos"] = []
    qbit["suspects"] = [f"{NOM_PIEGE}.scr"]
    t = {"hash": DL, "name": NOM_PIEGE, "progress": 1, "category": "Séries animation", "save_path": "/tmp"}
    torrents = {DL: t, vrai: {"hash": vrai, "name": NOM_PIEGE}}
    verdict, nouveau = lancer("sonarr", file_sonarr(), torrents, contexte(torrents))
    assert verdict["decision"] == "trap" and nouveau
    assert "1337x" in verdict["reason"]
    echecs = [a for a in appels if a[0] == "POST"]
    if simulation:
        assert echecs == [] and qbit["corbeille"] == []
    else:
        assert [a[1] for a in echecs] == ["history/failed/555"]
        assert qbit["corbeille"] == [(DL, True)]
    assert [(r["dry_run"], r["rule"]) for r in journal("trap")] == [(int(simulation), 7)]


def test_archive_seule_n_est_pas_un_piege(monkeypatch, qbit):
    monkeypatch.setattr(app, "_arr_request", faux_arr(routes_piege("c" * 40), []))
    monkeypatch.setattr(app, "IMPORT_ASSIST_PIEGE_DRY_RUN", False)
    qbit["videos"] = []
    qbit["suspects"] = [f"{NOM_PIEGE}.rar"]
    t = {"hash": DL, "name": NOM_PIEGE, "progress": 1, "category": "Séries animation", "save_path": "/tmp"}
    verdict, _ = lancer("sonarr", file_sonarr(), {DL: t}, contexte({DL: t}))
    assert verdict["decision"] == "ignored" and verdict["reason"] == "aucune vidéo dans le torrent"
    assert qbit["corbeille"] == []
