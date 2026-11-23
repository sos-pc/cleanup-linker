"""
Seuil du français à l'échelle de chaque profil. Le profil Opti note l'audio français +20000 ; le seuil fixe de 50,
fait pour testtesttest, y laissait un pack anglais (environ 3000) remplacer un pack partiel français. Faux Sonarr,
Prowlarr et qBittorrent.
"""
import re
from contextlib import contextmanager

import pytest
from faux import FauxQbit, faux_arr
from test_trous import contexte, journal, stugo  # noqa: F401  (stugo : fixture du cas StuGo)

import app

SERIE = 491
FRANCAIS = {"id": 1, "name": "Opti · Audio français",
            "specifications": [{"implementation": "LanguageSpecification", "negate": False, "required": False,
                                "fields": [{"name": "value", "value": 2}, {"name": "exceptLanguage", "value": False}]}]}
PACK = {"id": 2, "name": "Opti · Pack de saison",
        "specifications": [{"implementation": "ReleaseTypeSpecification", "negate": False, "required": True,
                            "fields": [{"name": "value", "value": 3}]}]}
VO_VF = {"id": 3, "name": "Language: Original + French",
         "specifications": [{"implementation": "LanguageSpecification", "negate": False, "required": True,
                             "fields": [{"name": "value", "value": -2}]},
                            {"implementation": "LanguageSpecification", "negate": False, "required": True,
                             "fields": [{"name": "value", "value": 2}]}]}
PAS_FRANCAIS = {"id": 4, "name": "Not French",
                "specifications": [{"implementation": "LanguageSpecification", "negate": True, "required": False,
                                    "fields": [{"name": "value", "value": 2}]}]}
SAUF_FRANCAIS = {"id": 5, "name": "Tout sauf le français",
                 "specifications": [{"implementation": "LanguageSpecification", "negate": False, "required": False,
                                     "fields": [{"name": "value", "value": 2}, {"name": "exceptLanguage", "value": True}]}]}
VFF = {"id": 6, "name": "VFF",
       "specifications": [{"implementation": "ReleaseTitleSpecification", "negate": False, "required": False,
                           "fields": [{"name": "value", "value": r"\b(TRUEFRENCH|VFF)\b"}]}]}
OPTI = {"items": [{"quality": {"id": 3}, "allowed": True}], "minFormatScore": -10000,
        "formatItems": [{"format": 1, "score": 20000}, {"format": 2, "score": 200}]}
SEUIL_OPTI = 15000         # les trois quarts de 20000
DEFAUT = 50                # IMPORT_ASSIST_PACK_MIN_SCORE, fait pour testtesttest
FRANCAIS_1080P = 23300     # pack français 1080p x265, hors bonus de pack
ANGLAIS_1080P = 3300       # le même sans français


def profil(formats, notes):
    routes = {("GET", "qualityprofile/7"): {"items": [], "minFormatScore": 0,
                                            "formatItems": [{"format": i, "score": s} for i, s in notes.items()]},
              ("GET", "customformat"): formats}
    return routes


def test_le_seuil_d_opti_est_les_trois_quarts_de_son_score_du_francais(monkeypatch):
    monkeypatch.setattr(app, "_arr_request", faux_arr(profil([FRANCAIS, PACK], {1: 20000, 2: 200}), []))
    p = app._quality_profile("u", "k", 7)
    assert p["seuil_fr"] == SEUIL_OPTI and p["bonus_pack"] == {2: 200}


def test_testtesttest_garde_le_seuil_de_50(monkeypatch):
    """Ni « Original + French » (deux conditions) ni « Not French » (négation) ni un titre ne portent le seuil."""
    monkeypatch.setattr(app, "_arr_request", faux_arr(profil([VO_VF, PAS_FRANCAIS, VFF], {3: 150, 4: -20, 6: 110}), []))
    assert app._quality_profile("u", "k", 7)["seuil_fr"] == app.IMPORT_ASSIST_PACK_MIN_SCORE == DEFAUT


@pytest.mark.parametrize("formats, notes", [
    ([FRANCAIS], {1: -5}),             # un format français pénalisé ne fixe pas de seuil
    ([FRANCAIS], {}),                   # ni un format à 0, absent des notes
    ([SAUF_FRANCAIS], {5: 20000}),      # « tout sauf le français » n'est pas le français
    ([PAS_FRANCAIS], {4: 100}),         # ni « pas de français », même bien noté
])
def test_seul_un_format_francais_note_positivement_compte(monkeypatch, formats, notes):
    monkeypatch.setattr(app, "_arr_request", faux_arr(profil(formats, notes), []))
    assert app._quality_profile("u", "k", 7)["seuil_fr"] == DEFAUT


def test_sans_profil_le_seuil_par_defaut(monkeypatch):
    monkeypatch.setattr(app, "_arr_request", faux_arr({}, []))
    assert app._quality_profile("u", "k", None)["seuil_fr"] == DEFAUT


# ── Règle 4 sur l'échelle d'Opti ─────────────────────────────────────────────

def lecture(titre):
    """Le parseur de Sonarr, réduit, avec les scores d'Opti : français 20000, 1080p 3000, x265 300, pack 200."""
    m = re.search(r"S01E(\d\d)", titre)
    if m:  # un fichier du .torrent
        n = int(m.group(1))
        return {"series": {"id": SERIE}, "episodes": [{"id": 3000 + n, "seasonNumber": 1}],
                "parsedEpisodeInfo": {"seriesTitle": "Haunted Hotel", "seasonNumber": 1, "episodeNumbers": [n]}}
    francais = "VFF" in titre
    score = (20000 if francais else 0) + 3000 + 300 + 200
    formats = [{"id": 1}, {"id": 2}] if francais else [{"id": 2}]
    return {"series": {"id": SERIE}, "customFormatScore": score, "customFormats": formats,
            "parsedEpisodeInfo": {"seriesTitle": "Haunted Hotel", "seasonNumber": 1, "fullSeason": True,
                                  "quality": {"quality": {"id": 3}}}}


@pytest.fixture
def prowlarr(monkeypatch):
    etat = {"titre": ""}
    monkeypatch.setattr(app, "_arr_request", faux_arr({
        ("GET", "parse"): lambda kw: lecture(kw["params"]["title"]),
        ("GET", "qualityprofile/7"): OPTI,
        ("GET", "customformat"): [FRANCAIS, PACK],
    }, []))
    monkeypatch.setattr(app, "PROWLARR_URL", "http://prowlarr.invalid")
    monkeypatch.setattr(app, "PROWLARR_API_KEY", "tests")
    monkeypatch.setattr(app, "_prowlarr_search", lambda requete: [
        {"title": etat["titre"], "protocol": "torrent", "downloadUrl": "http://prowlarr.invalid/1", "seeders": 9,
         "indexer": "nCore"}])

    def torrent(lien):
        noms = [f"Haunted.Hotel.S01E{n:02d}.mkv" for n in range(1, 4)]
        return app._bencode({"info": {"name": etat["titre"], "piece length": 16384, "pieces": b"\0" * 20,
                                      "files": [{"length": 1000, "path": [n]} for n in noms]}})

    monkeypatch.setattr(app, "_fetch_torrent", torrent)
    qb = FauxQbit()

    @contextmanager
    def session():
        yield qb

    monkeypatch.setattr(app, "qbit_session", session)
    return etat


def candidat(prowlarr, partiel, complet):
    prowlarr["titre"] = complet
    p = app._quality_profile("u", "k", 7)
    ref = app._release_ref("u", "k", partiel, p)
    episodes = [{"id": 3000 + n, "seasonNumber": 1, "episodeNumber": n} for n in range(1, 4)]
    serie = {"id": SERIE, "title": "Haunted Hotel", "qualityProfileId": 7, "alternateTitles": []}
    return app._pack_candidate("u", "k", serie, 1, episodes, {3001, 3002, 3003}, ref, p, set()), ref


def test_un_pack_anglais_ne_remplace_pas_un_partiel_francais(prowlarr):
    """Il ne sert qu'à combler les trous : le partiel français est gardé."""
    (cand, _), ref = candidat(prowlarr, "Haunted.Hotel.S01.MULTi.VFF.1080p.WEB.x265-TyHD",
                              "Haunted.Hotel.S01.1080p.NF.WEB-DL.DDP5.1.x265-GRP")
    assert ref["score"] == FRANCAIS_1080P  # le bonus de pack ne compte pas
    assert cand is not None and cand["score"] == ANGLAIS_1080P and cand["vo"] is True


def test_un_pack_francais_complet_remplace_le_partiel(prowlarr):
    (cand, _), _ = candidat(prowlarr, "Haunted.Hotel.S01.MULTi.VFF.1080p.WEB.x265-TyHD",
                            "Haunted.Hotel.S01.MULTi.VFF.1080p.WEB.x265-GL0P")
    assert cand is not None and cand["vo"] is False


def test_sous_un_partiel_deja_sans_francais_son_score_reste_le_seuil(prowlarr):
    (cand, _), ref = candidat(prowlarr, "Haunted.Hotel.S01.1080p.WEB.x265-AAA", "Haunted.Hotel.S01.1080p.WEB.x265-BBB")
    assert ref["score"] == ANGLAIS_1080P and cand is not None and cand["vo"] is False


# ── Saisons trouées sur l'échelle d'Opti (le cas StuGo, profil Opti) ────────

def test_saisons_trouees_un_pack_anglais_comble_en_vo(stugo, monkeypatch):  # noqa: F811 (fixture importée)
    """Sans pack partiel, la référence est le seuil du français du profil : un pack anglais ne fait que combler."""
    avant = app._arr_request

    def arr(method, base_url, api_key, endpoint, **kwargs):
        if endpoint == "qualityprofile/7":
            return dict(OPTI, items=[{"quality": {"id": 5}, "allowed": True}, {"quality": {"id": 3}, "allowed": True}])
        if endpoint == "customformat":
            return [FRANCAIS, PACK]
        r = avant(method, base_url, api_key, endpoint, **kwargs)
        if endpoint == "parse" and (r.get("parsedEpisodeInfo") or {}).get("fullSeason"):
            r = dict(r, customFormatScore=3500, customFormats=[{"id": 2}])  # 720p anglais : 3300 hors pack
        return r

    monkeypatch.setattr(app, "_arr_request", arr)
    verdicts, _ = app.run_season_holes(contexte())
    assert verdicts[0]["decision"] == "pack-hole-fill"
    [j] = journal()
    assert "pris en VO dans" in j["details"] and "score 3300" in j["details"]
