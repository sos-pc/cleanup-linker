"""
Le cas Pokémon du 21 novembre 2026. La version Netflix de la saison 1 (« Pokemon The Series ») : Sonarr ne la relie
que par la prise, et elle a retiré l'épisode interdit S01E18 « Beauty and the Beach », ce qui décale tous les suivants.
Ses vidéos sont importées par le titre de leur épisode (règle 2). La règle 4 admet les titres que Sonarr connaît quand
il ne reconnaît pas celui du pack partiel, sans rouvrir le piège Monster ni prendre Pokémon Horizons pour la saison 1.
"""
import re
from contextlib import contextmanager

import pytest
from faux import FauxQbit, faux_arr

import app

SERIE = 494
DL = "0c1ec103" + "0" * 32
# TVDB : l'épisode 18 existe ; Netflix l'a retiré et numérote les suivants à la suite.
TVDB = {15: "Battle Aboard the St. Anne", 16: "Pokémon Shipwreck", 17: "Island of the Giant Pokémon",
        18: "Beauty and the Beach", 19: "Tentacool & Tentacruel", 20: "The Ghost of Maiden's Peak",
        21: "Bye Bye Butterfree", 22: "Abra and the Psychic Showdown", 23: "The Tower of Terror"}
NETFLIX = {15: "Battle.Aboard.the.St.Anne", 16: "Pokemon.Shipwreck", 17: "Island.of.the.Giant.Pokemon",
           18: "Tentacool.and.Tentacruel", 19: "The.Ghost.of.Maiden.s.Peak", 20: "Bye.Bye.Butterfree",
           21: "Abra.and.the.Psychic.Showdown", 22: "FiNAL.The.Tower.of.Terror"}


def nom_netflix(n, titre):
    return f"Pokemon.The.Series.Indigo.League.S01E{n:02d}.{titre}.MULTi.1080p.NF.WEB.DDP.AV1-BTT.mkv"


def episodes_tvdb(avec_fichier=()):
    return [{"id": 2000 + n, "seasonNumber": 1, "episodeNumber": n, "title": titre, "hasFile": n in avec_fichier}
            for n, titre in TVDB.items()]


@pytest.fixture
def netflix(monkeypatch, tmp_path):
    """Le pack Netflix téléchargé, un faux Sonarr ; l'import manuel est noté, rien n'est appelé."""
    dossier = tmp_path / "Pokemon.The.Series.S01.Indigo.League.MULTi.1080p.NF.WEB.DDP.AV1-BTT"
    dossier.mkdir()
    etat = {"fichiers": dict(NETFLIX), "episodes": episodes_tvdb(), "commandes": [], "dossier": dossier}

    def ecrire():
        for f in dossier.iterdir():
            f.unlink()
        for n, titre in etat["fichiers"].items():
            (dossier / nom_netflix(n, titre)).write_bytes(b"x")

    etat["ecrire"] = ecrire
    routes = {
        ("GET", "episode"): lambda kw: etat["episodes"],
        ("GET", "manualimport"): lambda kw: [{"path": str(f), "quality": {"quality": {"id": 3}}, "languages": [{"id": 2}],
                                              "releaseGroup": "BTT"} for f in sorted(dossier.iterdir())],
        ("POST", "command"): lambda kw: etat["commandes"].append(kw["json"]) or {},
    }
    monkeypatch.setattr(app, "_arr_request", faux_arr(routes, []))
    monkeypatch.setattr(app, "IMPORT_ASSIST_NOM_DRY_RUN", False)
    monkeypatch.setattr(app, "IMPORT_ASSIST_TITRES_DRY_RUN", False)
    etat["torrent"] = {"hash": DL, "name": dossier.name, "content_path": str(dossier), "save_path": str(tmp_path)}
    etat["records"] = [{"episodeId": 2000 + n, "downloadId": DL.upper(), "title": dossier.name} for n in TVDB]
    return etat


def importer(etat, ecartees=None):
    etat["ecrire"]()
    ctx = {"torrents": {}, "categories": {}, "dry_run": False, "apercu": False, "ecartees": ecartees or {}}
    verdict = {"arr": "sonarr", "download_id": DL, "title": "Pokemon"}
    return app._import_by_titles("http://sonarr.invalid", "tests", DL, etat["records"], etat["torrent"], SERIE, ctx, verdict)


def ranges(etat):
    """Chaque vidéo importée → l'épisode TVDB sous lequel elle est rangée."""
    [commande] = etat["commandes"]
    assert (commande["name"], commande["importMode"]) == ("ManualImport", "copy")
    return {f["path"].split("S01E")[1][:2]: f["episodeIds"][0] - 2000 for f in commande["files"]}


# ── Règle 2 : import par titre d'épisode ─────────────────────────────────────

def test_chaque_video_netflix_rangee_sous_son_episode(netflix):
    verdict, nouveau = importer(netflix)
    assert nouveau and verdict["decision"] == "import-titles"
    # Jusqu'à 17, les numéros coïncident ; ensuite, Netflix a un épisode de retard sur TVDB.
    assert ranges(netflix) == {"15": 15, "16": 16, "17": 17, "18": 19, "19": 20, "20": 21, "21": 22, "22": 23}
    assert "5 rangée(s) sous un autre numéro que celui de leur nom" in verdict["reason"]
    assert "S01E19 « Tentacool & Tentacruel »" in verdict["reason"]


def test_simulation_rien_n_est_importe(netflix, monkeypatch):
    monkeypatch.setattr(app, "IMPORT_ASSIST_TITRES_DRY_RUN", True)
    verdict, _ = importer(netflix)
    assert verdict["decision"] == "import-titles" and netflix["commandes"] == []


def test_pas_deux_fois_en_24_h(netflix):
    importer(netflix)
    assert importer(netflix)[1] is False
    assert len(netflix["commandes"]) == 1


def test_trop_peu_de_titres_reconnus(netflix):
    """En dessous de 80 % des vidéos reconnues, le titre ne prouve rien : rien n'est importé."""
    for n in (15, 16, 17):
        netflix["fichiers"][n] = "Episode.Sans.Titre.Connu"
    assert importer(netflix) is None
    assert netflix["commandes"] == []


def test_deux_videos_pour_un_meme_episode(netflix):
    netflix["fichiers"][17] = "Pokemon.Shipwreck"
    assert importer(netflix) is None


def test_episode_deja_pourvu_ou_hors_de_la_prise(netflix):
    netflix["episodes"] = episodes_tvdb(avec_fichier={16})
    netflix["records"] = [r for r in netflix["records"] if r["episodeId"] != 2023]
    verdict, _ = importer(netflix)
    importes = ranges(netflix)
    assert 16 not in importes.values() and 23 not in importes.values()
    assert verdict["reason"].startswith("7/8 vidéo(s) reconnue(s) au titre exact de leur épisode, 6 importée(s)")


def test_saison_ecartee_par_la_regle_5(netflix):
    assert importer(netflix, ecartees={SERIE: {1}}) is None


@pytest.mark.parametrize("nom,attendu", [
    ("Show.S01E19.The.Ghost.of.Maiden.s.Peak.1080p.mkv", 20),   # le plus long des titres qui conviennent
    ("Show.S01E18.Tentacool.and.Tentacruel.mkv", 19),            # « & » lu « and »
    ("Show.S01E22.FiNAL.The.Tower.of.Terror.mkv", 23),           # mot de scène avant le titre
    ("Show.S01E21-E22.Abra.and.the.Psychic.Showdown.mkv", None),  # plusieurs épisodes : pas de titre unique
    ("Show.S01E05.Bye.mkv", None),                               # trop court pour prouver quelque chose
    ("Show.Bye.Bye.Butterfree.mkv", None),                       # sans SxxEyy
])
def test_episode_par_titre(nom, attendu):
    par_cle = {}
    for e in episodes_tvdb() + [{"id": 2099, "seasonNumber": 1, "episodeNumber": 99, "title": "The Ghost"},
                                {"id": 2098, "seasonNumber": 1, "episodeNumber": 98, "title": "Bye"}]:
        par_cle.setdefault(app._titre_cle(e["title"]), []).append(e)
    e = app._episode_by_title(nom, par_cle)
    assert (e["id"] - 2000 if e else None) == attendu


def test_meme_titre_dans_deux_saisons_departage_par_la_saison():
    par_cle = {app._titre_cle("Pokémon Emergency"): [{"id": 1, "seasonNumber": 1, "title": "Pokémon Emergency"},
                                                      {"id": 2, "seasonNumber": 7, "title": "Pokémon Emergency"}]}
    assert app._episode_by_title("Show.S07E03.Pokemon.Emergency.mkv", par_cle)["id"] == 2


def test_misnamed_essaie_les_titres_sans_titre_de_serie(monkeypatch):
    """Sans titre de la série dans le nom, la règle 2 tente l'import par titre avant d'abandonner (Sonarr seul)."""
    appels = []
    monkeypatch.setattr(app, "_arr_request", faux_arr({
        ("GET", "history"): {"records": [{"eventType": "grabbed", "sourceTitle": "Pokemon.The.Series.S01.MULTi.FRENCH.1080p.NF.WEB-BTT",
                                          "date": "2026-11-21T09:58:00Z"}]},
        ("GET", "parse"): {},
    }, []))
    monkeypatch.setattr(app, "_suspects", lambda dl: [])
    monkeypatch.setattr(app, "_id_confirmee", lambda *a: "aucun titre connu de Pokémon dans le nom")
    monkeypatch.setattr(app, "_import_by_titles", lambda *a: appels.append(a) or ({"decision": "import-titles"}, True))
    ctx = {"torrents": {}, "categories": {}, "dry_run": False, "apercu": False, "ecartees": {}}

    def ecarte(raison, regle, dry):
        return {"decision": "ignored", "reason": raison}, True

    records = [{"downloadId": DL.upper(), "episodeId": 2015, "title": "Pokemon"}]
    torrent = {"hash": DL, "name": "Pokemon.The.Series.S01.Indigo.League.MULTi.1080p.NF.WEB.DDP.AV1-BTT"}
    verdict, _ = app._misnamed("sonarr", "u", "k", DL, records, torrent, SERIE, ctx, {}, ecarte)
    assert verdict["decision"] == "import-titles" and len(appels) == 1
    verdict, _ = app._misnamed("radarr", "u", "k", DL, records, torrent, SERIE, ctx, {}, ecarte)
    assert verdict["decision"] == "ignored" and len(appels) == 1


# ── Règle 4 : titres admis pour un pack complet ──────────────────────────────

POKEMON = {"id": SERIE, "title": "Pokémon", "qualityProfileId": 7, "alternateTitles": [
    {"title": "Pokemon", "seasonNumber": -1}, {"title": "Pokemon Indigo League", "sceneSeasonNumber": 1},
    {"title": "Pokemon Horizons", "sceneSeasonNumber": 25}]}
MONSTER = {"id": 459, "title": "Monster", "qualityProfileId": 7, "alternateTitles": [
    {"title": "Monster The Lizzie Borden Story", "seasonNumber": -1}]}


def lecture(titre):
    """Le parseur de Sonarr, réduit : il ne reconnaît pas « Pokemon The Series »."""
    serie_lue = re.split(r"\.S\d\d", titre)[0].replace(".", " ")
    connue = {"Pokemon": SERIE, "Pokemon Horizons": SERIE, "Monster": 459, "Monster The Lizzie Borden Story": 459}
    m = re.search(r"S(\d\d)E(\d\d)", titre)
    serie = {"id": connue[serie_lue]} if serie_lue in connue else None
    if m:
        return {"series": serie, "episodes": [{"id": 3000 + int(m.group(2)), "seasonNumber": 1}],
                "parsedEpisodeInfo": {"seriesTitle": serie_lue, "seasonNumber": 1, "episodeNumbers": [int(m.group(2))]}}
    return {"series": serie, "customFormatScore": 110, "customFormats": [],
            "parsedEpisodeInfo": {"seriesTitle": serie_lue, "seasonNumber": 1, "fullSeason": True,
                                  "quality": {"quality": {"id": 3}}}}


@pytest.fixture
def prowlarr(monkeypatch):
    etat = {"titre": ""}
    monkeypatch.setattr(app, "_arr_request", faux_arr({
        ("GET", "parse"): lambda kw: lecture(kw["params"]["title"]),
        ("GET", "qualityprofile/7"): {"items": [{"quality": {"id": 3}, "allowed": True}], "minFormatScore": -20,
                                      "formatItems": []},
    }, []))
    monkeypatch.setattr(app, "PROWLARR_URL", "http://prowlarr.invalid")
    monkeypatch.setattr(app, "PROWLARR_API_KEY", "tests")
    monkeypatch.setattr(app, "_prowlarr_search", lambda requete: [
        {"title": etat["titre"], "protocol": "torrent", "downloadUrl": "http://prowlarr.invalid/1", "seeders": 9,
         "indexer": "C411"}])
    def torrent(lien):
        noms = [f"{etat['titre'].split('.S01')[0]}.S01E{n:02d}.mkv" for n in range(1, 4)]
        return app._bencode({"info": {"name": etat["titre"], "piece length": 16384, "pieces": b"\0" * 20,
                                      "files": [{"length": 1000, "path": [n]} for n in noms]}})

    monkeypatch.setattr(app, "_fetch_torrent", torrent)
    qb = FauxQbit()

    @contextmanager
    def session():
        yield qb

    monkeypatch.setattr(app, "qbit_session", session)
    return etat


def candidat(prowlarr, serie, partiel, complet):
    prowlarr["titre"] = complet
    profil = app._quality_profile("u", "k", 7)
    ref = app._release_ref("u", "k", partiel, profil)
    episodes = [{"id": 3000 + n, "seasonNumber": 1, "episodeNumber": n} for n in range(1, 4)]
    return app._pack_candidate("u", "k", serie, 1, episodes, {3001, 3002, 3003}, ref, profil, set())


def test_titre_du_partiel_inconnu_de_sonarr_les_siens_sont_admis(prowlarr):
    cand, ecartes = candidat(prowlarr, POKEMON, "Pokemon.The.Series.S01.MULTi.1080p.NF.WEB-BTT", "Pokemon.S01.MULTi.1080p.WEB-GRP")
    assert cand is not None and cand["title"] == "Pokemon.S01.MULTi.1080p.WEB-GRP"


def test_monster_reste_protege(prowlarr):
    """« Monster The Lizzie Borden Story » est un titre que Sonarr connaît : il reste seul juge."""
    cand, ecartes = candidat(prowlarr, MONSTER, "Monster.The.Lizzie.Borden.Story.S01.1080p.WEB-GRP", "Monster.S01.1080p.WEB-GRP")
    assert cand is None and any("sous un autre titre" in e for e in ecartes)


def test_horizons_n_est_pas_la_saison_1_de_pokemon(prowlarr):
    cand, ecartes = candidat(prowlarr, POKEMON, "Pokemon.The.Series.S01.MULTi.1080p.NF.WEB-BTT", "Pokemon.Horizons.S01.1080p.WEB-GRP")
    assert cand is None and any("sous un autre titre" in e for e in ecartes)


def test_requetes_de_pack_de_la_bonne_saison_sans_accents():
    serie = dict(POKEMON, alternateTitles=POKEMON["alternateTitles"] + [
        {"title": "Pokemon Johto League Champions", "sceneSeasonNumber": 4}])
    assert app._pack_queries(serie, 4) == ["Pokemon S04", "Pokemon Johto League Champions S04"]
    assert app._pack_queries(serie, 1) == ["Pokemon S01", "Pokemon Indigo League S01"]
