"""Règle 2 élargie (identité confirmée), releases piégées (règle 7), mort-nés (règle 6) : fonctions pures."""
import pytest

import app

CAS_IDENTITE = [
    # (nom de la release, titres connus du média, année du média, reconnu ?)
    ("Celles.Qu.On.N.A.Pas.Eues.1981.TRUEFRENCH.1080p", ["The Ones That Got Away", "Celles qu'on n'a pas eues"], 1981, True),
    ("Pokémon.Film.14.Noir.Victini.et.Reshiram.2011.TRUEFRENCH", ["Pokémon, le film : Noir - Victini et Reshiram"], 2011, True),
    ("Pokémon.Film.14.Blanc.Victini.et.Zekrom.2011.TRUEFRENCH",
     ["Pokémon, le film : Noir - Victini et Reshiram", "Pokémon the Movie: Black - Victini and Reshiram"], 2011, False),
    ("Hunger.Games.La.Revolte.Partie.2.2015.MULTi.1080p",
     ["Hunger Games : La Révolte, partie 1", "The Hunger Games: Mockingjay - Part 1"], 2014, False),
    ("Hunger.Games.La.Revolte.Partie.1.2014.MULTi.1080p", ["Hunger Games : La Révolte, partie 1"], 2014, True),
    ("Ghosts.2019.S06E03.1080p.WEB", ["Ghosts", "Ghosts : Fantômes à la maison"], 2021, False),
    ("Ghosts.Fantomes.A.La.Maison.S06E01.VOSTFR.1080p", ["Ghosts", "Ghosts : Fantômes à la maison"], 2021, True),
    ("Le.College.Noir.Complainte.D.Auvergne.S02E06.FRENCH", ["Le Collège Noir"], 2025, True),
    ("Le.Collegien.S01E01.FRENCH.1080p", ["Le Collège Noir"], 2025, False),
    ("The.Blacklist.S01E01.1080p", ["Black Lightning"], 2018, False),
    ("Bug.2007.1080p.BluRay", ["Bug"], 2007, True),
    ("Bug.1975.1080p.BluRay", ["Bug"], 2007, False),
    ("Bug.1080p.BluRay", ["Bug"], 2007, False),  # un seul mot utile : l'année est exigée
    ("Alien.Romulus.2024.1080p", ["Alien"], 1979, False),
    ("Aliens.1986.1080p", ["Alien"], 1979, False),
    ("Devil.May.Cry.S01.AD.MULTI.VFF.1080p.WEB", ["Devil May Cry (2025)"], 2025, True),
    ("[PM]Pocket Monsters XY Movie 17 Premiere Special with TV Special - Diancie, Princess of the Ore Country[H264 720P]",
     ["Pokémon: Diancie - Princess of the Diamond Domain", "Diancie, Princess of the Ore Country"], 2014, True),
    ("Pokemon.Movie.17.Diancie.and.the.Cocoon.of.Destruction.2014.720p",
     ["Pokémon: Diancie - Princess of the Diamond Domain", "Diancie, Princess of the Ore Country"], 2014, False),
]


@pytest.mark.parametrize("nom,titres,annee,attendu", CAS_IDENTITE)
def test_titre_reconnu(nom, titres, annee, attendu):
    assert (app._id_titre_reconnu(nom, titres, annee) is not None) is attendu


@pytest.mark.parametrize("nom,titre_arr,annee,code,attendu", [
    ("Celles.Qu.On.N.A.Pas.Eues.1981.TRUEFRENCH.1080p.CNLP.WEB.x264.EAC3-STORiES.mkv", "The Ones That Got Away", 1981, "",
     "The Ones That Got Away 1981.TRUEFRENCH.1080p.CNLP.WEB.x264.EAC3-STORiES"),
    ("Pokémon.Film.14.Noir.Victini.et.Reshiram.2011.TRUEFRENCH.WEB-DL.1080p.AVC-MaitreAnonyme.mkv",
     "Pokémon the Movie: Black - Victini and Reshiram", 2011, "",
     "Pokémon the Movie Black - Victini and Reshiram 2011.TRUEFRENCH.WEB-DL.1080p.AVC-MaitreAnonyme"),
    ("[PM]Pocket Monsters XY Movie 17 Premiere Special with TV Special - Diancie, Princess of the Ore Country[H264 720P][B7633A19].mkv",
     "Pokémon: Diancie - Princess of the Diamond Domain", 2014, "",
     "Pokémon Diancie - Princess of the Diamond Domain 2014 H264 720P][B7633A19]"),
    ("Le.College.Noir.Complainte.D.Auvergne.S02E06.FRENCH.1080p.WEB.x264-AMB3R", "Le College Noir", None, "",
     "Le College Noir S02E06.FRENCH.1080p.WEB.x264-AMB3R"),
    ("L.Attaque.Des.Titans.2013.S03E01-E22.MULTI.VFF.720p.BluRay.AAC.2.0.x264-WDQ", "Attack on Titan", None, "",
     "Attack on Titan 2013.S03E01-E22.MULTI.VFF.720p.BluRay.AAC.2.0.x264-WDQ"),
    ("Devil.May.Cry.S01.AD.MULTI.VFF.1080p.WEB.EAC3.5.1.H265-FW", "Devil May Cry (2025)", None, "",
     "Devil May Cry 2025 S01.AD.MULTI.VFF.1080p.WEB.EAC3.5.1.H265-FW"),
    ("Saison 3", "Attack on Titan", None, "S03", "Attack on Titan S03"),
])
def test_nom_refait(nom, titre_arr, annee, code, attendu):
    assert app._id_nom_refait(nom, titre_arr, annee, code) == attendu


def test_source_ajoutee_apres_l_annee():
    refait = "Pokémon Diancie - Princess of the Diamond Domain 2014 H264 720P][B7633A19]"
    assert app._id_avec_source(refait, "WEBDL-720p") == \
        "Pokémon Diancie - Princess of the Diamond Domain 2014 WEB-DL H264 720P][B7633A19]"
    assert app._id_avec_source(refait, "Bluray-1080p Remux").split(" 2014 ")[1].startswith("BluRay Remux")
    assert app._id_avec_source(refait, "SDTV") is None


def test_piege_exclut_les_archives():
    assert ".scr" in app._PIEGE_EXTENSIONS and ".exe" in app._PIEGE_EXTENSIONS
    assert ".rar" not in app._PIEGE_EXTENSIONS and ".rar" in app._NOM_SUSPECTS


def test_nom_compact():
    assert app._nom_compact("Ghosts 2021 S06E03 1080p AMZN WEB-DL DDP5 1 H 264 DUAL-BiOMA") == \
        app._nom_compact("Ghosts 2021 S06E03 1080p AMZN WEB-DL DDP 5.1 H 264 DUAL-BiOMA.scr")


def test_mort_ne(monkeypatch):
    monkeypatch.setattr(app, "IMPORT_ASSIST_MORT_JOURS", 3.0)
    monkeypatch.setattr(app, "IMPORT_ASSIST_MORT_JAMAIS_HEURES", 12.0)
    jamais = {"downloaded": 0, "completed": 0, "seen_complete": -1}
    entame = {"downloaded": 10, "completed": 10, "seen_complete": 0}
    assert app._never_started(jamais) and not app._never_started(entame)
    assert app._dead_threshold(jamais, True) == 12 * 3600
    assert app._dead_threshold(jamais, False) == 3 * 86400
    assert app._dead_threshold(entame, True) == 3 * 86400
    assert "il y a 14 h" in app._dead_desc(jamais, 14 * 3600)
    assert app._dead_desc(entame, 5 * 86400) == "plus rien reçu depuis 5 j"
