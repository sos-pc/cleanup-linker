"""
Les torrents de corbeille et le geste qui les ajoute : vrais dossiers dans un dossier jetable, faux qBittorrent
qui répond comme la version 5.2 (409 quand il n'ajoute rien). Deux dossiers jumeaux, la même release sous deux
trackers de cross-seeds, donnent deux torrents ; une erreur au milieu du geste est journalisée avec ce qui a changé.
"""
import logging
import os
from contextlib import contextmanager

import pytest
from faux import FauxQbit

import app

RELEASE = "Serie.S01.MULTi.1080p.WEB.x264-GRP"


@pytest.fixture
def qb(monkeypatch, caplog):
    faux = FauxQbit()

    @contextmanager
    def session():
        yield faux

    monkeypatch.setattr(app, "qbit_session", session)
    monkeypatch.setattr(app, "_jellyfin_libraries", set)
    with app.get_db() as conn:
        conn.execute("DELETE FROM cleanup_log")
        conn.execute("DELETE FROM corbeille_log")
    caplog.set_level(logging.INFO)
    return faux


def release(base, nom=RELEASE, fichiers=("e01.mkv", "e02.mkv", "e01.nfo")):
    """Un dossier de release et ses fichiers."""
    racine = os.path.join(base, nom)
    os.makedirs(racine)
    chemins = []
    for f in fichiers:
        chemin = os.path.join(racine, f)
        with open(chemin, "wb") as sortie:
            sortie.write(b"x" * 10)
        chemins.append(chemin)
    return racine, chemins


def jumeau(racine, base):
    """Le même dossier sous un autre tracker : mêmes noms, mêmes fichiers (liens durs)."""
    autre = os.path.join(base, os.path.basename(racine))
    os.makedirs(autre)
    chemins = []
    for f in sorted(os.listdir(racine)):
        os.link(os.path.join(racine, f), os.path.join(autre, f))
        chemins.append(os.path.join(autre, f))
    return autre, chemins


def corbeille(racine, fichiers, visible=False):
    return {"racine": racine, "nom": app._CORBEILLE_PREFIXE + os.path.basename(racine), "fichiers": sorted(fichiers),
            "videos": len(fichiers), "fusion": 0, "autres": 0, "remplace": [], "visible": visible}


def plan(tmp_path, *corbeilles, torrents=()):
    p = app._plan_vide([])
    p["corbeilles"] = list(corbeilles)
    p["torrents"] = list(torrents)
    p["dossier_corbeille"] = str(tmp_path / "Downloads" / app.TARGET_CAT)
    return p


def notes():
    with app.get_db() as conn:
        return [r[0] for r in conn.execute("SELECT torrent_hash FROM cleanup_log ORDER BY id")]


def deplacements(qb):
    return [d["location"] for action, d in qb.ecritures if action == "setLocation"]


def jumeaux(tmp_path):
    a, fa = release(tmp_path / "cross-seeds/TR4KER")
    b, fb = jumeau(a, tmp_path / "cross-seeds/C411")
    return (a, fa), (b, fb)


def test_jumeaux_donnent_deux_torrents(tmp_path):
    (a, fa), (b, fb) = jumeaux(tmp_path)
    assert app._trash_torrent(a, fa)[1] != app._trash_torrent(b, fb)[1]
    # Le même dossier redonne le même torrent : un passage suivant le reconnaît.
    assert app._trash_torrent(a, fa) == app._trash_torrent(a, list(reversed(fa)))


def test_jumeaux_tous_deux_en_corbeille(tmp_path, qb):
    (a, fa), (b, fb) = jumeaux(tmp_path)
    faits = app._trash_apply(plan(tmp_path, corbeille(a, fa), corbeille(b, fb)), qb, "test")
    assert faits == {"torrents": 0, "corbeilles": 2}
    assert sorted(t["content_path"] for t in qb.torrents) == sorted([a, b])
    assert len(notes()) == len(set(notes())) == 2


def test_deja_la_ni_reajoute_ni_recompte(tmp_path, qb):
    a, fa = release(tmp_path / "Films")
    app._trash_apply(plan(tmp_path, corbeille(a, fa)), qb, "test")
    qb.ecritures.clear()
    faits = app._trash_apply(plan(tmp_path, corbeille(a, fa)), qb, "test")
    assert faits["corbeilles"] == 0
    assert qb.ecritures == []
    assert len(notes()) == 1


@pytest.mark.parametrize("code", [409, 415])
def test_un_refus_n_arrete_pas_les_suivants(tmp_path, qb, caplog, code):
    qb.refus = {1: code}
    a, fa = release(tmp_path / "Films")
    c, fc = release(tmp_path / "Films", "Autre.Film.2020.1080p")
    faits = app._trash_apply(plan(tmp_path, corbeille(a, fa), corbeille(c, fc)), qb, "test")
    assert faits["corbeilles"] == 1
    assert [t["content_path"] for t in qb.torrents] == [c]
    assert f"refuse le torrent de corbeille {app._CORBEILLE_PREFIXE}{RELEASE} (HTTP {code}" in caplog.text


def test_fichier_disparu_les_autres_continuent(tmp_path, qb):
    a, fa = release(tmp_path / "Films")
    c, fc = release(tmp_path / "Films", "Autre.Film.2020.1080p")
    os.remove(fa[0])
    faits = app._trash_apply(plan(tmp_path, corbeille(a, fa), corbeille(c, fc)), qb, "test")
    assert faits["corbeilles"] == 1


def test_visible_sort_de_la_bibliotheque(tmp_path, qb):
    a, fa = release(tmp_path / "Films")
    app._trash_apply(plan(tmp_path, corbeille(a, fa, visible=True)), qb, "test")
    assert deplacements(qb) == [str(tmp_path / "Downloads" / app.TARGET_CAT)]


def test_jumeaux_visibles_un_seul_deplace(tmp_path, qb, caplog):
    """Les deux iraient au même endroit de la corbeille : le second reste à sa place."""
    (a, fa), (b, fb) = jumeaux(tmp_path)
    faits = app._trash_apply(plan(tmp_path, corbeille(a, fa, visible=True), corbeille(b, fb, visible=True)), qb, "test")
    assert faits["corbeilles"] == 2
    assert len(deplacements(qb)) == 1
    assert "est déjà pris" in caplog.text


def test_dossier_d_un_autre_torrent_pas_deplace(tmp_path, qb):
    """Un torrent vivant a ce dossier pour racine, sans lister les noms orphelins : le dossier ne se coupe pas."""
    a, fa = release(tmp_path / "Films")
    qb.torrents.append({"hash": "b" * 40, "name": RELEASE, "category": "Films",
                        "save_path": str(tmp_path / "Films"), "content_path": a})
    app._trash_apply(plan(tmp_path, corbeille(a, fa, visible=True)), qb, "test")
    assert deplacements(qb) == []


def test_place_deja_prise_sur_le_disque(tmp_path, qb):
    a, fa = release(tmp_path / "Films")
    os.makedirs(tmp_path / "Downloads" / app.TARGET_CAT / RELEASE)  # reste d'un vidage
    app._trash_apply(plan(tmp_path, corbeille(a, fa, visible=True)), qb, "test")
    assert deplacements(qb) == []


@pytest.mark.parametrize("passage", ["geste", "repos"])
def test_erreur_au_milieu_journal_exact(tmp_path, qb, monkeypatch, passage):
    """Un torrent mis en corbeille et un torrent de corbeille ajouté, puis qBittorrent en panne."""
    vieux = {"hash": "a" * 40, "name": "Vieux.Film.2019", "category": "Films",
             "save_path": str(tmp_path / "Films"), "content_path": str(tmp_path / "Films/Vieux.Film.2019")}
    qb.torrents.append(dict(vieux))
    a, fa = release(tmp_path / "Films")
    c, fc = release(tmp_path / "Films", "Autre.Film.2020.1080p")
    p = plan(tmp_path, corbeille(a, fa), corbeille(c, fc), torrents=[vieux])
    qb.refus = {2: 500}
    if passage == "geste":
        monkeypatch.setattr(app, "_trash_plan", lambda chemins, s, lues: p)
        resultat = app.global_trash(["/x"], "test", "suppression", False)
    else:
        monkeypatch.setattr(app, "_rest_plan", lambda s, lues, en_file, geres: p)
        resultat = app.rest_trash(False, set())
    assert resultat["resume"] == ("erreur après 1 torrent(s) mis en corbeille et 1 torrent(s) de corbeille "
                                  "ajouté(s) : HTTP 500")
    assert resultat["moved"] == 2
    with app.get_db() as conn:
        ligne = conn.execute("SELECT resume, decision FROM corbeille_log ORDER BY id DESC LIMIT 1").fetchone()
    assert ligne["resume"] == resultat["resume"]
    assert ligne["decision"]


def test_erreur_avant_tout_changement(qb, monkeypatch):
    def plan_impossible(chemins, s, lues):
        raise RuntimeError("qBittorrent injoignable")

    monkeypatch.setattr(app, "_trash_plan", plan_impossible)
    resultat = app.global_trash(["/x"], "test", "suppression", False)
    assert resultat["resume"] == "erreur, rien n'a été fait : qBittorrent injoignable"
    assert resultat["moved"] == 0
