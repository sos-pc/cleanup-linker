"""
/restore remet en place ce qu'un geste a mis en corbeille. Un torrent de corbeille (tag sans-torrent) est retiré
sans ses fichiers ; un vrai torrent revient dans sa catégorie d'origine, même vide : il n'est jamais retiré.
"""
import time
from contextlib import contextmanager

import pytest
from faux import FauxQbit, FilsImmediats

import app

CORBEILLE = app.TARGET_CAT


@pytest.fixture
def qb(monkeypatch):
    faux = FauxQbit()

    @contextmanager
    def session():
        yield faux

    monkeypatch.setattr(app, "qbit_session", session)
    monkeypatch.setattr(app, "threading", FilsImmediats())
    with app.get_db() as conn:
        conn.execute("DELETE FROM cleanup_log")
    return faux


def en_corbeille(qb, h, nom, tags=""):
    """Un torrent dans la corbeille, dans son propre dossier."""
    qb.torrents.append({"hash": h, "name": nom, "category": CORBEILLE, "tags": tags,
                        "save_path": "/corbeille", "content_path": f"/corbeille/{nom}"})


def noter(h, nom, categorie, source="cleanup", il_y_a=60):
    with app.get_db() as conn:
        conn.execute("INSERT INTO cleanup_log (torrent_hash, torrent_name, original_cat, source, moved_at) "
                     "VALUES (?, ?, ?, ?, ?)", (h, nom, categorie, source, int(time.time()) - il_y_a))


def restaurer(**parametres):
    reponse = app.app.test_client().post("/restore", query_string={"token": "tests", **parametres})
    assert reponse.status_code == 200


def categorie(qb, h):
    """La catégorie du torrent, None s'il n'est plus dans qBittorrent."""
    return next((t["category"] for t in qb.torrents if t["hash"] == h), None)


def retires(qb):
    return [d["hashes"] for action, d in qb.ecritures if action == "delete"]


def test_vrai_torrent_sans_categorie_revient_sans_categorie(qb):
    """Le cas de White Cat Legend : mis en corbeille par un webhook, sans catégorie d'origine."""
    en_corbeille(qb, "a" * 40, "White.Cat.Legend.S01.WEBRip.1080p")
    noter("a" * 40, "White.Cat.Legend.S01.WEBRip.1080p", "", source="webhook", il_y_a=3600)
    restaurer(since=int(time.time()) - 7200)
    assert categorie(qb, "a" * 40) == ""
    assert retires(qb) == []


def test_torrent_de_corbeille_retire_sans_ses_fichiers(qb):
    en_corbeille(qb, "b" * 40, "[sans torrent] Serie.S01", tags="sans-torrent")
    noter("b" * 40, "[sans torrent] Serie.S01", "")
    restaurer()
    assert categorie(qb, "b" * 40) is None
    assert ("delete", {"hashes": "b" * 40, "deleteFiles": "false"}) in qb.ecritures


@pytest.mark.parametrize("tags", ["sans-torrent", "autre, sans-torrent"])
def test_les_trois_cas_ensemble(qb, tags):
    en_corbeille(qb, "a" * 40, "Sans.Categorie.2020")
    en_corbeille(qb, "b" * 40, "[sans torrent] Serie.S01", tags=tags)
    en_corbeille(qb, "c" * 40, "Film.2020.1080p")
    noter("a" * 40, "Sans.Categorie.2020", "")
    noter("b" * 40, "[sans torrent] Serie.S01", "")
    noter("c" * 40, "Film.2020.1080p", "[seed] Films")
    restaurer()
    assert categorie(qb, "a" * 40) == ""
    assert categorie(qb, "b" * 40) is None
    assert categorie(qb, "c" * 40) == "[seed] Films"
    assert retires(qb) == ["b" * 40]
