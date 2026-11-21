"""
La sync et ses garde-fous : vrais fichiers et vrais hardlinks dans un dossier jetable, faux qBittorrent.
Une lecture qui ferait trop maigrir l'index est refusée ; une sync ratée est reprise vite ; /cleanup refuse
de juger après une chute, même jusqu'à zéro.
"""
import os
import time
from contextlib import contextmanager

import pytest
from faux import FilsImmediats

import app


@pytest.fixture
def sync(monkeypatch, tmp_path):
    """Index et journal vides, /data/Media pointé sur un dossier jetable, qBittorrent simulé."""
    with app.get_db() as conn:
        conn.execute("DELETE FROM files")
        conn.execute("DELETE FROM sync_log")
    qbit = {"categories": {}, "torrents": [], "panne": False}

    @contextmanager
    def session():
        if qbit["panne"]:
            raise ConnectionError("qBittorrent injoignable")
        yield object()

    monkeypatch.setattr(app, "qbit_session", session)
    monkeypatch.setattr(app, "qbit_get_categories", lambda s: dict(qbit["categories"]))
    monkeypatch.setattr(app, "qbit_get_torrents", lambda s: list(qbit["torrents"]))
    monkeypatch.setattr(app, "backup_db", lambda: None)
    monkeypatch.setattr(app, "refresh_arr_managed_from_links", lambda: 0)
    monkeypatch.setattr(app, "purge_arr_managed_absents", lambda hashes: 0)
    monkeypatch.setattr(app, "jellyfin_enabled", lambda: False)
    monkeypatch.setattr(app, "_SYNC_MEDIA_ROOT", str(tmp_path / "Media"))
    monkeypatch.setattr(app, "SYNC_MAX_DROP_PERCENT", 50.0)
    return qbit


def bibliotheque(tmp_path, qbit, n=3, liens=3):
    """n torrents d'une vidéo chacun dans la catégorie « Cat », dont `liens` ont un hardlink dans Media."""
    cat = tmp_path / "Multimedia/Cat"
    qbit["categories"] = {"Cat": str(cat)}
    for i in range(n):
        dossier = cat / f"Release.{i}"
        os.makedirs(dossier)
        video = dossier / f"Release.{i}.mkv"
        video.write_bytes(b"x")
        qbit["torrents"].append({"hash": f"{i:040x}", "name": f"Release.{i}", "category": "Cat",
                                 "content_path": str(dossier), "save_path": str(cat)})
        if i < liens:
            os.makedirs(tmp_path / f"Media/Titre {i}")
            os.link(video, tmp_path / f"Media/Titre {i}/Titre {i}.mkv")


def index():
    with app.get_db() as conn:
        return {r["path"] for r in conn.execute("SELECT path FROM files WHERE stale = 0")}


def dernier_journal():
    with app.get_db() as conn:
        return dict(conn.execute("SELECT status, rows_before, rows_after FROM sync_log ORDER BY id DESC LIMIT 1").fetchone())


def test_sync_normale(tmp_path, sync):
    bibliotheque(tmp_path, sync)
    assert app.sync_db() == "success"
    assert len(index()) == 6
    assert dernier_journal() == {"status": "success", "rows_before": 0, "rows_after": 6}


def test_montage_media_perdu_index_garde(tmp_path, sync, monkeypatch):
    monkeypatch.setattr(app, "_SYNC_MIN_ROWS", 2)
    bibliotheque(tmp_path, sync)
    app.sync_db()
    avant = index()
    os.rename(tmp_path / "Media", tmp_path / "Media.demonte")
    assert app.sync_db() == "refused"
    assert index() == avant
    assert dernier_journal()["status"] == "refused"


def test_force_passe_outre(tmp_path, sync, monkeypatch):
    monkeypatch.setattr(app, "_SYNC_MIN_ROWS", 2)
    bibliotheque(tmp_path, sync)
    app.sync_db()
    os.rename(tmp_path / "Media", tmp_path / "Media.demonte")
    assert app.sync_db(force=True) == "success"
    assert not any("/Media/" in p for p in index())


def test_qbittorrent_vide_index_garde(tmp_path, sync, monkeypatch):
    monkeypatch.setattr(app, "_SYNC_MIN_ROWS", 2)
    bibliotheque(tmp_path, sync, n=3, liens=1)
    app.sync_db()
    avant = index()
    sync["categories"], sync["torrents"] = {}, []
    assert app.sync_db() == "refused"
    assert index() == avant


def test_petit_index_jamais_bloque(tmp_path, sync):
    bibliotheque(tmp_path, sync)
    app.sync_db()
    os.rename(tmp_path / "Media", tmp_path / "Media.demonte")
    assert app.sync_db() == "success"  # sous _SYNC_MIN_ROWS lignes : une installation neuve se remplit librement


def test_garde_fou_desactive(tmp_path, sync, monkeypatch):
    monkeypatch.setattr(app, "_SYNC_MIN_ROWS", 2)
    monkeypatch.setattr(app, "SYNC_MAX_DROP_PERCENT", 0.0)
    bibliotheque(tmp_path, sync)
    app.sync_db()
    os.rename(tmp_path / "Media", tmp_path / "Media.demonte")
    assert app.sync_db() == "success"


def test_qbittorrent_injoignable(tmp_path, sync):
    bibliotheque(tmp_path, sync)
    app.sync_db()
    avant = index()
    sync["panne"] = True
    assert app.sync_db() == "failed"
    assert index() == avant
    assert dernier_journal()["status"] == "failed"


def test_compte_reel_de_l_index(tmp_path, sync):
    """Deux catégories sur le même dossier : la lecture double ses lignes, l'index non. Le journal note l'index."""
    bibliotheque(tmp_path, sync)
    sync["categories"]["Cat2"] = sync["categories"]["Cat"]
    assert app.sync_db() == "success"
    assert dernier_journal()["rows_after"] == len(index()) == 6


def test_doublons_sans_torrent_ne_sont_pas_une_chute(tmp_path, sync, monkeypatch):
    """Trois catégories sur un même dossier triplent en base les fichiers sans torrent (UNIQUE ne compare
    pas NULL). Deux syncs identiques se suivent sans refus : avant et après se comptent pareil."""
    monkeypatch.setattr(app, "_SYNC_MIN_ROWS", 2)
    partage = tmp_path / "Multimedia/Partage"
    os.makedirs(partage)
    for i in range(4):
        (partage / f"Sans.torrent.{i}.mkv").write_bytes(b"x")
    sync["categories"] = {"A": str(partage), "B": str(partage), "C": str(partage)}
    assert app.sync_db() == "success"
    assert app.sync_db() == "success"
    assert dernier_journal()["rows_after"] == 12


@pytest.mark.parametrize("statut,echecs,attendu", [
    ("success", 0, 12 * 3600), ("success", 5, 12 * 3600),
    ("failed", 0, 300), ("failed", 1, 900), ("refused", 2, 1800), ("failed", 3, 3600), ("failed", 40, 3600),
])
def test_delai_avant_la_sync_suivante(monkeypatch, statut, echecs, attendu):
    monkeypatch.setattr(app, "SYNC_INTERVAL", 12 * 3600)
    assert app._sync_delay(statut, echecs) == attendu


def test_delai_jamais_plus_long_que_l_intervalle(monkeypatch):
    monkeypatch.setattr(app, "SYNC_INTERVAL", 600)
    assert app._sync_delay("failed", 3) == 600


def test_boucle_reprend_vite_apres_un_echec(monkeypatch):
    """La boucle compte les échecs d'affilée, exception comprise, et repart de zéro après une réussite."""
    class Fin(Exception):
        pass

    statuts = ["failed", RuntimeError("panne"), "refused", "success", "failed"]
    attentes = []

    def sync_db():
        statut = statuts.pop(0)
        if isinstance(statut, Exception):
            raise statut
        return statut

    def dormir(secondes):
        attentes.append(secondes)
        if not statuts:
            raise Fin

    monkeypatch.setattr(app, "SYNC_INTERVAL", 12 * 3600)
    monkeypatch.setattr(app, "sync_db", sync_db)
    monkeypatch.setattr(app.time, "sleep", dormir)
    app._sync_started.clear()
    try:
        with pytest.raises(Fin):
            app.sync_loop()
    finally:
        app._sync_started.clear()
    assert attentes == [5, 300, 900, 1800, 12 * 3600, 300]


@pytest.mark.parametrize("statut,poursuit", [("refused", False), ("failed", False), ("success", True)])
def test_cleanup_s_arrete_si_sa_sync_n_a_pas_reussi(sync, monkeypatch, statut, poursuit):
    """Un index refusé ou une sync ratée ne disent pas l'état des liens : /cleanup ne va pas plus loin."""
    suite = []

    def file_des_arr():
        suite.append(True)
        raise RuntimeError("fin du test")

    monkeypatch.setattr(app, "threading", FilsImmediats())
    monkeypatch.setattr(app, "sync_db", lambda force=False: statut)
    monkeypatch.setattr(app, "_arr_queue_hashes", file_des_arr)
    reponse = app.app.test_client().post("/cleanup?token=tests&dry_run=true")
    assert reponse.status_code == 200
    assert bool(suite) is poursuit


@pytest.mark.parametrize("age,avant,apres,refuse", [
    (60, 1000, 0, True),        # index vidé : l'ancien garde-fou le laissait passer
    (60, 1000, 400, True),
    (60, 1000, 900, False),
    (60, 0, 0, False),          # première sync d'une installation neuve
    (3 * 12 * 3600, 1000, 1000, True),  # trop vieille
])
def test_garde_fou_de_cleanup(sync, monkeypatch, age, avant, apres, refuse):
    monkeypatch.setattr(app, "SYNC_INTERVAL", 12 * 3600)
    with app.get_db() as conn:
        conn.execute("INSERT INTO sync_log (started_at, finished_at, status, rows_before, rows_after) "
                     "VALUES (?, ?, 'success', ?, ?)", (int(time.time()) - age, int(time.time()) - age, avant, apres))
    assert bool(app._cleanup_refus()) is refuse


def test_cleanup_sans_sync_reussie(sync):
    assert app._cleanup_refus() == ""
