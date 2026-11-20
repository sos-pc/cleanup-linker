"""
Charge app.py hors du NAS : variables obligatoires factices, base SQLite jetable.
Aucun appel réseau : chaque test remplace les accès à qBittorrent, aux *arr et à Seerr.
"""
import os
import sys
import tempfile

import pytest

_TMP = tempfile.mkdtemp(prefix="cleanup-linker-tests-")
for nom, valeur in {
    "QBIT_URL": "http://qbittorrent.invalid",
    "QBIT_USER": "tests",
    "QBIT_PASS": "tests",
    "WEBHOOK_TOKEN": "tests",
    "DB_PATH": os.path.join(_TMP, "db.sqlite"),
    "BACKUP_DIR": os.path.join(_TMP, "backups"),
}.items():
    os.environ[nom] = valeur
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app  # noqa: E402

app.init_db()


@pytest.fixture(autouse=True)
def journal_vide():
    """Chaque test part d'un journal de l'assistant vide : ses verdicts ne se mélangent pas."""
    with app.get_db() as conn:
        conn.execute("DELETE FROM assist_log")
    yield
