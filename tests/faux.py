"""
Doublures partagées par les tests : un qBittorrent en mémoire qui répond comme la version 5.2, un faux Sonarr
ou Radarr qui répond par route, et un module threading dont les fils tournent tout de suite, pour les routes
qui travaillent en arrière-plan.
"""
import os
import threading

import requests

import app


class Rep:
    def __init__(self, data=None, text="", code=200):
        self.data, self.text, self.status_code = data, text, code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")

    def json(self):
        return self.data


class FauxQbit:
    """qBittorrent 5.2 en mémoire : 409 quand il n'ajoute rien. `refus` : code HTTP imposé au n-ième ajout."""

    def __init__(self):
        self.torrents: list[dict] = []
        self.ecritures: list[tuple[str, dict]] = []
        self.refus: dict[int, int] = {}
        self.ajouts = 0

    def _torrent(self, h):
        return next(t for t in self.torrents if t["hash"] == h)

    def get(self, url, params=None, timeout=None):
        if url.endswith("/torrents/info"):
            liste = [dict(t) for t in self.torrents]
            if params and params.get("hashes"):
                voulus = set(params["hashes"].split("|"))
                liste = [t for t in liste if t["hash"] in voulus]
            return Rep(liste)
        raise AssertionError("GET inattendu : " + url)

    def post(self, url, data=None, timeout=None, files=None):
        action = url.rsplit("/", 1)[-1]
        self.ecritures.append((action, dict(data or {})))
        if action == "add":
            self.ajouts += 1
            if self.ajouts in self.refus:
                return Rep(text="refusé", code=self.refus[self.ajouts])
            meta = app._torrent_meta(files["torrents"][1])
            if any(t["hash"] == meta["hash"] for t in self.torrents):
                return Rep(text="Conflict", code=409)  # qBittorrent 5.2 : rien d'ajouté
            self.torrents.append({"hash": meta["hash"], "name": data["rename"], "category": data["category"],
                                  "tags": data["tags"], "save_path": data["savepath"],
                                  "content_path": os.path.join(data["savepath"], meta["name"])})
            return Rep({"success_count": 1, "failure_count": 0, "pending_count": 0,
                        "added_torrent_ids": [meta["hash"]]})
        if action == "setCategory":
            for h in data["hashes"].split("|"):
                self._torrent(h)["category"] = data["category"]
        elif action == "setLocation":
            for h in data["hashes"].split("|"):
                t = self._torrent(h)
                t["save_path"] = data["location"]
                t["content_path"] = os.path.join(data["location"], os.path.basename(t["content_path"]))
        elif action == "delete":
            enleves = set(data["hashes"].split("|"))
            self.torrents = [t for t in self.torrents if t["hash"] not in enleves]
        else:
            raise AssertionError("POST inattendu : " + url)
        return Rep()


def faux_arr(routes: dict, appels: list):
    """Remplace _arr_request : répond par route (méthode, point d'entrée), note chaque appel."""
    def _arr_request(method, base_url, api_key, endpoint, timeout=30, retries=3, **kwargs):
        appels.append((method, endpoint, kwargs.get("params"), kwargs.get("json")))
        if (method, endpoint) not in routes:
            raise AssertionError(f"appel inattendu : {method} {endpoint}")
        reponse = routes[(method, endpoint)]
        return reponse(kwargs) if callable(reponse) else reponse
    return _arr_request


class FilsImmediats:
    """Remplace le module threading d'app : le fil lancé par une route tourne dans le test même."""

    class Thread:
        def __init__(self, target, args=(), kwargs=None, daemon=None):
            self.target, self.args, self.kwargs = target, args, kwargs or {}

        def start(self):
            self.target(*self.args, **self.kwargs)

    def __getattr__(self, nom):
        return getattr(threading, nom)
