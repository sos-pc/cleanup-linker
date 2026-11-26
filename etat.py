"""
État du flux, en lecture seule.

Ce que deviennent les demandes Seerr des _ETAT_JOURS derniers jours, ce qui
attend dans les files de Sonarr et de Radarr, ce qu'il reste après les
suppressions, et ce que cleanup-linker a fait. Rien ici n'agit.

Un passage calcule un instantané toutes les _ETAT_INTERVALLE secondes, dans le
maître gunicorn comme la sync, et l'écrit à côté de la base. La route /etat
l'affiche, et `?refresh=1` en relance un. Chaque passage ajoute son résumé à la
table etat_log : la page montre l'évolution, jour par jour.

Les définitions sont celles des règles : un téléchargement est bloqué comme le
juge la règle 1 (_assist_stuck), et mort comme le juge la règle 6 (_dead_since,
_dead_threshold).
"""
from __future__ import annotations

import collections
import html
import json
import os
import re
import statistics
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterable, Iterator

import app

_ETAT_INTERVALLE = 30 * 60      # un instantané toutes les 30 min
_ETAT_JOURS = 60                # fenêtre des demandes Seerr
_ETAT_RECHERCHE = 24 * 3600     # sans rien en file passé ce délai, une demande est introuvable
_ETAT_EMBALLEMENT = 10          # téléchargements ajoutés en 24 h pour un même média : signalé
_JOURNAL_JOURS = 14
FICHIER = app.DB_PATH + ".etat.json"

# États d'une demande, regroupés comme sur la page.
A_REGARDER = ("bloquée à l'import", "téléchargement mort", "introuvable")
EN_ATTENTE = ("à approuver", "pas encore sortie", "en cours")
SANS_SUITE = ("non suivie", "fiche retirée")
ABOUTIE = "disponible"

# Décisions de l'assistant qui font aboutir une demande.
_AIDES = ("rename", "accord-fix", "pack-replace", "pack-fill", "pack-reimport", "dead-won", "import-titles")


def _ts(valeur: str | None) -> float | None:
    return datetime.fromisoformat(valeur.replace("Z", "+00:00")).timestamp() if valeur else None


def _date(instant: float) -> str:
    return datetime.fromtimestamp(instant, timezone.utc).strftime("%Y-%m-%d")


# ── Jugements : fonctions pures ─────────────────────────────────────────────


def etat_telechargement(records: list[dict[str, Any]], t: dict[str, Any] | None, maintenant: float) -> tuple[str, str]:
    """(état, détail) d'un téléchargement de la file : bloqué, mort, en cours, fini, ou absent de qBittorrent."""
    if any(app._assist_stuck(r) for r in records):
        message = next((m for r in records for s in r.get("statusMessages") or [] for m in s.get("messages") or []), "")
        return "bloqué", message or (records[0].get("trackedDownloadState") or "")
    if not t:
        return "absent de qBittorrent", ""
    if (t.get("progress") or 0) >= 1:
        return "fini", ""
    inactif = app._dead_since(t, maintenant)
    if inactif is not None and inactif >= app._dead_threshold(t, True):
        return "mort", app._dead_desc(t, inactif)
    return "en cours", f"{100 * (t.get('progress') or 0):.0f} %"


def objet(arr: str, records: list[dict[str, Any]]) -> str:
    """
    Ce que sert un téléchargement : un film ou un épisode attendu (surveillé et
    sans fichier), un upgrade (tout a déjà un fichier), ou rien de surveillé.
    Même définition que la règle 6 pour lancer une course.
    """
    cibles = [r.get("movie") or {} for r in records[:1]] if arr == "radarr" else [r.get("episode") or {} for r in records]
    if any(c.get("monitored") and not c.get("hasFile") for c in cibles):
        return "attendu"
    if cibles and all(c.get("hasFile") for c in cibles):
        return "upgrade"
    return "non surveillé"


def sortie_film(film: dict[str, Any], maintenant: float) -> str:
    """
    "" si le film est sorti en vidéo, sinon ce qu'on en sait. Radarr cherche dès
    l'annonce (disponibilité minimale « announced »), mais rien ne paraît avant.
    """
    dates = {nom: _ts(film.get(cle)) for nom, cle in (("numérique", "digitalRelease"), ("physique", "physicalRelease"))}
    if any(d is not None and d <= maintenant for d in dates.values()):
        return ""
    futures = [f"sortie {nom} le {_date(d)}" for nom, d in dates.items() if d is not None]
    if film.get("status") == "released" and not futures:
        return ""
    cinema = _ts(film.get("inCinemas"))
    if cinema:
        debut = ("au cinéma depuis le " if cinema <= maintenant else "au cinéma le ") + _date(cinema)
    else:
        debut = "annoncé"
    return ", ".join([debut] + (futures or ["pas encore de date de sortie en vidéo"]))


def etat_film(film: dict[str, Any] | None, telechargements: list[dict[str, str]], age: float,
              maintenant: float) -> tuple[str, str]:
    """(état, détail) d'une demande de film. `telechargements` : ses téléchargements en file (titre, état, détail)."""
    if film is None:
        return "fiche retirée", "plus de fiche Radarr"
    if film.get("hasFile"):
        return ABOUTIE, ""
    if not film.get("monitored"):
        return "non suivie", "film non surveillé dans Radarr"
    for etat_dl, etat in (("bloqué", "bloquée à l'import"), ("mort", "téléchargement mort")):
        dl = next((d for d in telechargements if d["etat"] == etat_dl), None)
        if dl:
            return etat, f"{dl['titre']} : {dl['detail']}"
    if telechargements:
        return "en cours", f"{telechargements[0]['titre']} : {telechargements[0]['detail'] or telechargements[0]['etat']}"
    sortie = sortie_film(film, maintenant)
    if sortie:
        return "pas encore sortie", sortie
    if age < _ETAT_RECHERCHE:
        return "en cours", "recherche en cours"
    return "introuvable", "rien dans la file : aucune release acceptée par Radarr"


def saisons_jugees(saisons: list[int]) -> list[int]:
    """
    Les saisons qui décident de l'état d'une demande. Les spéciaux (saison 0) d'un
    anime ne sortent souvent jamais en épisode, et Seerr les laisse demander avec
    la vraie saison : ils ne comptent que si rien d'autre n'est demandé.
    """
    return [s for s in saisons if s != 0] or saisons


# Épisodes manquants, du plus grave au moins grave.
_MANQUES = ("bloqué(s) à l'import", "mort(s)", "sans téléchargement", "en cours", "non surveillé(s)")


def etat_serie(episodes: list[dict[str, Any]], par_episode: dict[int, list[dict[str, str]]], age: float,
               maintenant: float) -> tuple[str, str]:
    """
    (état, détail) d'une demande de série, sur les épisodes déjà diffusés des
    saisons demandées. `par_episode` : id d'épisode → ses téléchargements en file.
    """
    if not episodes:
        return "introuvable", "Sonarr ne connaît aucun épisode de ces saisons (numérotation TMDB et TVDB différentes ?)"
    diffuses = [e for e in episodes if (_ts(e.get("airDateUtc")) or float("inf")) <= maintenant]
    if not diffuses:
        return "pas encore sortie", "aucun épisode diffusé"
    manquants = [e for e in diffuses if not e.get("hasFile")]
    if not manquants:
        return ABOUTIE, ""
    compte: collections.Counter[str] = collections.Counter()
    for e in manquants:
        etats = {d["etat"] for d in par_episode.get(e["id"], [])}
        if "bloqué" in etats:
            compte["bloqué(s) à l'import"] += 1
        elif "mort" in etats:
            compte["mort(s)"] += 1
        elif etats:
            compte["en cours"] += 1
        elif not e.get("monitored"):
            compte["non surveillé(s)"] += 1
        else:
            compte["sans téléchargement"] += 1
    detail = (f"{len(diffuses) - len(manquants)}/{len(diffuses)} épisodes ; manquants : "
              + ", ".join(f"{compte[k]} {k}" for k in _MANQUES if compte[k]))
    if compte["bloqué(s) à l'import"]:
        return "bloquée à l'import", detail
    if compte["mort(s)"]:
        return "téléchargement mort", detail
    if compte["sans téléchargement"]:
        return ("introuvable" if age >= _ETAT_RECHERCHE else "en cours"), detail
    if compte["en cours"]:
        return "en cours", detail
    return "non suivie", detail + " (souvent après une suppression)"


def delai(cree: float, imports: Iterable[float]) -> float | None:
    """Délai entre la demande et le premier import qui la suit. Une minute de marge : l'*arr peut dater l'import avant Seerr."""
    apres = [x for x in imports if x >= cree - 60]
    return min(apres) - cree if apres else None


def stats_delais(valeurs: list[float]) -> dict[str, Any]:
    v = sorted(valeurs)
    if not v:
        return {"n": 0}
    return {"n": len(v), "mediane": statistics.median(v), "p90": v[min(len(v) - 1, int(0.9 * len(v)))],
            "moins_1h": sum(x < 3600 for x in v), "moins_24h": sum(x < 86400 for x in v)}


# ── Lecture des services ────────────────────────────────────────────────────


def _seerr_demandes(debut: float) -> list[dict[str, Any]]:
    """Demandes Seerr créées depuis `debut`, les plus récentes d'abord."""
    demandes: list[dict[str, Any]] = []
    skip = 0
    while True:
        page = app._seerr_get("request", take=100, skip=skip, filter="all", sort="added") or {}
        lot = page.get("results") or []
        for d in lot:
            if (_ts(d.get("createdAt")) or 0) < debut:
                return demandes
            demandes.append(d)
        skip += len(lot)
        if not lot or skip >= ((page.get("pageInfo") or {}).get("results") or 0):
            return demandes


def _seerr_ouvertes(debut: float) -> list[dict[str, Any]]:
    """
    Demandes créées avant `debut` que Seerr tient encore pour ouvertes : approuvées
    sans être terminées, en attente d'approbation, ou en échec. Une demande bloquée
    depuis des mois ne doit pas sortir de la page parce qu'elle a vieilli.
    """
    ouvertes: dict[Any, dict[str, Any]] = {}
    for filtre in ("processing", "pending", "failed"):
        skip = 0
        while True:
            page = app._seerr_get("request", take=100, skip=skip, filter=filtre, sort="added") or {}
            lot = page.get("results") or []
            for d in lot:
                if (_ts(d.get("createdAt")) or 0) < debut:
                    ouvertes[d["id"] if d.get("id") is not None else id(d)] = d
            skip += len(lot)
            if not lot or skip >= ((page.get("pageInfo") or {}).get("results") or 0):
                break
    return list(ouvertes.values())


def _arrs() -> Iterator[tuple[str, str, str]]:
    for arr, url, cle in (("sonarr", app.SONARR_URL, app.SONARR_API_KEY), ("radarr", app.RADARR_URL, app.RADARR_API_KEY)):
        if url and cle:
            yield arr, url, cle


def _files(maintenant: float, torrents: dict[str, dict[str, Any]]) -> dict[str, dict[str, dict[str, Any]]]:
    """Téléchargements en file de chaque *arr : downloadId → lignes, torrent, état, détail."""
    params = {"sonarr": {"includeEpisode": "true", "includeSeries": "true"}, "radarr": {"includeMovie": "true"}}
    files: dict[str, dict[str, dict[str, Any]]] = {}
    for arr, url, cle in _arrs():
        par_dl: dict[str, list[dict[str, Any]]] = {}
        for r in app._queue_pages(url, cle, params[arr]):
            if r.get("downloadId"):
                par_dl.setdefault(r["downloadId"].lower(), []).append(r)
        files[arr] = {}
        for dl, records in par_dl.items():
            t = torrents.get(dl)
            etat, detail = etat_telechargement(records, t, maintenant)
            files[arr][dl] = {"records": records, "torrent": t, "etat": etat, "detail": detail}
    return files


def _historique(debut: float) -> tuple[dict[tuple[str, int], list[float]], dict[tuple[str, int], list[tuple[str, float]]]]:
    """Imports et prises depuis `debut`, par (arr, id du film ou de l'épisode)."""
    imports: dict[tuple[str, int], list[float]] = collections.defaultdict(list)
    prises: dict[tuple[str, int], list[tuple[str, float]]] = collections.defaultdict(list)
    date = datetime.fromtimestamp(debut, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for arr, url, cle in _arrs():
        champ = "movieId" if arr == "radarr" else "episodeId"
        for h in app._arr_request("GET", url, cle, "history/since", params={"date": date}, timeout=300) or []:
            media, quand = (arr, h.get(champ)), _ts(h.get("date"))
            if quand is None:
                continue
            if h.get("eventType") == "downloadFolderImported":
                imports[media].append(quand)
            elif h.get("eventType") == "grabbed" and h.get("downloadId"):
                prises[media].append((h["downloadId"].lower(), quand))
    return imports, prises


def _aides(debut: float) -> set[str]:
    """Téléchargements qu'une règle a fait aboutir depuis `debut`."""
    with app._db_lock:
        with app.get_db() as conn:
            lignes = conn.execute(
                f"SELECT DISTINCT lower(download_id) FROM assist_log WHERE dry_run = 0 AND at >= ? "
                f"AND decision IN ({', '.join('?' * len(_AIDES))})", (int(debut), *_AIDES)).fetchall()
    return {r[0] for r in lignes}


def _demandes(maintenant: float, files: dict[str, dict[str, dict[str, Any]]]) -> dict[str, Any]:
    debut = maintenant - _ETAT_JOURS * 86400
    demandes = [d for d in _seerr_demandes(debut) if d.get("status") != 3]  # refusées : pas une demande en cours
    films: dict[Any, dict[str, Any]] = {}
    series: list[dict[str, Any]] = []
    for arr, url, cle in _arrs():
        if arr == "radarr":
            films = {m.get("tmdbId"): m for m in app._arr_request("GET", url, cle, "movie", timeout=120) or []}
        else:
            series = app._arr_request("GET", url, cle, "series", timeout=120) or []
    par_tvdb = {s.get("tvdbId"): s for s in series}
    par_tmdb = {s.get("tmdbId"): s for s in series if s.get("tmdbId")}
    imports, prises = _historique(debut)
    aides = _aides(debut)

    dl_film: dict[Any, list[dict[str, str]]] = collections.defaultdict(list)
    for d in (files.get("radarr") or {}).values():
        r = d["records"][0]
        dl_film[r.get("movieId")].append({"titre": r.get("title") or "", "etat": d["etat"], "detail": d["detail"]})
    dl_ep: dict[int, list[dict[str, str]]] = collections.defaultdict(list)
    for d in (files.get("sonarr") or {}).values():
        for r in d["records"]:
            dl_ep[r.get("episodeId")].append({"titre": r.get("title") or "", "etat": d["etat"], "detail": d["detail"]})

    episodes_serie: dict[int, list[dict[str, Any]]] = {}

    def juger(d: dict[str, Any]) -> dict[str, Any]:
        """L'état d'une demande, avec ce qu'il faut pour mesurer son délai."""
        m = d.get("media") or {}
        cree = _ts(d.get("createdAt")) or maintenant
        age = maintenant - cree
        qui = (d.get("requestedBy") or {}).get("displayName") or "?"
        if d.get("type") == "movie":
            film = films.get(m.get("tmdbId"))
            titre = f"{film['title']} ({film.get('year')})" if film else f"film TMDB {m.get('tmdbId')}"
            etat, detail = etat_film(film, dl_film.get(film["id"], []) if film else [], age, maintenant)
            cles = [("radarr", film["id"])] if film else []
            attente = delai(cree, imports.get(cles[0], [])) if cles and etat == ABOUTIE else None
            genre = "films"
        else:
            serie = par_tvdb.get(m.get("tvdbId")) or par_tmdb.get(m.get("tmdbId"))
            saisons = sorted({s.get("seasonNumber") for s in d.get("seasons") or []})
            jugees = saisons_jugees(saisons)
            nom = serie["title"] if serie else f"série TVDB {m.get('tvdbId')}"
            titre = f"{nom} S{','.join(str(s) for s in saisons)}"
            if serie is None:
                etat, detail, eps = "fiche retirée", "plus de fiche Sonarr", []
            else:
                if serie["id"] not in episodes_serie:
                    episodes_serie[serie["id"]] = app._arr_request(
                        "GET", app.SONARR_URL, app.SONARR_API_KEY, "episode", params={"seriesId": serie["id"]}) or []
                eps = [e for e in episodes_serie[serie["id"]] if e.get("seasonNumber") in jugees]
                etat, detail = etat_serie(eps, dl_ep, age, maintenant)
                if jugees != saisons and etat != ABOUTIE:
                    specials = [e for e in episodes_serie[serie["id"]] if e.get("seasonNumber") == 0]
                    detail += f" ; spéciaux non comptés : {sum(bool(e.get('hasFile')) for e in specials)}/{len(specials)} en place"
            diffuses = [e for e in eps if (_ts(e.get("airDateUtc")) or float("inf")) <= maintenant]
            cles = [("sonarr", e["id"]) for e in diffuses]
            premiers = [x for x in (delai(cree, imports.get(c, [])) for c in cles) if x is not None]
            attente = max(premiers) if premiers and etat == ABOUTIE else None
            genre = "series"
        if d.get("status") == 1:
            etat, detail = "à approuver", "demande pas encore approuvée dans Seerr"
        if etat == "fiche retirée" and m.get("mediaAddedAt"):
            detail += " ; le média avait été disponible : sans doute supprimé après coup"
        return {"etat": etat, "detail": detail, "titre": titre, "genre": genre, "cles": cles, "attente": attente,
                "cree": cree, "age": age, "qui": qui}

    def ligne(j: dict[str, Any], ancienne: bool) -> dict[str, Any]:
        return {"age": j["age"], "type": "film" if j["genre"] == "films" else "série", "titre": j["titre"],
                "demandeur": j["qui"], "etat": j["etat"], "detail": j["detail"], "ancienne": ancienne}

    par_etat: collections.Counter[str] = collections.Counter()
    delais: dict[str, list[float]] = {"films": [], "series": []}
    lignes: list[dict[str, Any]] = []
    deja_la = aidees = 0
    for d in demandes:
        j = juger(d)
        par_etat[j["etat"]] += 1
        if j["etat"] == ABOUTIE:
            if j["attente"] is None:
                deja_la += 1
            else:
                delais[j["genre"]].append(j["attente"])
                telechargements = {dl for c in j["cles"] for dl, quand in prises.get(c, []) if quand >= j["cree"] - 60}
                aidees += bool(telechargements & aides)
            continue
        lignes.append(ligne(j, False))
    # Les demandes plus anciennes que la fenêtre, encore ouvertes pour Seerr : elles
    # restent sur la page, sans entrer dans les chiffres des _ETAT_JOURS jours.
    anciennes: collections.Counter[str] = collections.Counter()
    for d in _seerr_ouvertes(debut):
        j = juger(d)
        anciennes[j["etat"]] += 1
        if j["etat"] != ABOUTIE:
            lignes.append(ligne(j, True))
    ordre = {e: i for i, e in enumerate(A_REGARDER + EN_ATTENTE + SANS_SUITE)}
    lignes.sort(key=lambda x: (ordre.get(x["etat"], 99), -x["age"]))
    return {"jours": _ETAT_JOURS, "total": len(demandes), "par_etat": dict(par_etat), "deja_la": deja_la,
            "aidees": aidees, "delais": {g: stats_delais(v) for g, v in delais.items()}, "lignes": lignes,
            "anciennes": {"total": sum(anciennes.values()), "par_etat": dict(anciennes)}}


def _resume_files(files: dict[str, dict[str, dict[str, Any]]], maintenant: float) -> dict[str, Any]:
    sortie: dict[str, Any] = {}
    for arr, dls in files.items():
        medias: dict[str, dict[str, int]] = {}
        morts, bloques = [], []
        for d in dls.values():
            r = d["records"][0]
            media = ((r.get("series") or {}).get("title") if arr == "sonarr" else (r.get("movie") or {}).get("title")) or "?"
            m = medias.setdefault(media, {"total": 0, "recents": 0, "morts": 0})
            m["total"] += 1
            ajoute = (d["torrent"] or {}).get("added_on")
            m["recents"] += bool(ajoute and maintenant - ajoute < 86400)
            if d["etat"] == "mort":
                m["morts"] += 1
                morts.append({"titre": r.get("title") or "", "media": media, "detail": d["detail"], "objet": objet(arr, d["records"])})
            elif d["etat"] == "bloqué":
                bloques.append({"titre": r.get("title") or "", "media": media, "detail": d["detail"]})
        top = sorted(medias.items(), key=lambda x: (-x[1]["total"], x[0]))[:10]
        sortie[arr] = {"total": len(dls), "par_etat": dict(collections.Counter(d["etat"] for d in dls.values())),
                       "medias": [{"media": k, **v, "emballement": v["recents"] >= _ETAT_EMBALLEMENT} for k, v in top],
                       "morts": morts, "bloques": bloques}
    return sortie


def _fin_de_vie(maintenant: float, torrents: dict[str, dict[str, Any]]) -> dict[str, Any]:
    corbeille = [t for t in torrents.values() if t.get("category") == app.TARGET_CAT]
    erreurs = [{"nom": t.get("name") or "", "categorie": t.get("category") or "", "etat": t.get("state")}
               for t in torrents.values() if t.get("state") in ("error", "missingFiles") and t.get("category") != app.TARGET_CAT]
    fantomes: collections.Counter[str] = collections.Counter()
    items = 0
    if app.jellyfin_enabled():
        for i in app.jellyfin_fetch_items():
            chemin = i.get("Path")
            if not chemin:
                continue
            items += 1
            local = app.map_jellyfin_path(chemin)
            if not os.path.exists(local):
                fantomes["/".join(local.split("/")[:4])] += 1
    with app._db_lock:
        with app.get_db() as conn:
            gestes = conn.execute(
                "SELECT source, COUNT(*), SUM(COALESCE(json_extract(decision, '$.liberes'), 0)) FROM corbeille_log "
                "WHERE dry_run = 0 AND at >= ? GROUP BY source ORDER BY 2 DESC", (int(maintenant - 7 * 86400),)).fetchall()
    return {
        "corbeille": {"torrents": len(corbeille), "octets": sum(t.get("size") or 0 for t in corbeille),
                      "seedent": sum(1 for t in corbeille if (t.get("progress") or 0) >= 1 and not str(t.get("state")).startswith("stopped")),
                      "arretes": sum(1 for t in corbeille if str(t.get("state")).startswith(("stopped", "paused")))},
        "erreurs": erreurs,
        "fantomes": {"items": items, "total": sum(fantomes.values()), "par_dossier": dict(fantomes.most_common())},
        "gestes_7j": [{"source": s, "gestes": n, "liberes": lib or 0} for s, n, lib in gestes],
    }


def _activite(maintenant: float) -> dict[str, Any]:
    depuis = int(maintenant - 7 * 86400)
    with app._db_lock:
        with app.get_db() as conn:
            decisions = conn.execute("SELECT rule, decision, COUNT(*) FROM assist_log WHERE dry_run = 0 AND at >= ? "
                                     "GROUP BY rule, decision", (depuis,)).fetchall()
            courses = conn.execute("SELECT COUNT(*) FROM dead_race WHERE done = 0").fetchone()[0]
            derniere = conn.execute("SELECT MAX(finished_at) FROM sync_log WHERE status = 'success'").fetchone()[0]
            echecs = conn.execute("SELECT COUNT(*) FROM sync_log WHERE status = 'failed' AND started_at >= ?",
                                  (depuis,)).fetchone()[0]
    compte: collections.Counter[tuple[int, str]] = collections.Counter()
    for regle, decision, n in decisions:
        compte[(regle or 0, (decision or "?").split(":")[0])] += n  # « ignored:<raison> » regroupé
    return {"decisions": [{"regle": r, "decision": dec, "n": n} for (r, dec), n in sorted(compte.items())],
            "courses_ouvertes": courses, "derniere_sync": derniere, "syncs_echouees_7j": echecs,
            "modes": app._assist_modes(), "corbeille_globale": app.CORBEILLE_GLOBALE}


def _resume(inst: dict[str, Any]) -> dict[str, Any]:
    """Les chiffres suivis jour après jour."""
    d = inst.get("demandes") or {}
    # Ce qu'il y a à regarder, quel que soit l'âge de la demande : fenêtre et anciennes encore ouvertes.
    par_etat = collections.Counter(d.get("par_etat") or {}) + collections.Counter((d.get("anciennes") or {}).get("par_etat") or {})
    files = inst.get("files") or {}
    fin = inst.get("fin_de_vie") or {}
    return {
        "demandes": d.get("total"), "a_regarder": sum(par_etat.get(e, 0) for e in A_REGARDER),
        "a_regarder_anciennes": sum(((d.get("anciennes") or {}).get("par_etat") or {}).get(e, 0) for e in A_REGARDER),
        **{e: par_etat.get(e, 0) for e in A_REGARDER},
        "mediane_films": ((d.get("delais") or {}).get("films") or {}).get("mediane"),
        "file_sonarr": (files.get("sonarr") or {}).get("total"), "file_radarr": (files.get("radarr") or {}).get("total"),
        "morts_en_file": sum(len(f.get("morts") or []) for f in files.values()),
        "corbeille_octets": (fin.get("corbeille") or {}).get("octets"), "fantomes": (fin.get("fantomes") or {}).get("total"),
        "erreurs": len(inst.get("erreurs") or {}),
    }


def _sans_cle(message: str) -> str:
    """Un message d'erreur ne doit pas montrer de clé : les longues chaînes hexadécimales sont masquées."""
    return re.sub(r"[0-9a-fA-F]{20,}", "…", message)[:300]


def calculer(maintenant: float | None = None) -> dict[str, Any]:
    """Un instantané complet. Un service injoignable n'efface que sa partie, notée dans `erreurs`."""
    debut = time.time()
    maintenant = maintenant or debut
    inst: dict[str, Any] = {"at": maintenant, "erreurs": {}}

    def partie(nom: str, calcul: Any, defaut: Any) -> Any:
        try:
            return calcul()
        except Exception as e:
            inst["erreurs"][nom] = _sans_cle(f"{type(e).__name__} : {e}")
            app.log.warning("État du flux : %s en erreur (%s)", nom, inst["erreurs"][nom])
            return defaut

    def lire_torrents() -> dict[str, dict[str, Any]]:
        with app.qbit_session() as s:
            return {t["hash"].lower(): t for t in app.qbit_get_torrents(s)}

    torrents = partie("qbittorrent", lire_torrents, {})
    files = partie("files", lambda: _files(maintenant, torrents), {})
    inst["demandes"] = partie("demandes", lambda: _demandes(maintenant, files), None)
    inst["files"] = _resume_files(files, maintenant)
    inst["fin_de_vie"] = partie("fin_de_vie", lambda: _fin_de_vie(maintenant, torrents), None)
    inst["activite"] = partie("activite", lambda: _activite(maintenant), None)
    inst["duree_s"] = round(time.time() - debut, 1)
    inst["resume"] = _resume(inst)
    return inst


# ── Écriture, lecture, boucle ───────────────────────────────────────────────


def _init_journal(conn: Any) -> None:
    conn.execute("CREATE TABLE IF NOT EXISTS etat_log (id INTEGER PRIMARY KEY AUTOINCREMENT, at INTEGER NOT NULL, "
                 "resume TEXT NOT NULL)")


def ecrire(inst: dict[str, Any]) -> None:
    """L'instantané remplace le précédent d'un coup (aucun lecteur ne voit un fichier à moitié écrit) ; son résumé s'ajoute au journal."""
    temporaire = FICHIER + ".tmp"
    with open(temporaire, "w", encoding="utf-8") as f:
        json.dump(inst, f, ensure_ascii=False)
    os.replace(temporaire, FICHIER)
    with app._db_lock:
        with app.get_db() as conn:
            _init_journal(conn)
            conn.execute("INSERT INTO etat_log (at, resume) VALUES (?, ?)", (int(inst["at"]), json.dumps(inst["resume"])))


def lire() -> dict[str, Any] | None:
    try:
        with open(FICHIER, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def journal(maintenant: float | None = None, jours: int = _JOURNAL_JOURS) -> list[dict[str, Any]]:
    """Le dernier résumé de chaque jour, sur `jours` jours, le plus récent d'abord."""
    depuis = int((maintenant or time.time()) - jours * 86400)
    with app._db_lock:
        with app.get_db() as conn:
            _init_journal(conn)
            lignes = conn.execute("SELECT at, resume FROM etat_log WHERE at >= ? ORDER BY at", (depuis,)).fetchall()
    par_jour: dict[str, dict[str, Any]] = {}
    for at, resume in lignes:
        par_jour[_date(at)] = {"jour": _date(at), **json.loads(resume)}
    return sorted(par_jour.values(), key=lambda x: x["jour"], reverse=True)


@contextmanager
def _exclusif() -> Iterator[bool]:
    """Un seul calcul à la fois entre tous les process : la boucle dans le maître, ?refresh=1 dans un worker. False si un autre calcule."""
    import fcntl  # POSIX seulement : importé ici pour que le module se charge ailleurs

    fd = os.open(app.DB_PATH + ".etat.lock", os.O_CREAT | os.O_RDWR, 0o644)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def passage() -> bool:
    """Calcule et écrit un instantané. False si un autre calcul est en cours."""
    with _exclusif() as libre:
        if not libre:
            return False
        inst = calculer()
        ecrire(inst)
        app.log.info("État du flux : instantané calculé en %.0f s (%s)", inst["duree_s"],
                     ", ".join(f"{k} {v}" for k, v in inst["resume"].items() if v))
        return True


def lancer() -> None:
    threading.Thread(target=passage, daemon=True).start()


def boucle() -> None:
    """Un instantané deux minutes après le démarrage, puis toutes les _ETAT_INTERVALLE secondes."""
    time.sleep(120)
    while True:
        try:
            passage()
        except Exception as e:
            app.log.error("État du flux en erreur : %s", _sans_cle(str(e)))
        time.sleep(_ETAT_INTERVALLE)


# ── Page ────────────────────────────────────────────────────────────────────


def _duree(secondes: float | None) -> str:
    if secondes is None:
        return "–"
    if secondes < 3600:
        return f"{secondes / 60:.0f} min"
    if secondes < 2 * 86400:
        return f"{secondes / 3600:.1f} h".replace(".", ",")
    return f"{secondes / 86400:.0f} j"


def _taille(octets: float | None) -> str:
    if not octets:
        return "0"
    for unite, seuil in (("To", 1e12), ("Go", 1e9), ("Mo", 1e6)):
        if octets >= seuil:
            return f"{octets / seuil:.2f} {unite}".replace(".", ",")
    return f"{octets:.0f} o"


def _e(valeur: Any) -> str:
    return html.escape("" if valeur is None else str(valeur))


def _table(entetes: list[str], lignes: list[list[Any]], vide: str = "Rien.") -> str:
    if not lignes:
        return f'<p class="vide">{_e(vide)}</p>'
    tete = "".join(f"<th>{_e(h)}</th>" for h in entetes)
    corps = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in ligne) + "</tr>" for ligne in lignes)
    return f'<div class="defile"><table><thead><tr>{tete}</tr></thead><tbody>{corps}</tbody></table></div>'


def _delais_texte(nom: str, s: dict[str, Any]) -> str:
    if not s.get("n"):
        return f"<li>{_e(nom)} : aucune demande aboutie avec un import mesurable.</li>"
    pct = lambda k: f"{100 * s[k] / s['n']:.0f} %"  # noqa: E731
    return (f"<li>{_e(nom)} : médiane <b>{_duree(s['mediane'])}</b>, {pct('moins_1h')} en moins d'une heure, "
            f"{pct('moins_24h')} en moins de 24 h, 9 sur 10 en moins de {_duree(s['p90'])} (n = {s['n']}).</li>")


_CSS = """
:root{--fond:#f7f7f5;--carte:#fff;--texte:#1d1d1f;--doux:#6b6b70;--trait:#e2e2e0;--alerte:#b3261e;--attente:#8a5a00;--ok:#1e7b34}
@media (prefers-color-scheme:dark){:root{--fond:#161618;--carte:#1f1f22;--texte:#ececee;--doux:#9a9aa0;--trait:#333338;--alerte:#ff8a80;--attente:#f0b44c;--ok:#7bd88f}}
*{box-sizing:border-box}body{margin:0;background:var(--fond);color:var(--texte);font:15px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif}
main{max-width:1150px;margin:0 auto;padding:24px 16px 48px}h1{font-size:24px;margin:0 0 4px}h2{font-size:19px;margin:32px 0 8px}
h3{font-size:16px;margin:20px 0 6px}.doux{color:var(--doux)}.vide{color:var(--doux);font-style:italic}
.cartes{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin:12px 0}
.carte{background:var(--carte);border:1px solid var(--trait);border-radius:10px;padding:10px 12px}.carte b{display:block;font-size:22px}
.alerte b,.alerte{color:var(--alerte)}.attente b{color:var(--attente)}.ok b{color:var(--ok)}
.defile{overflow-x:auto;background:var(--carte);border:1px solid var(--trait);border-radius:10px}.defile table{min-width:520px}.nw{white-space:nowrap}
table{border-collapse:collapse;width:100%;font-size:14px}th,td{text-align:left;padding:6px 10px;border-bottom:1px solid var(--trait);vertical-align:top}
th{color:var(--doux);font-weight:600;white-space:nowrap}tr:last-child td{border-bottom:none}td.n{text-align:right;white-space:nowrap}
.bandeau{background:var(--carte);border:1px solid var(--alerte);border-radius:10px;padding:8px 12px;margin:12px 0}
details{margin-top:8px}summary{cursor:pointer;color:var(--doux)}a{color:inherit}
"""


def page_html(inst: dict[str, Any] | None, histoire: list[dict[str, Any]], lien_refresh: str, maintenant: float | None = None) -> str:
    maintenant = maintenant or time.time()
    tete = ('<!doctype html><html lang="fr"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
            f"<title>État du flux</title><style>{_CSS}</style></head><body><main><h1>État du flux</h1>")
    if not inst:
        return (tete + f'<p>Aucun instantané pour l\'instant : le premier est en cours de calcul. '
                f'<a href="{_e(lien_refresh)}">Recharger</a> dans une minute.</p></main></body></html>')
    morceaux = [tete, f'<p class="doux">Instantané d\'il y a {_duree(maintenant - inst["at"])}, '
                f'calculé en {str(inst.get("duree_s", "?")).replace(".", ",")} s · <a href="{_e(lien_refresh)}">recalculer</a></p>']
    if inst.get("erreurs"):
        morceaux.append('<div class="bandeau">Parties incomplètes : '
                        + "; ".join(f"<b>{_e(k)}</b> ({_e(v)})" for k, v in inst["erreurs"].items()) + "</div>")

    d = inst.get("demandes")
    if d:
        par = d.get("par_etat") or {}
        # Les cartes comptent les lignes des tableaux, demandes anciennes encore ouvertes comprises.
        toutes = d.get("lignes") or []
        n_regarder = sum(1 for x in toutes if x["etat"] in A_REGARDER)
        n_attente = sum(1 for x in toutes if x["etat"] in EN_ATTENTE)
        n_sans = sum(1 for x in toutes if x["etat"] in SANS_SUITE)
        n_anciennes = sum(1 for x in toutes if x.get("ancienne"))
        morceaux.append(f"<h2>Demandes Seerr des {d['jours']} derniers jours</h2>")
        morceaux.append('<div class="cartes">'
                        f'<div class="carte"><b>{d["total"]}</b>demandes</div>'
                        f'<div class="carte ok"><b>{par.get(ABOUTIE, 0)}</b>disponibles, dont {d.get("aidees", 0)} grâce à une règle</div>'
                        f'<div class="carte {"alerte" if n_regarder else ""}"><b>{n_regarder}</b>à regarder</div>'
                        f'<div class="carte attente"><b>{n_attente}</b>en attente normale</div>'
                        f'<div class="carte"><b>{n_sans}</b>sans suite attendue</div></div>')
        delais = d.get("delais") or {}
        deja = d.get("deja_la") or 0
        morceaux.append("<ul>" + _delais_texte("Films", delais.get("films") or {}) + _delais_texte("Séries", delais.get("series") or {})
                        + (f'<li class="doux">{deja} demande(s) portaient sur un média déjà là.</li>' if deja else "") + "</ul>")
        if n_anciennes:
            morceaux.append(f'<p class="doux">Les trois dernières cartes et les tableaux comptent aussi {n_anciennes} demande(s) de plus de '
                            f'{d["jours"]} jours que Seerr tient encore pour ouvertes ; les délais n\'en tiennent pas compte.</p>')
        groupes = (("À regarder : bloquées, mortes, introuvables", A_REGARDER, True),
                   ("En attente normale : pas encore sorties, en cours", EN_ATTENTE, True),
                   ("Sans suite attendue : non suivies ou fiches retirées", SANS_SUITE, False))
        for titre, etats, ouvert in groupes:
            lignes = [[f'<span class="nw">{_e(_duree(x["age"]))}</span>',
                       f'{_e(x["titre"])}<br><span class="doux">{_e(x["type"])} · {_e(x["demandeur"])}</span>',
                       f'<b>{_e(x["etat"])}</b>', _e(x["detail"])] for x in d.get("lignes") or [] if x["etat"] in etats]
            table = _table(["Depuis", "Demande", "État", "Détail"], lignes, "Aucune.")
            morceaux.append(f"<h3>{_e(titre)} ({len(lignes)})</h3>" + table if ouvert
                            else f"<details><summary>{_e(titre)} ({len(lignes)})</summary>{table}</details>")

    files = inst.get("files") or {}
    if files:
        morceaux.append("<h2>Files de Sonarr et de Radarr</h2>")
        for arr, f in files.items():
            etats = " · ".join(f"{_e(e)} : {n}" for e, n in sorted(f["par_etat"].items(), key=lambda x: -x[1]))
            morceaux.append(f"<h3>{arr.capitalize()} : {f['total']} téléchargement(s)</h3><p>{etats or 'File vide.'}</p>")
            if f.get("medias"):
                lignes = [[_e(m["media"]), f'<span class="n">{m["total"]}</span>', str(m["recents"])
                           + (' <b class="alerte">emballement ?</b>' if m["emballement"] else ""), str(m["morts"])]
                          for m in f["medias"]]
                morceaux.append(_table(["Média", "En file", "Ajoutés en 24 h", "Morts"], lignes))
            if f.get("morts"):
                lignes = [[_e(m["titre"]), _e(m["media"]), _e(m["detail"]), _e(m["objet"])] for m in f["morts"]]
                morceaux.append(f"<details><summary>Téléchargements morts ({len(f['morts'])})</summary>"
                                + _table(["Release", "Média", "Depuis", "Sert"], lignes) + "</details>")
            if f.get("bloques"):
                lignes = [[_e(b["titre"]), _e(b["media"]), _e(b["detail"])] for b in f["bloques"]]
                morceaux.append(f"<details><summary>Imports bloqués ({len(f['bloques'])})</summary>"
                                + _table(["Release", "Média", "Message"], lignes) + "</details>")

    fin = inst.get("fin_de_vie")
    if fin:
        c = fin["corbeille"]
        morceaux.append("<h2>Fin de vie</h2>")
        morceaux.append(f"<p>Corbeille : <b>{c['torrents']}</b> torrents, {_taille(c['octets'])} ; {c['seedent']} seedent, "
                        f"{c['arretes']} sont arrêtés. Elle se vide à la main.</p>")
        fa = fin["fantomes"]
        detail = ", ".join(f"{_e(k)} : {n}" for k, n in fa["par_dossier"].items())
        morceaux.append(f"<p>Jellyfin : <b>{fa['total']}</b> élément(s) sur {fa['items']} dont le fichier n'existe plus"
                        + (f" ({detail})" if detail else "") + ".</p>")
        lignes = [[_e(e["nom"]), _e(e["categorie"]), _e(e["etat"])] for e in fin["erreurs"]]
        morceaux.append("<h3>Torrents en erreur, hors corbeille</h3>" + _table(["Torrent", "Catégorie", "État"], lignes, "Aucun."))
        lignes = [[_e(g["source"]), f'<span class="n">{g["gestes"]}</span>', _e(_taille(g["liberes"]))] for g in fin["gestes_7j"]]
        morceaux.append("<h3>Corbeille globale, 7 derniers jours</h3>" + _table(["Source", "Gestes", "Libéré"], lignes, "Aucun geste."))

    a = inst.get("activite")
    if a:
        morceaux.append("<h2>Activité de cleanup-linker, 7 derniers jours</h2>")
        morceaux.append(f'<p class="doux">{_e(a["modes"])}. Corbeille globale : {_e(a["corbeille_globale"])}.</p>')
        morceaux.append(f"<p>Courses de la règle 6 ouvertes : <b>{a['courses_ouvertes']}</b>. Dernière sync réussie il y a "
                        f"{_duree(maintenant - a['derniere_sync']) if a.get('derniere_sync') else '–'} ; "
                        f"syncs échouées : {a['syncs_echouees_7j']}.</p>")
        lignes = [[str(x["regle"]), _e(x["decision"]), f'<span class="n">{x["n"]}</span>'] for x in a["decisions"]]
        morceaux.append(_table(["Règle", "Verdict", "Nombre"], lignes, "Aucun verdict."))

    if histoire:
        morceaux.append(f"<h2>Évolution, {len(histoire)} dernier(s) jour(s)</h2>")
        lignes = [[_e(h["jour"]), str(h.get("a_regarder", "–")), str(h.get("téléchargement mort", "–")),
                   str(h.get("bloquée à l'import", "–")), str(h.get("introuvable", "–")), _e(_duree(h.get("mediane_films"))),
                   str(h.get("file_sonarr", "–")), str(h.get("file_radarr", "–")), str(h.get("morts_en_file", "–")),
                   _e(_taille(h.get("corbeille_octets"))), str(h.get("fantomes", "–"))] for h in histoire]
        morceaux.append(_table(["Jour", "À regarder", "Mortes", "Bloquées", "Introuvables", "Médiane films",
                                "File Sonarr", "File Radarr", "Morts en file", "Corbeille", "Fantômes"], lignes))
    morceaux.append("</main></body></html>")
    return "".join(morceaux)
