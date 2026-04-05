#!/usr/bin/env python3
"""
Rijkswaterstaat Golfhoogte Proxy
=================================
Haalt actuele Hm0-observaties op van de Rijkswaterstaat API en serveert
ze als GeoJSON met CORS-headers zodat de kaartpagina ze kan gebruiken.

Gebruik:  python3 rws-proxy.py
Vereist:  Python 3.6+  (geen pip-packages nodig)
Poort:    3001
"""

import json
import time
import os
import threading
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, parse_qs

PORT      = int(os.environ.get("PORT", 3001))
RWS_BASE  = "https://ddapi20-waterwebservices.rijkswaterstaat.nl"
CEFAS_URL = "https://wavenet-api.cefas.co.uk/api/Map/Current"
CACHE_S   = 10 * 60   # 10 minuten cache

BUIENRADAR_URL = "https://data.buienradar.nl/2.0/feed/json"

# ── Hulpfunctie: POST naar RWS API ──────────────────────────────────────────

def rws_post(path, body):
    data = json.dumps(body).encode("utf-8")
    req  = urllib.request.Request(
        RWS_BASE + path,
        data    = data,
        method  = "POST",
        headers = {
            "Content-Type": "application/json",
            "Accept":       "application/json",
            "User-Agent":   "RWS-Golfhoogte-Proxy/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


# ── Stap 1: catalogus laden en Hm0-stations selecteren ──────────────────────

def fetch_hm0_stations():
    print("[RWS] Catalogus ophalen…")
    catalog = rws_post(
        "/METADATASERVICES/OphalenCatalogus",
        {
            "CatalogusFilter": {
                "Grootheden":     True,
                "Eenheden":       True,
                "Compartimenten": True,
                "ProcesTypes":    True,
                "Groeperingen":   True,
            }
        },
    )

    if not catalog.get("Succesvol", True) and catalog.get("Fout"):
        raise RuntimeError("Catalogus-fout: " + str(catalog["Fout"]))

    # AquoMetadata-IDs die bij Hm0 horen
    hm0_meta_ids = {
        m["AquoMetadata_MessageID"]
        for m in catalog.get("AquoMetadataLijst", [])
        if m.get("Grootheid", {}).get("Code") == "Hm0"
    }

    # Locatie-IDs die Hm0 meten
    hm0_loc_ids = {
        rel["Locatie_MessageID"]
        for rel in catalog.get("AquoMetadataLocatieLijst", [])
        if rel.get("AquoMetaData_MessageID") in hm0_meta_ids
    }

    # Locaties met coördinaten
    stations = [
        loc for loc in catalog.get("LocatieLijst", [])
        if loc.get("Locatie_MessageID") in hm0_loc_ids
        and loc.get("Lat") is not None
        and loc.get("Lon") is not None
    ]

    print(f"[RWS] {len(stations)} Hm0-meetstations gevonden")
    return stations


# ── Stap 2: laatste waarden ophalen (batches van 20) ────────────────────────

def fetch_latest_values(stations):
    BATCH   = 20
    results = []

    for i in range(0, len(stations), BATCH):
        batch = stations[i:i + BATCH]
        print(f"[RWS] Waarnemingen {i+1}–{min(i+BATCH, len(stations))} "
              f"van {len(stations)}…")
        resp = rws_post(
            "/ONLINEWAARNEMINGENSERVICES/OphalenLaatsteWaarnemingen",
            {
                "AquoPlusWaarnemingMetadataLijst": [{
                    "AquoMetadata": {
                        "Compartiment": {"Code": "OW"},
                        "Eenheid":      {"Code": "cm"},
                        "Grootheid":    {"Code": "Hm0"},
                    }
                }],
                "LocatieLijst": [{"Code": s["Code"]} for s in batch],
            },
        )
        results.extend(resp.get("WaarnemingenLijst", []))

    return results


# ── Filter: welke stations uitsluiten ────────────────────────────────────────
#
# Uitgesloten worden:
#   1. Binnenwateren: IJsselmeer, Markermeer, Slotermeer en aanverwante meren
#   2. Scheepsstations: lichtschip, historische boorschepen (Penrod, Sean P)
#
INLAND_KEYWORDS = [
    "ijsselmeer", "markermeer", "markerwaard", "markerwadden",
    "slotermeer", "woudsend",
]
SHIP_CODES = {
    "texel.lichtschip", "penrod", "seanpplatform",
    "petten.meetraai3",
    "ijmuiden.5a",
    "ijgeul.2.boei",
    "noordzee.boei.b75n",
    "noordwijk.meetpost",
    "texel",
    "q1.1",
    "d15",
}

def is_excluded(code, naam):
    code_l = (code or "").lower()
    naam_l = (naam or "").lower()
    if code_l in SHIP_CODES:
        return True
    for kw in INLAND_KEYWORDS:
        if kw in code_l or kw in naam_l:
            return True
    return False


# ── GeoJSON bouwen ───────────────────────────────────────────────────────────

def build_geojson(stations, waarnemingen):
    station_map = {s["Code"]: s for s in stations}

    # Dedupliceer per stationcode: bewaar de meting met het recentste tijdstip.
    # Elk fysiek station kan meerdere sensoren hebben die elk een Hm0 rapporteren;
    # we tonen er slechts één per locatie.
    best = {}   # code → w (de "beste" waarneming voor dit station)
    for w in waarnemingen:
        loc_code = (w.get("Locatie") or {}).get("Code")
        if not loc_code:
            continue
        metingen  = w.get("MetingenLijst") or []
        tijdstip  = metingen[0].get("Tijdstip") if metingen else None
        if loc_code not in best:
            best[loc_code] = w
        else:
            prev_t = ((best[loc_code].get("MetingenLijst") or [{}])[0]).get("Tijdstip") or ""
            if (tijdstip or "") > prev_t:
                best[loc_code] = w

    features = []
    for loc_code, w in best.items():
        station  = station_map.get(loc_code) or w.get("Locatie") or {}
        lat = station.get("Lat") or (w.get("Locatie") or {}).get("Lat")
        lon = station.get("Lon") or (w.get("Locatie") or {}).get("Lon")

        if lat is None or lon is None:
            continue

        naam = (w.get("Locatie") or {}).get("Naam") or station.get("Naam") or loc_code

        if is_excluded(loc_code, naam):
            continue

        metingen   = w.get("MetingenLijst") or []
        meting     = metingen[0] if metingen else {}
        meetwaarde = (meting.get("Meetwaarde") or {}).get("Waarde_Numeriek")
        hm0_m      = round(meetwaarde / 100, 2) if meetwaarde is not None else None
        tijdstip   = meting.get("Tijdstip")

        # Sla stations over waarvan de laatste meting ouder is dan 48 uur
        if tijdstip:
            try:
                meting_dt = datetime.fromisoformat(tijdstip)
                if meting_dt.tzinfo is None:
                    meting_dt = meting_dt.replace(tzinfo=timezone.utc)
                if (datetime.now(timezone.utc) - meting_dt).total_seconds() > 48 * 3600:
                    continue
            except Exception:
                pass
        meta       = meting.get("WaarnemingMetadata") or {}
        status_lst = meta.get("StatuswaardeLijst") or []
        kwal_lst   = meta.get("KwaliteitswaardecodeLijst") or []

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "code":      loc_code,
                "naam":      naam,
                "hm0_m":    hm0_m,
                "tijdstip":  tijdstip,
                "status":    status_lst[0] if status_lst else None,
                "kwaliteit": kwal_lst[0]   if kwal_lst   else None,
            },
        })

    return {
        "type":           "FeatureCollection",
        "features":       features,
        "opgehaald":      datetime.now(timezone.utc).isoformat(),
        "aantalStations": len(features),
    }


# ── BSH MARNET: Duitse Noordzee meetstations ─────────────────────────────────
#
# Bron: Bundesamt für Seeschifffahrt und Hydrographie (BSH)
# Bestand: https://www2.bsh.de/aktdat/seegang/Seegang_MARNET_FINO_RAVE.txt
# Formaat: ruimtegescheiden tekst, golfhoogte in meters
#
# Alleen Noord-Zee stations worden opgenomen (Baltische stations uitgesloten).
# Coördinaten zijn hardgecodeerd omdat het bestand ze niet bevat.

BSH_STATIONS = {
    # code: (naam, lat, lon)
    # ── Noordzee ────────────────────────────────────────────────────────
    "HEL": ("Helgoland-Süd",   54.1750,  7.8840),
    "HEO": ("Helgoland-Nord",  54.1870,  7.9070),
    "LTH": ("Helgoland LT",    54.1500,  7.9970),
    "BUD": ("Butendiek",       54.9920,  7.7430),
    "DBU": ("Deutsche Bucht",  54.1770,  6.3280),
    "NO1": ("NordseeOne",      54.4345,  6.6220),
    "NOR": ("Nordergründe",    53.7390,  8.3160),
    "ELB": ("Elbe",            54.0028,  8.1017),
    "NOO": ("NordseOst",       54.4340,  6.6580),
    # ── Oostzee ─────────────────────────────────────────────────────────
    "FN2": ("FINO 2 (Oostzee)",      55.0070, 13.1542),
    "DAR": ("Darßer Schwelle",       54.7000, 12.7000),
    "ARK": ("Arkona Becken",         54.8853, 13.8607),
    "SEE": ("Fehmarnbelt",           54.5967, 11.1167),
}

BSH_URL = "https://www2.bsh.de/aktdat/seegang/Seegang_MARNET_FINO_RAVE.txt"


def fetch_bsh_data():
    req = urllib.request.Request(BSH_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=15) as r:
        content = r.read().decode("utf-8", errors="replace")

    now      = datetime.now(timezone.utc)
    features = []

    for line in content.strip().splitlines():
        parts = line.split()
        if len(parts) < 4 or parts[0] == "Typ":
            continue

        ort, zeit_str, hs_str = parts[1], parts[2], parts[3]

        if ort not in BSH_STATIONS:
            continue  # Onbekend of Baltisch station

        naam, lat, lon = BSH_STATIONS[ort]

        try:
            hm0 = round(float(hs_str), 2)
        except ValueError:
            hm0 = None

        try:
            tijdstip_dt = datetime.strptime(zeit_str, "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
            if (now - tijdstip_dt).total_seconds() > 48 * 3600:
                continue  # Ouder dan 48 uur
            tijdstip = tijdstip_dt.isoformat()
            _record_bsh_history(ort, tijdstip, hm0)
        except ValueError:
            tijdstip = None

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "code":      f"bsh.{ort.lower()}",
                "naam":      naam,
                "hm0_m":    hm0,
                "tijdstip":  tijdstip,
                "status":    None,
                "kwaliteit": None,
                "bron":      "BSH",
            },
        })

    print(f"[BSH] {len(features)} Noord-Zee stations geladen")
    return features


# ── CEFAS WaveNet: Britse golfmeetstations ───────────────────────────────────
#
# Bron: Centre for Environment, Fisheries and Aquaculture Science (CEFAS)
# API:  https://wavenet-api.cefas.co.uk/api/Map/Current
# Licentie: Open Government Licence
#
# Filter: alleen stations met recente data (< 48 uur) en ruwweg in het
# Noordzeegebied (lon > -10°, lat > 49°) worden opgenomen.

def fetch_cefas_data():
    req = urllib.request.Request(
        CEFAS_URL,
        headers={
            "Accept":    "application/json",
            "User-Agent": "Mozilla/5.0",
            "Origin":    "https://wavenet.cefas.co.uk",
            "Referer":   "https://wavenet.cefas.co.uk/",
        },
    )
    with urllib.request.urlopen(req, timeout=20) as r:
        data = json.loads(r.read().decode("utf-8"))

    now      = datetime.now(timezone.utc)
    features = []

    for f in data.get("features", []):
        props = f.get("properties", {})
        coords = f.get("geometry", {}).get("coordinates", [])
        if len(coords) < 2:
            continue

        lon, lat = coords[0], coords[1]

        # Filter: alleen Noord-Zee / nabije Atlantische kust
        if lon < -10 or lat < 49:
            continue

        station_id = props.get("id", "")
        naam       = props.get("title", station_id)
        source     = props.get("source", "INT")
        tijdstip_s = props.get("timestamp", "")

        # 48-uurs staleness filter
        if tijdstip_s:
            try:
                ts_dt = datetime.fromisoformat(tijdstip_s)
                if ts_dt.tzinfo is None:
                    ts_dt = ts_dt.replace(tzinfo=timezone.utc)
                if (now - ts_dt).total_seconds() > 48 * 3600:
                    continue
            except Exception:
                continue
        else:
            continue

        hm0_info = props.get("results", {}).get("Hm0", {})
        hm0_vals  = hm0_info.get("values", [])
        try:
            hm0_m = round(float(hm0_vals[0]), 2) if hm0_vals and hm0_vals[0] else None
        except (ValueError, IndexError):
            hm0_m = None

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "code":         f"cefas.{station_id.lower()}",
                "naam":         naam,
                "hm0_m":       hm0_m,
                "tijdstip":     tijdstip_s,
                "status":       None,
                "kwaliteit":    None,
                "bron":         "CEFAS",
                "cefas_id":     station_id,
                "cefas_source": source,
            },
        })

    print(f"[CEFAS] {len(features)} stations geladen")
    return features


# ── La Bouée: Golf van Biskaje boeien ────────────────────────────────────────
#
# Bron: La Bouée (labouee.app) – aggregator van Puertos del Estado (ES) en CANDHIS (FR)
# Formaat: JSON per boei, https://labouee.app/data/buoys/{slug}/latest.json
# Licentie: CC BY 4.0

LABOUEE_STATIONS = {
    # slug: (naam, lat, lon)
    "bilbao-vizcaya-buoy": ("Bilbao-Vizcaya",    43.640, -3.040),
    "anglet":              ("Anglet",             43.532, -1.615),
    "saint-jean-de-luz":   ("Saint-Jean-de-Luz",  43.408, -1.682),
    "cap-ferret":          ("Cap Ferret",         44.653, -1.447),
    "noirmoutier":         ("Noirmoutier",        46.917, -2.466),
    "belle-ile":           ("Belle-Île",          47.285, -3.285),
    "les-pierres-noires":  ("Les Pierres Noires", 48.290, -4.968),
}

LABOUEE_BASE = "https://labouee.app/data/buoys"

def fetch_labouee_data():
    now      = datetime.now(timezone.utc)
    features = []
    for slug, (naam, lat, lon) in LABOUEE_STATIONS.items():
        code = f"labouee.{slug}"
        try:
            url = f"{LABOUEE_BASE}/{slug}/latest.json"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read().decode("utf-8"))
            if data.get("status") != "ok":
                continue
            reading  = data.get("latest_reading", {})
            hm0      = reading.get("wave_height_m")
            tijdstip_s = reading.get("measured_at", "")
            try:
                ts_dt = datetime.fromisoformat(tijdstip_s.replace("Z", "+00:00"))
                if (now - ts_dt).total_seconds() > 48 * 3600:
                    continue
                tijdstip = ts_dt.isoformat()
            except Exception:
                tijdstip = tijdstip_s or None
            if hm0 is not None:
                hm0 = round(float(hm0), 2)
            _record_labouee_history(slug, tijdstip, hm0)
            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {
                    "code":     code,
                    "naam":     naam,
                    "hm0_m":    hm0,
                    "tijdstip": tijdstip,
                    "status":   None,
                    "kwaliteit":None,
                    "bron":     "LaBouee",
                },
            })
        except Exception as e:
            print(f"[LaBouée] {slug}: {e}")
    print(f"[LaBouée] {len(features)} stations geladen")
    return features


# ── La Bouée geschiedenis: in-memory ring buffer ──────────────────────────────

_labouee_history = {}  # slug → {tijdstip_iso: hm0_m}

def _record_labouee_history(slug, tijdstip_iso, hm0_m):
    if slug not in _labouee_history:
        _labouee_history[slug] = {}
    if tijdstip_iso:
        _labouee_history[slug][tijdstip_iso] = hm0_m
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=25)).isoformat()
    _labouee_history[slug] = {
        ts: v for ts, v in _labouee_history[slug].items() if ts >= cutoff
    }

def get_labouee_history(slug):
    naam   = LABOUEE_STATIONS.get(slug, (slug,))[0]
    buf    = _labouee_history.get(slug, {})
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    data   = [{"t": ts, "v": v} for ts, v in buf.items() if ts >= cutoff and v is not None]
    data.sort(key=lambda x: x["t"])
    return {"code": f"labouee.{slug}", "naam": naam, "data": data}

def _seed_labouee_history():
    """Laad La Bouée-geschiedenis uit GitHub-bestanden bij opstarten."""
    for slug in LABOUEE_STATIONS:
        safe  = slug.replace("-", "_")
        url   = f"{GITHUB_RAW}/data/history/labouee-{safe}.json"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                payload = json.loads(r.read().decode("utf-8"))
            count = 0
            for pt in payload.get("data", []):
                ts = pt.get("t"); v = pt.get("v")
                if ts and v is not None:
                    _record_labouee_history(slug, ts, v)
                    count += 1
            print(f"[LaBouée] {slug}: {count} historische punten geladen")
        except Exception as e:
            print(f"[LaBouée] {slug}: geen GitHub-history ({e})")


# ── BSH geschiedenis: in-memory ring buffer ───────────────────────────────────
#
# BSH publiceert alleen de meest recente snapshot. We bouwen 24-uursgeschiedenis
# op door elke poll-cyclus (10 min) de waarden op te slaan in een ring buffer.
# Na ~24 uur heeft elke station ~144 datapunten.

_bsh_history = {}   # ort → {tijdstip_iso: hm0_m}

def _record_bsh_history(ort, tijdstip_iso, hm0_m):
    """Sla een BSH meting op in de ring buffer (max 25 uur)."""
    if ort not in _bsh_history:
        _bsh_history[ort] = {}
    _bsh_history[ort][tijdstip_iso] = hm0_m

    # Verwijder metingen ouder dan 25 uur
    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(hours=25)).isoformat()
    _bsh_history[ort] = {
        ts: v for ts, v in _bsh_history[ort].items() if ts >= cutoff
    }


def get_bsh_history(ort):
    """Geef 24-uursgeschiedenis terug vanuit de ring buffer."""
    naam = BSH_STATIONS.get(ort, (ort,))[0]
    buf  = _bsh_history.get(ort, {})
    now  = datetime.now(timezone.utc)
    cutoff = (now - timedelta(hours=24)).isoformat()
    data = [
        {"t": ts, "v": v}
        for ts, v in buf.items()
        if ts >= cutoff and v is not None
    ]
    data.sort(key=lambda x: x["t"])
    return {"code": f"bsh.{ort.lower()}", "naam": naam, "data": data}


GITHUB_RAW = "https://raw.githubusercontent.com/awillemse-dev/golfhoogtes-noordzee/main"

def _seed_bsh_history():
    """Laad BSH-geschiedenis uit GitHub-bestanden bij opstarten (ring buffer pre-seeden)."""
    for ort in BSH_STATIONS:
        url = f"{GITHUB_RAW}/data/history/bsh-{ort.lower()}.json"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                payload = json.loads(r.read().decode("utf-8"))
            count = 0
            for pt in payload.get("data", []):
                ts = pt.get("t")
                v  = pt.get("v")
                if ts and v is not None:
                    _record_bsh_history(ort, ts, v)
                    count += 1
            print(f"[BSH] {ort}: {count} historische punten geladen uit GitHub")
        except Exception as e:
            print(f"[BSH] {ort}: geen GitHub-history beschikbaar ({e})")


# ── CEFAS geschiedenis: Detail/Results API ───────────────────────────────────

CEFAS_API_BASE = "https://wavenet-api.cefas.co.uk/api"
CEFAS_HEADERS  = {
    "Accept":    "application/json",
    "User-Agent": "Mozilla/5.0",
    "Origin":    "https://wavenet.cefas.co.uk",
    "Referer":   "https://wavenet.cefas.co.uk/",
}

def fetch_cefas_history(station_id, source="INT"):
    """Haal 24-uursgeschiedenis op voor een CEFAS station."""
    now   = datetime.now(timezone.utc)
    start = (now - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S")
    end   = now.strftime("%Y-%m-%dT%H:%M:%S")

    url = (f"{CEFAS_API_BASE}/Detail/Results/{station_id}/{source}"
           f"?showForecast=false&dateFrom={start}&dateTo={end}")
    req = urllib.request.Request(url, headers=CEFAS_HEADERS)
    with urllib.request.urlopen(req, timeout=20) as r:
        rows = json.loads(r.read().decode("utf-8"))

    data = []
    for row in rows:
        if row.get("isForecast"):
            continue
        ts  = row.get("timestamp", "")
        hm0 = next((x.get("value") for x in row.get("results", []) if x.get("identifier") == "Hm0"), None)
        if ts and hm0:
            try:
                data.append({"t": ts, "v": round(float(hm0), 2)})
            except ValueError:
                pass

    data.sort(key=lambda x: x["t"])

    # Get station name from Detail endpoint
    try:
        detail_req = urllib.request.Request(
            f"{CEFAS_API_BASE}/Detail/{station_id}/{source}", headers=CEFAS_HEADERS)
        with urllib.request.urlopen(detail_req, timeout=10) as r:
            detail = json.loads(r.read().decode("utf-8"))
        naam = detail.get("description", station_id)
    except Exception:
        naam = station_id

    return {"code": f"cefas.{station_id.lower()}", "naam": naam, "data": data}


# ── Temperatuurgeschiedenis ───────────────────────────────────────────────────

def fetch_rws_temp_history(loc_code):
    """Haal 24-uursgeschiedenis op voor een RWS temperatuurstation (parameter T)."""
    now   = datetime.now(timezone.utc)
    start = now - timedelta(hours=24)
    fmt   = "%Y-%m-%dT%H:%M:%S.000+00:00"

    resp = rws_post(
        "/ONLINEWAARNEMINGENSERVICES/OphalenWaarnemingen",
        {
            "AquoPlusWaarnemingMetadata": {
                "AquoMetadata": {
                    "Compartiment": {"Code": "OW"},
                    "Eenheid":      {"Code": "oC"},
                    "Grootheid":    {"Code": "T"},
                }
            },
            "Locatie": {"Code": loc_code},
            "Periode": {
                "Begindatumtijd": start.strftime(fmt),
                "Einddatumtijd":  now.strftime(fmt),
            },
        }
    )

    waarnemingen = resp.get("WaarnemingenLijst") or []
    if not waarnemingen:
        return {"code": f"rws.temp.{loc_code.lower()}", "naam": loc_code, "data": []}

    best = max(waarnemingen, key=lambda w: len(w.get("MetingenLijst") or []))
    naam = (best.get("Locatie") or {}).get("Naam") or loc_code

    data = []
    for m in (best.get("MetingenLijst") or []):
        waarde   = (m.get("Meetwaarde") or {}).get("Waarde_Numeriek")
        tijdstip = m.get("Tijdstip")
        if waarde is not None and tijdstip:
            data.append({"t": tijdstip, "v": round(waarde, 1)})

    data.sort(key=lambda x: x["t"])
    return {"code": f"rws.temp.{loc_code.lower()}", "naam": naam, "data": data}


def fetch_cefas_temp_history(station_id, source="INT"):
    """Haal 24-uursgeschiedenis op voor een CEFAS temperatuurstation (parameter TEMP)."""
    now   = datetime.now(timezone.utc)
    start = (now - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S")
    end   = now.strftime("%Y-%m-%dT%H:%M:%S")

    url = (f"{CEFAS_API_BASE}/Detail/Results/{station_id}/{source}"
           f"?showForecast=false&dateFrom={start}&dateTo={end}")
    req = urllib.request.Request(url, headers=CEFAS_HEADERS)
    with urllib.request.urlopen(req, timeout=20) as r:
        rows = json.loads(r.read().decode("utf-8"))

    data = []
    for row in rows:
        if row.get("isForecast"):
            continue
        ts   = row.get("timestamp", "")
        temp = next((x.get("value") for x in row.get("results", [])
                     if x.get("identifier") == "TEMP"), None)
        if ts and temp is not None:
            try:
                data.append({"t": ts, "v": round(float(temp), 1)})
            except ValueError:
                pass

    data.sort(key=lambda x: x["t"])

    try:
        detail_req = urllib.request.Request(
            f"{CEFAS_API_BASE}/Detail/{station_id}/{source}", headers=CEFAS_HEADERS)
        with urllib.request.urlopen(detail_req, timeout=10) as r:
            detail = json.loads(r.read().decode("utf-8"))
        naam = detail.get("description", station_id)
    except Exception:
        naam = station_id

    return {"code": f"cefas.temp.{station_id.lower()}", "naam": naam, "data": data}


# ── Geschiedenis: laatste 24 uur voor één station ────────────────────────────

def fetch_history(code):
    now   = datetime.now(timezone.utc)
    start = now - timedelta(hours=24)

    fmt = "%Y-%m-%dT%H:%M:%S.000+00:00"
    resp = rws_post(
        "/ONLINEWAARNEMINGENSERVICES/OphalenWaarnemingen",
        {
            "AquoPlusWaarnemingMetadata": {
                "AquoMetadata": {
                    "Compartiment": {"Code": "OW"},
                    "Eenheid":      {"Code": "cm"},
                    "Grootheid":    {"Code": "Hm0"},
                }
            },
            "Locatie": {"Code": code},
            "Periode": {
                "Begindatumtijd": start.strftime(fmt),
                "Einddatumtijd":  now.strftime(fmt),
            },
        }
    )

    waarnemingen = resp.get("WaarnemingenLijst") or []
    if not waarnemingen:
        return {"code": code, "naam": code, "data": []}

    # Kies de sensor met de meeste datapunten
    best = max(waarnemingen, key=lambda w: len(w.get("MetingenLijst") or []))
    naam = (best.get("Locatie") or {}).get("Naam") or code

    data = []
    for m in (best.get("MetingenLijst") or []):
        waarde_cm = (m.get("Meetwaarde") or {}).get("Waarde_Numeriek")
        tijdstip  = m.get("Tijdstip")
        if waarde_cm is not None and tijdstip:
            data.append({"t": tijdstip, "v": round(waarde_cm / 100, 2)})

    data.sort(key=lambda x: x["t"])
    return {"code": code, "naam": naam, "data": data}


# ── Cache ────────────────────────────────────────────────────────────────────

_cache          = None
_cache_time     = 0
_stations       = None
_temp_cache     = None
_temp_time      = 0
_rws_temp_hist  = {}   # code → {tijdstip_iso: temp_c}  (ring buffer, net als BSH)


def _record_rws_temp(code, tijdstip, temp_c):
    """Sla RWS temperatuurmeting op in ring buffer (max 25 uur)."""
    if tijdstip is None or temp_c is None:
        return
    if code not in _rws_temp_hist:
        _rws_temp_hist[code] = {}
    _rws_temp_hist[code][tijdstip] = temp_c
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=25)).isoformat()
    _rws_temp_hist[code] = {
        ts: v for ts, v in _rws_temp_hist[code].items() if ts >= cutoff
    }


def get_rws_temp_history(code):
    """Geef 24-uursgeschiedenis terug vanuit de RWS ring buffer."""
    buf    = _rws_temp_hist.get(code, {})
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    data   = [{"t": ts, "v": v} for ts, v in buf.items() if ts >= cutoff and v is not None]
    data.sort(key=lambda x: x["t"])
    return {"code": code, "naam": code, "data": data}


_refresh_lock = threading.Lock()

def _do_refresh():
    """Haal alle bronnen parallel op en sla op in cache."""
    global _cache, _cache_time, _stations

    if _stations is None:
        _stations = fetch_hm0_stations()

    # RWS, BSH, CEFAS en La Bouée parallel ophalen
    with ThreadPoolExecutor(max_workers=4) as ex:
        fut_rws     = ex.submit(fetch_latest_values, _stations)
        fut_bsh     = ex.submit(fetch_bsh_data)
        fut_cefas   = ex.submit(fetch_cefas_data)
        fut_labouee = ex.submit(fetch_labouee_data)

        waarnemingen = fut_rws.result()
        rws_geojson  = build_geojson(_stations, waarnemingen)

        try:
            rws_geojson["features"].extend(fut_bsh.result())
        except Exception as e:
            print(f"[BSH] Fout: {e}")

        try:
            rws_geojson["features"].extend(fut_cefas.result())
        except Exception as e:
            print(f"[CEFAS] Fout: {e}")

        try:
            rws_geojson["features"].extend(fut_labouee.result())
        except Exception as e:
            print(f"[LaBouée] Fout: {e}")

    rws_geojson["aantalStations"] = len(rws_geojson["features"])
    _cache      = rws_geojson
    _cache_time = time.time()
    print(f"[TOTAAL] {_cache['aantalStations']} stations (RWS + BSH + CEFAS + LaBouée)")


def get_data():
    global _cache, _cache_time

    if _cache and (time.time() - _cache_time) < CACHE_S:
        return _cache

    # Voorkom dat meerdere requests tegelijk verversen
    with _refresh_lock:
        if _cache and (time.time() - _cache_time) < CACHE_S:
            return _cache
        _do_refresh()

    return _cache


def get_temp_data():
    """Haal zeewatertemperatuur op van RWS en CEFAS (gecached)."""
    global _temp_cache, _temp_time

    if _temp_cache and (time.time() - _temp_time) < CACHE_S:
        return _temp_cache

    now      = datetime.now(timezone.utc)
    features = []

    # ── RWS temperatuur ──────────────────────────────────────────────
    TEMP_INLAND = [
        "ijsselmeer", "markermeer", "markerwaard", "markerwadden", "slotermeer",
        "woudsend", "waddenzee", "grevelingen", "veerse", "volkerak", "haringvliet",
        "hollands diep", "lek", "waal", "rijn", "maas", "ijssel", "zwarte meer",
        "randmeer", "veluwemeer", "eemmeer", "gooimeer", "almere", "strand",
        "zwembad", "badstrand", "recreatie", "triathlon", "bosbaan",
    ]

    try:
        catalog = rws_post("/METADATASERVICES/OphalenCatalogus", {"CatalogusFilter": {
            "Grootheden": True, "Eenheden": True,
            "Compartimenten": True, "ProcesTypes": True, "Groeperingen": True,
        }})
        meta      = catalog.get("AquoMetadataLijst", [])
        locs      = catalog.get("LocatieLijst", [])
        meta_locs = catalog.get("AquoMetadataLocatieLijst", [])

        temp_meta_ids = {m["AquoMetadata_MessageID"] for m in meta
                         if m.get("Grootheid", {}).get("Code") == "T"
                         and m.get("Compartiment", {}).get("Code") == "OW"}
        temp_loc_ids  = {r["Locatie_MessageID"] for r in meta_locs
                         if r.get("AquoMetaData_MessageID") in temp_meta_ids}

        stations = [l for l in locs
                    if l.get("Locatie_MessageID") in temp_loc_ids
                    and l.get("Lat") and l.get("Lon")
                    and 2.0 < l["Lon"] < 9.5 and 51.0 < l["Lat"] < 56.5
                    and not any(kw in l.get("Code","").lower() or kw in l.get("Naam","").lower()
                                for kw in TEMP_INLAND)]

        BATCH = 20
        station_map = {s["Code"]: s for s in stations}
        for i in range(0, len(stations), BATCH):
            batch = stations[i:i + BATCH]
            try:
                resp = rws_post("/ONLINEWAARNEMINGENSERVICES/OphalenLaatsteWaarnemingen", {
                    "AquoPlusWaarnemingMetadataLijst": [{"AquoMetadata": {
                        "Compartiment": {"Code": "OW"},
                        "Eenheid":      {"Code": "oC"},
                        "Grootheid":    {"Code": "T"},
                    }}],
                    "LocatieLijst": [{"Code": s["Code"]} for s in batch],
                })
            except Exception:
                continue

            best = {}
            for w in resp.get("WaarnemingenLijst", []):
                loc_code = (w.get("Locatie") or {}).get("Code")
                if not loc_code:
                    continue
                metingen = w.get("MetingenLijst") or []
                tijdstip = metingen[0].get("Tijdstip") if metingen else None
                if loc_code not in best or (tijdstip or "") > (
                    ((best[loc_code].get("MetingenLijst") or [{}])[0]).get("Tijdstip") or ""
                ):
                    best[loc_code] = w

            for loc_code, w in best.items():
                station  = station_map.get(loc_code) or w.get("Locatie") or {}
                lat = station.get("Lat") or (w.get("Locatie") or {}).get("Lat")
                lon = station.get("Lon") or (w.get("Locatie") or {}).get("Lon")
                if lat is None or lon is None:
                    continue
                naam     = (w.get("Locatie") or {}).get("Naam") or station.get("Naam") or loc_code
                metingen = w.get("MetingenLijst") or []
                meting   = metingen[0] if metingen else {}
                waarde   = (meting.get("Meetwaarde") or {}).get("Waarde_Numeriek")
                temp_c   = round(waarde, 1) if waarde is not None else None
                tijdstip = meting.get("Tijdstip")
                if tijdstip:
                    try:
                        dt = datetime.fromisoformat(tijdstip)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        if (now - dt).total_seconds() > 48 * 3600:
                            continue
                    except Exception:
                        pass
                if temp_c is not None and not (-2 < temp_c < 35):
                    continue
                rws_code = f"rws.temp.{loc_code.lower()}"
                _record_rws_temp(rws_code, tijdstip, temp_c)
                features.append({"type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [lon, lat]},
                    "properties": {"code": rws_code,
                        "naam": naam, "temp_c": temp_c, "tijdstip": tijdstip, "bron": "RWS"},
                })
        print(f"[RWS temp] {sum(1 for f in features if f['properties']['bron']=='RWS')} stations")
    except Exception as e:
        print(f"[RWS temp] Fout: {e}")

    # ── CEFAS temperatuur ─────────────────────────────────────────────
    try:
        req = urllib.request.Request(
            CEFAS_URL,
            headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0",
                     "Origin": "https://wavenet.cefas.co.uk", "Referer": "https://wavenet.cefas.co.uk/"})
        with urllib.request.urlopen(req, timeout=20) as r:
            cefas_data = json.loads(r.read().decode("utf-8"))

        for f in cefas_data.get("features", []):
            props  = f.get("properties", {})
            coords = f.get("geometry", {}).get("coordinates", [])
            if len(coords) < 2:
                continue
            lon, lat = coords[0], coords[1]
            if lon < -10 or lat < 49:
                continue
            station_id = props.get("id", "")
            naam       = props.get("title", station_id)
            source     = props.get("source", "INT")
            tijdstip_s = props.get("timestamp", "")
            if not tijdstip_s:
                continue
            try:
                ts_dt = datetime.fromisoformat(tijdstip_s)
                if ts_dt.tzinfo is None:
                    ts_dt = ts_dt.replace(tzinfo=timezone.utc)
                if (now - ts_dt).total_seconds() > 48 * 3600:
                    continue
            except Exception:
                continue
            temp_vals = props.get("results", {}).get("TEMP", {}).get("values", [])
            try:
                temp_c = round(float(temp_vals[0]), 1) if temp_vals and temp_vals[0] else None
            except (ValueError, IndexError):
                temp_c = None
            if temp_c is None:
                continue
            features.append({"type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {"code": f"cefas.temp.{station_id.lower()}",
                    "naam": naam, "temp_c": temp_c, "tijdstip": tijdstip_s, "bron": "CEFAS",
                    "cefas_id": station_id, "cefas_source": source},
            })
        print(f"[CEFAS temp] {sum(1 for f in features if f['properties']['bron']=='CEFAS')} stations")
    except Exception as e:
        print(f"[CEFAS temp] Fout: {e}")

    _temp_cache = {"type": "FeatureCollection", "features": features,
                   "opgehaald": now.isoformat(), "aantalStations": len(features)}
    _temp_time  = time.time()
    print(f"[TEMP TOTAAL] {len(features)} stations")
    return _temp_cache


# ── Windsnelheid (RWS) ────────────────────────────────────────────────────────

_wind_cache    = None
_wind_time     = 0
_wind_stations = None   # eenmalig geladen uit catalogus, daarna hergebruikt

WIND_INLAND = [
    "ijsselmeer", "markermeer", "markerwaard", "markerwadden", "slotermeer",
    "woudsend", "waddenzee", "grevelingen", "veerse", "volkerak", "haringvliet",
    "hollands diep", "lek", "waal", "rijn", "maas", "ijssel", "zwarte meer",
    "randmeer", "veluwemeer", "eemmeer", "gooimeer", "almere", "strand",
    "zwembad", "badstrand", "recreatie", "bosbaan",
]

def _fetch_wind_stations():
    """Haal eenmalig de lijst van wind-stations op uit de RWS-catalogus."""
    catalog   = rws_post("/METADATASERVICES/OphalenCatalogus", {"CatalogusFilter": {
        "Grootheden": True, "Eenheden": True,
        "Compartimenten": True, "ProcesTypes": True, "Groeperingen": True,
    }})
    meta      = catalog.get("AquoMetadataLijst", [])
    locs      = catalog.get("LocatieLijst", [])
    meta_locs = catalog.get("AquoMetadataLocatieLijst", [])

    wind_meta_ids = {m["AquoMetadata_MessageID"] for m in meta
                     if m.get("Grootheid", {}).get("Code") == "WINDSHD"
                     and m.get("Eenheid", {}).get("Code") == "m/s"}
    wind_loc_ids  = {r["Locatie_MessageID"] for r in meta_locs
                     if r.get("AquoMetaData_MessageID") in wind_meta_ids}

    return [l for l in locs
            if l.get("Locatie_MessageID") in wind_loc_ids
            and l.get("Lat") and l.get("Lon")
            and 2.0 < l["Lon"] < 15.0 and 51.0 < l["Lat"] < 57.5
            and not any(kw in l.get("Code","").lower() or kw in l.get("Naam","").lower()
                        for kw in WIND_INLAND)]


def _fetch_wind_batch(batch, station_map, now):
    """Haal één batch windwaarnemingen op; retourneert lijst van features."""
    try:
        resp = rws_post("/ONLINEWAARNEMINGENSERVICES/OphalenLaatsteWaarnemingen", {
            "AquoPlusWaarnemingMetadataLijst": [{"AquoMetadata": {
                "Compartiment": {"Code": "LT"},
                "Eenheid":      {"Code": "m/s"},
                "Grootheid":    {"Code": "WINDSHD"},
            }}],
            "LocatieLijst": [{"Code": s["Code"]} for s in batch],
        })
    except Exception:
        return []

    best = {}
    for w in resp.get("WaarnemingenLijst", []):
        loc_code = (w.get("Locatie") or {}).get("Code")
        if not loc_code:
            continue
        metingen = w.get("MetingenLijst") or []
        tijdstip = metingen[0].get("Tijdstip") if metingen else None
        if loc_code not in best or (tijdstip or "") > (
            ((best[loc_code].get("MetingenLijst") or [{}])[0]).get("Tijdstip") or ""
        ):
            best[loc_code] = w

    features = []
    for loc_code, w in best.items():
        station  = station_map.get(loc_code) or w.get("Locatie") or {}
        lat = station.get("Lat") or (w.get("Locatie") or {}).get("Lat")
        lon = station.get("Lon") or (w.get("Locatie") or {}).get("Lon")
        if lat is None or lon is None:
            continue
        naam     = (w.get("Locatie") or {}).get("Naam") or station.get("Naam") or loc_code
        metingen = w.get("MetingenLijst") or []
        meting   = metingen[0] if metingen else {}
        waarde   = (meting.get("Meetwaarde") or {}).get("Waarde_Numeriek")
        wind_ms  = round(waarde, 1) if waarde is not None else None
        tijdstip = meting.get("Tijdstip")
        if tijdstip:
            try:
                dt = datetime.fromisoformat(tijdstip)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                if (now - dt).total_seconds() > 6 * 3600:
                    continue
            except Exception:
                pass
        if wind_ms is not None and not (0 <= wind_ms <= 60):
            continue
        features.append({"type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "code":     f"rws.wind.{loc_code.lower()}",
                "rws_code": loc_code,
                "naam":     naam,
                "wind_ms":  wind_ms,
                "tijdstip": tijdstip,
                "bron":     "RWS",
            },
        })
    return features


def get_wind_data():
    """Haal actuele windsnelheid op van RWS offshore stations (gecached)."""
    global _wind_cache, _wind_time, _wind_stations

    if _wind_cache and (time.time() - _wind_time) < CACHE_S:
        return _wind_cache

    now = datetime.now(timezone.utc)
    features = []

    try:
        # Catalogus maar één keer ophalen; daarna hergebruiken
        if _wind_stations is None:
            _wind_stations = _fetch_wind_stations()
            print(f"[WIND] {len(_wind_stations)} stations gevonden in catalogus")

        stations    = _wind_stations
        station_map = {s["Code"]: s for s in stations}
        BATCH       = 20
        batches     = [stations[i:i+BATCH] for i in range(0, len(stations), BATCH)]

        # Alle batches parallel ophalen
        with ThreadPoolExecutor(max_workers=len(batches)) as ex:
            futs = [ex.submit(_fetch_wind_batch, b, station_map, now) for b in batches]
            for fut in futs:
                try:
                    features.extend(fut.result())
                except Exception as e:
                    print(f"[WIND] Batch fout: {e}")

        print(f"[WIND] {len(features)} stations geladen")
    except Exception as e:
        print(f"[WIND] Fout: {e}")

    _wind_cache = {"type": "FeatureCollection", "features": features,
                   "opgehaald": now.isoformat(), "aantalStations": len(features)}
    _wind_time  = time.time()
    return _wind_cache


def fetch_wind_history(rws_code, naam):
    """Haal 24u windsnelheid op van RWS via OphalenWaarnemingen."""
    now   = datetime.now(timezone.utc)
    start = now - timedelta(hours=24)
    fmt   = "%Y-%m-%dT%H:%M:%S.000+00:00"
    resp  = rws_post(
        "/ONLINEWAARNEMINGENSERVICES/OphalenWaarnemingen",
        {
            "AquoPlusWaarnemingMetadata": {"AquoMetadata": {
                "Compartiment": {"Code": "LT"},
                "Eenheid":      {"Code": "m/s"},
                "Grootheid":    {"Code": "WINDSHD"},
            }},
            "Locatie": {"Code": rws_code},
            "Periode": {
                "Begindatumtijd": start.strftime(fmt),
                "Einddatumtijd":  now.strftime(fmt),
            },
        }
    )
    waarnemingen = resp.get("WaarnemingenLijst") or []
    if not waarnemingen:
        return {"code": f"rws.wind.{rws_code.lower()}", "naam": naam, "data": []}

    best = max(waarnemingen, key=lambda w: len(w.get("MetingenLijst") or []))
    data = []
    for m in (best.get("MetingenLijst") or []):
        waarde   = (m.get("Meetwaarde") or {}).get("Waarde_Numeriek")
        tijdstip = m.get("Tijdstip")
        if waarde is not None and tijdstip:
            data.append({"t": tijdstip, "v": round(waarde, 1)})
    data.sort(key=lambda x: x["t"])
    return {"code": f"rws.wind.{rws_code.lower()}", "naam": naam, "data": data}


# ── Nederland actuele waarnemingen (Buienradar/KNMI) ─────────────────────────

_knmi_cache = None
_knmi_time  = 0

def fetch_knmi_data():
    """Haalt actuele waarnemingen op van Buienradar (elke 10 min, geen API key)."""
    req = urllib.request.Request(
        BUIENRADAR_URL,
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        data = json.loads(r.read().decode("utf-8"))

    stations = data.get("actual", {}).get("stationmeasurements", [])
    now = datetime.now(timezone.utc)

    features = []
    for s in stations:
        lat = s.get("lat")
        lon = s.get("lon")
        if lat is None or lon is None:
            continue
        if lat < 50.5 or lat > 55.5 or lon < 2.5 or lon > 8.0:
            continue

        naam = s.get("stationname", "").replace("Meetstation ", "")
        ts   = s.get("timestamp", now.isoformat())

        def fval(key):
            v = s.get(key)
            return round(float(v), 2) if v is not None and v != "" else None

        def ival(key):
            v = s.get(key)
            return int(v) if v is not None and v != "" else None

        dd_raw = ival("winddirectiondegrees")
        dd     = None if dd_raw in (0, 990) else dd_raw
        vv_m   = fval("visibility")
        vv     = round(vv_m / 1000, 2) if vv_m is not None else None
        rh     = fval("rainFallLastHour")

        features.append({
            "type":     "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "naam":       naam,
                "station_id": str(s.get("stationid", "")),
                "bron":       "Buienradar/KNMI",
                "tijdstip":   ts,
                "ta":   fval("temperature"),
                "ff":   fval("windspeed"),
                "dd":   dd,
                "rh":   fval("humidity"),
                "pp":   fval("airpressure"),
                "qg":   fval("sunpower"),
                "R1H":  rh,
                "vv":   vv,
                "td":   None,
                "n":    None,
                "windgusts":          fval("windgusts"),
                "winddirection":      s.get("winddirection"),
                "weatherdescription": s.get("weatherdescription"),
                "feeltemperature":    fval("feeltemperature"),
                "groundtemperature":  fval("groundtemperature"),
            },
        })

    print(f"[KNMI/BR] {len(features)} stations geladen")
    return {
        "type":           "FeatureCollection",
        "features":       features,
        "opgehaald":      now.isoformat(),
        "aantalStations": len(features),
    }


def get_knmi_data():
    global _knmi_cache, _knmi_time
    if _knmi_cache and (time.time() - _knmi_time) < CACHE_S:
        return _knmi_cache
    try:
        result = fetch_knmi_data()
        _knmi_cache = result
        _knmi_time  = time.time()
    except Exception as e:
        print(f"[KNMI/BR] Fout: {e}")
        if _knmi_cache:
            return _knmi_cache
        raise
    return _knmi_cache




# ── HTTP-handler ─────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        print(f"[HTTP] {self.address_string()} – {fmt % args}")

    def send_cors(self):
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_cors()
        self.end_headers()

    def do_GET(self):
        path = self.path.split("?")[0]

        # ── /api/waves ──────────────────────────────────────────────────────
        if path == "/api/waves":
            try:
                data = get_data()
                body = json.dumps(data, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type",   "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_cors()
                self.end_headers()
                self.wfile.write(body)
            except Exception as exc:
                print(f"[FOUT] {exc}")
                body = json.dumps({"error": str(exc)}).encode("utf-8")
                self.send_response(500)
                self.send_header("Content-Type",   "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_cors()
                self.end_headers()
                self.wfile.write(body)

        # ── /api/temp ────────────────────────────────────────────────────
        elif path == "/api/temp":
            try:
                data = get_temp_data()
                body = json.dumps(data, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type",   "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_cors()
                self.end_headers()
                self.wfile.write(body)
            except Exception as exc:
                print(f"[FOUT temp] {exc}")
                body = json.dumps({"error": str(exc)}).encode("utf-8")
                self.send_response(500)
                self.send_header("Content-Type",   "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_cors()
                self.end_headers()
                self.wfile.write(body)

        # ── /api/wind ────────────────────────────────────────────────────
        elif path == "/api/wind":
            try:
                data = get_wind_data()
                body = json.dumps(data, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type",   "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_cors()
                self.end_headers()
                self.wfile.write(body)
            except Exception as exc:
                body = json.dumps({"error": str(exc)}).encode("utf-8")
                self.send_response(500)
                self.send_header("Content-Type",   "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_cors()
                self.end_headers()
                self.wfile.write(body)

        # ── /api/wind-history?code=... ───────────────────────────────────
        elif path == "/api/wind-history":
            params = parse_qs(urlparse(self.path).query)
            code   = (params.get("code") or [None])[0]
            if not code:
                body = json.dumps({"error": "code parameter verplicht"}).encode()
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_cors()
                self.end_headers()
                self.wfile.write(body)
                return
            try:
                # code = "rws.wind.{loc_code}" → strip prefix
                rws_code = code[9:].upper() if code.startswith("rws.wind.") else code.upper()
                # naam opzoeken in cache
                naam = rws_code
                cached = get_wind_data()
                for feat in cached.get("features", []):
                    if feat["properties"].get("code") == code:
                        naam     = feat["properties"].get("naam", naam)
                        rws_code = feat["properties"].get("rws_code", rws_code)
                        break
                data = fetch_wind_history(rws_code, naam)
                body = json.dumps(data, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type",   "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_cors()
                self.end_headers()
                self.wfile.write(body)
            except Exception as exc:
                print(f"[FOUT wind-history] {exc}")
                body = json.dumps({"error": str(exc)}).encode()
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_cors()
                self.end_headers()
                self.wfile.write(body)

        # ── /api/knmi ────────────────────────────────────────────────────
        elif path == "/api/knmi":
            try:
                data = get_knmi_data()
                body = json.dumps(data, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type",   "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_cors()
                self.end_headers()
                self.wfile.write(body)
            except Exception as exc:
                print(f"[FOUT knmi] {exc}")
                body = json.dumps({"error": str(exc)}).encode("utf-8")
                self.send_response(500)
                self.send_header("Content-Type",   "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_cors()
                self.end_headers()
                self.wfile.write(body)

        # ── /api/history?code=... ────────────────────────────────────────
        elif path == "/api/history":
            params = parse_qs(urlparse(self.path).query)
            code   = (params.get("code") or [None])[0]
            if not code:
                body = json.dumps({"error": "code parameter verplicht"}).encode()
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_cors()
                self.end_headers()
                self.wfile.write(body)
                return
            try:
                if code.startswith("cefas."):
                    # Zoek cefas_id en cefas_source op in de cache
                    station_id = code[6:].upper()
                    source     = "INT"
                    cached     = get_data()
                    for feat in cached.get("features", []):
                        if feat["properties"].get("code") == code:
                            station_id = feat["properties"].get("cefas_id", station_id)
                            source     = feat["properties"].get("cefas_source", source)
                            break
                    data = fetch_cefas_history(station_id, source)
                elif code.startswith("bsh."):
                    ort  = code[4:].upper()
                    data = get_bsh_history(ort)
                elif code.startswith("labouee."):
                    slug = code[8:]
                    data = get_labouee_history(slug)
                else:
                    data = fetch_history(code)
                body = json.dumps(data, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type",   "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_cors()
                self.end_headers()
                self.wfile.write(body)
            except Exception as exc:
                print(f"[FOUT history] {exc}")
                body = json.dumps({"error": str(exc)}).encode()
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_cors()
                self.end_headers()
                self.wfile.write(body)

        # ── /api/temp-history?code=... ──────────────────────────────────
        elif path == "/api/temp-history":
            params = parse_qs(urlparse(self.path).query)
            code   = (params.get("code") or [None])[0]
            if not code:
                body = json.dumps({"error": "code parameter verplicht"}).encode()
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_cors()
                self.end_headers()
                self.wfile.write(body)
                return
            try:
                if code.startswith("cefas.temp."):
                    # CEFAS: live API voor volledige 24u data
                    station_id = code[11:].upper()
                    source     = "INT"
                    cached = get_temp_data()
                    for feat in cached.get("features", []):
                        if feat["properties"].get("code") == code:
                            station_id = feat["properties"].get("cefas_id", station_id)
                            source     = feat["properties"].get("cefas_source", "INT")
                            break
                    data = fetch_cefas_temp_history(station_id, source)
                else:
                    # RWS: in-memory ring buffer (groeit elke 10 min)
                    data = get_rws_temp_history(code)
                body = json.dumps(data, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type",   "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_cors()
                self.end_headers()
                self.wfile.write(body)
            except Exception as exc:
                print(f"[FOUT temp-history] {exc}")
                body = json.dumps({"error": str(exc)}).encode()
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_cors()
                self.end_headers()
                self.wfile.write(body)

        # ── / of /index.html ─────────────────────────────────────────────
        elif path in ("/", "/index.html"):
            html_path = os.path.join(os.path.dirname(__file__),
                                     "golfhoogtes-noordzee.html")
            try:
                with open(html_path, "rb") as f:
                    body = f.read()
                self.send_response(200)
                self.send_header("Content-Type",   "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except FileNotFoundError:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"golfhoogtes-noordzee.html niet gevonden")

        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Niet gevonden")


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print()
    print("╔══════════════════════════════════════════════════╗")
    print("║  Rijkswaterstaat Golfhoogte Proxy                ║")
    print("║  Bronnen: RWS + BSH + CEFAS WaveNet             ║")
    print("╠══════════════════════════════════════════════════╣")
    print(f"║  Kaart:  http://localhost:{PORT}/                   ║")
    print(f"║  API:    http://localhost:{PORT}/api/waves           ║")
    print(f"║  Cache:  {CACHE_S // 60} minuten                         ║")
    print("╚══════════════════════════════════════════════════╝")
    print()
    print("Eerste request kan ~15 sec duren (catalogus laden).")
    print("Druk Ctrl+C om te stoppen.")
    print()
    print("[BSH] Geschiedenis pre-seeden vanuit GitHub…")
    _seed_bsh_history()
    print("[LaBouée] Geschiedenis pre-seeden vanuit GitHub…")
    _seed_labouee_history()
    print()

    def _refresh_waves():
        with _refresh_lock:
            _do_refresh()

    # Waves + temp + wind + KNMI parallel pre-warmen vóór de server start
    print("[CACHE] Gegevens vooraf ophalen (waves + temp + wind + KNMI parallel)…")
    try:
        with ThreadPoolExecutor(max_workers=4) as ex:
            fw    = ex.submit(_refresh_waves)
            ft    = ex.submit(get_temp_data)
            fwnd  = ex.submit(get_wind_data)
            fknmi = ex.submit(get_knmi_data)
            fw.result(); ft.result(); fwnd.result()
            try: fknmi.result()
            except Exception as e: print(f"[KNMI] Prewarm mislukt: {e}")
        print("[CACHE] Klaar — proxy gereed voor requests.\n")
    except Exception as e:
        print(f"[CACHE] Fout bij vooraf laden: {e}\n")

    # Achtergrond-refresh elke 9 minuten (alle caches tegelijk)
    def _background_loop():
        while True:
            time.sleep(9 * 60)
            print("[CACHE] Achtergrond-refresh gestart…")
            try:
                with ThreadPoolExecutor(max_workers=4) as ex:
                    fw    = ex.submit(_refresh_waves)
                    ft    = ex.submit(get_temp_data)
                    fwnd  = ex.submit(get_wind_data)
                    fknmi = ex.submit(get_knmi_data)
                    fw.result(); ft.result(); fwnd.result()
                    try: fknmi.result()
                    except Exception as e: print(f"[KNMI] Achtergrond-refresh mislukt: {e}")
            except Exception as e:
                print(f"[CACHE] Achtergrond-refresh mislukt: {e}")
    threading.Thread(target=_background_loop, daemon=True).start()

    server = HTTPServer(("", PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nProxy gestopt.")
