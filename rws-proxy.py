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

# ── NDBC: NOAA boeien (Noord-Atlantisch / Europees gebied) ───────────────────
#
# Bron: NOAA National Data Buoy Center, https://www.ndbc.noaa.gov/
# URL:  https://www.ndbc.noaa.gov/data/latest_obs/latest_obs.txt
# Formaat: space-delimited, eerste twee regels zijn headers
# Licentie: US Government Open Data (publiek domein)
#
# Filter: alleen boeien in het Europese / Noord-Atlantische gebied
#   lat 40–70 N, lon -40 – 15 O  → relevante NDBC/WMO boeien voor dit kaartgebied
# Kolommen: 0=STN 1=LAT 2=LON 3=YYYY 4=MM 5=DD 6=hh 7=mm
#           8=WDIR 9=WSPD 10=GST 11=WVHT 12=DPD 13=APD 14=MWD
#           15=PRES 16=ATMP 17=WTMP ...
# MM = ontbrekende waarde

NDBC_URL = "https://www.ndbc.noaa.gov/data/latest_obs/latest_obs.txt"

def fetch_ndbc_history(station_id):
    """Haal 24-uursgeschiedenis op voor een NDBC-station via realtime2 tekstbestand."""
    url = f"https://www.ndbc.noaa.gov/data/realtime2/{station_id.upper()}.txt"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=15) as r:
        lines = r.read().decode("utf-8", errors="replace").splitlines()

    now    = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=24)
    data   = []

    for line in lines:
        if line.startswith("#") or not line.strip():
            continue
        cols = line.split()
        if len(cols) < 12:
            continue
        try:
            ts = datetime(int(cols[0]), int(cols[1]), int(cols[2]),
                          int(cols[3]), int(cols[4]), tzinfo=timezone.utc)
        except ValueError:
            continue
        if ts < cutoff:
            continue
        try:
            hm0 = None if cols[8] == "MM" else round(float(cols[8]), 2)
        except (ValueError, IndexError):
            hm0 = None
        if hm0 is not None:
            data.append({"t": ts.isoformat(), "v": hm0})

    data.sort(key=lambda x: x["t"])
    return {"code": f"ndbc.{station_id.lower()}", "naam": f"NDBC {station_id}", "data": data}


def fetch_ndbc_wind_history(station_id):
    """Haal 24u windsnelheid + richting op voor een NDBC-station via realtime2."""
    # realtime2 cols: YY MM DD hh mm WDIR WSPD GST WVHT ...
    # index:          0  1  2  3  4   5    6   7   8
    url = f"https://www.ndbc.noaa.gov/data/realtime2/{station_id.upper()}.txt"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=15) as r:
        lines = r.read().decode("utf-8", errors="replace").splitlines()

    now    = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=24)
    data     = []
    dir_data = []

    for line in lines:
        if line.startswith("#") or not line.strip():
            continue
        cols = line.split()
        if len(cols) < 7:
            continue
        try:
            ts = datetime(int(cols[0]), int(cols[1]), int(cols[2]),
                          int(cols[3]), int(cols[4]), tzinfo=timezone.utc)
        except ValueError:
            continue
        if ts < cutoff:
            continue
        t = ts.isoformat()
        try:
            wspd = None if cols[6] == "MM" else round(float(cols[6]), 1)
        except (ValueError, IndexError):
            wspd = None
        try:
            wdir = None if cols[5] == "MM" else int(float(cols[5])) % 360
        except (ValueError, IndexError):
            wdir = None
        if wspd is not None:
            data.append({"t": t, "v": wspd})
        if wdir is not None:
            dir_data.append({"t": t, "v": wdir})

    data.sort(key=lambda x: x["t"])
    dir_data.sort(key=lambda x: x["t"])
    return {"code": f"ndbc.wind.{station_id.lower()}", "naam": f"NDBC {station_id}",
            "data": data, "dir_data": dir_data}


_ndbc_wind_features = []   # gevuld door fetch_ndbc_data(), gebruikt door get_wind_data()

def fetch_ndbc_data():
    req = urllib.request.Request(
        NDBC_URL,
        headers={"User-Agent": "Mozilla/5.0 (compatible; ZeedataProxy/1.0)"},
    )
    with urllib.request.urlopen(req, timeout=20) as r:
        lines = r.read().decode("utf-8", errors="replace").splitlines()

    global _ndbc_wind_features
    now           = datetime.now(timezone.utc)
    wave_features = []
    wind_features = []

    for line in lines:
        if line.startswith("#") or not line.strip():
            continue
        cols = line.split()
        if len(cols) < 12:
            continue
        try:
            lat = float(cols[1])
            lon = float(cols[2])
        except ValueError:
            continue

        station_id = cols[0]

        # Tijdstip
        try:
            tijdstip = datetime(
                int(cols[3]), int(cols[4]), int(cols[5]),
                int(cols[6]), int(cols[7]), tzinfo=timezone.utc
            ).isoformat()
            dt = datetime.fromisoformat(tijdstip)
            if (now - dt).total_seconds() > 6 * 3600:
                continue
        except Exception:
            tijdstip = None

        # Golfhoogte (WVHT, kolom 11)
        try:
            hm0 = None if cols[11] == "MM" else round(float(cols[11]), 2)
        except (ValueError, IndexError):
            hm0 = None

        # Windsnelheid (WSPD, kolom 9) en richting (WDIR, kolom 8)
        try:
            wind_ms  = None if cols[9]  == "MM" else round(float(cols[9]),  1)
            wind_dir = None if cols[8]  == "MM" else int(float(cols[8]))
        except (ValueError, IndexError):
            wind_ms  = None
            wind_dir = None

        code = f"ndbc.{station_id.lower()}"
        naam = f"NDBC {station_id}"
        geom = {"type": "Point", "coordinates": [lon, lat]}

        if hm0 is not None:
            wave_features.append({
                "type": "Feature",
                "geometry": geom,
                "properties": {
                    "code":     code,
                    "naam":     naam,
                    "hm0_m":    hm0,
                    "tijdstip": tijdstip,
                    "bron":     "NDBC/NOAA",
                },
            })

        if wind_ms is not None and 0 <= wind_ms <= 60:
            wind_features.append({
                "type": "Feature",
                "geometry": geom,
                "properties": {
                    "code":     f"ndbc.wind.{station_id.lower()}",
                    "naam":     naam,
                    "wind_ms":  wind_ms,
                    "wind_dir": wind_dir,
                    "tijdstip": tijdstip,
                    "bron":     "NDBC/NOAA",
                },
            })

    _ndbc_wind_features = wind_features
    print(f"[NDBC] {len(wave_features)} golfstations, {len(wind_features)} windstations geladen")
    return wave_features

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


# ── FMI: Fins Meteorologisch Instituut – Baltische/Scandinavische golven ─────
#
# Bron: Finnish Meteorological Institute Open Data WFS
# URL:  https://opendata.fmi.fi/wfs  (storedquery: fmi::observations::wave::simple)
# Licentie: Creative Commons Attribution 4.0

try:
    import xml.etree.ElementTree as _ET
    _FMI_ET = _ET
except ImportError:
    _FMI_ET = None

FMI_WFS = ("https://opendata.fmi.fi/wfs?service=WFS&version=2.0.0"
           "&request=getFeature&storedquery_id=fmi::observations::wave::simple"
           "&bbox=-30,-90,50,90&maxlocations=500")

_FMI_NS = {
    "wfs":   "http://www.opengis.net/wfs/2.0",
    "BsWfs": "http://xml.fmi.fi/schema/wfs/2.0",
    "gml":   "http://www.opengis.net/gml/3.2",
}

def fetch_fmi_data():
    if _FMI_ET is None:
        return []
    now = datetime.now(timezone.utc)
    req = urllib.request.Request(FMI_WFS, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=25) as r:
        content = r.read()

    root = _FMI_ET.fromstring(content)
    members = root.findall(".//BsWfs:BsWfsElement", _FMI_NS)

    # Groepeer per locatie → pak meest recente WaveHs
    stations = {}   # "lat,lon" → {naam, lat, lon, tijdstip, hm0}
    for m in members:
        name_el  = m.findtext("BsWfs:ParameterName",  namespaces=_FMI_NS)
        if name_el != "WaveHs":
            continue
        val_s    = m.findtext("BsWfs:ParameterValue", namespaces=_FMI_NS)
        time_s   = m.findtext("BsWfs:Time",           namespaces=_FMI_NS)
        loc_el   = m.find("BsWfs:Location", _FMI_NS)
        if loc_el is None:
            continue
        pos_el   = loc_el.find(".//gml:pos", _FMI_NS)
        if pos_el is None or not pos_el.text:
            continue
        try:
            lat, lon = [float(x) for x in pos_el.text.split()]
            hm0      = round(float(val_s), 2)
            ts_dt    = datetime.fromisoformat(time_s.replace("Z", "+00:00"))
        except Exception:
            continue

        if (now - ts_dt).total_seconds() > 24 * 3600:
            continue

        key = f"{lat:.4f},{lon:.4f}"
        if key not in stations or time_s > stations[key]["tijdstip"]:
            stations[key] = {
                "lat": lat, "lon": lon,
                "hm0": hm0, "tijdstip": ts_dt.isoformat(),
            }

    features = []
    for i, (key, s) in enumerate(stations.items()):
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [s["lon"], s["lat"]]},
            "properties": {
                "code":     f"fmi.{i}",
                "naam":     f"FMI {s['lat']:.2f}°N {s['lon']:.2f}°E",
                "hm0_m":    s["hm0"],
                "tijdstip": s["tijdstip"],
                "bron":     "FMI",
            },
        })

    print(f"[FMI] {len(features)} stations geladen")
    return features


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



CDIP_THREDDS  = "https://thredds.cdip.ucsd.edu/thredds"
CDIP_CATALOG  = CDIP_THREDDS + "/catalog/cdip/realtime/catalog.xml"
CDIP_ODAP     = CDIP_THREDDS + "/dodsC/cdip/realtime"
_cdip_cache   = None
_cdip_time    = 0


# ── SOCIB: THREDDS OPeNDAP – Middellandse Zee (Balearen) ─────────────────────
#
# Bron: SOCIB (Sistema d'Observació i Predicció Costaner de les Illes Balears)
# URL:  https://thredds.socib.es/thredds/  (THREDDS/OPeNDAP)
# Licentie: Open data (CC BY 4.0)
#
# Golfboeien: WAV_HEI_SIG (significante golfhoogte)
# Weerstations: WIN_SPE (windsnelheid m/s) + WIN_DIR (windrichting graden)
#
# Ophaalstrategie (zelfde als CDIP):
#   1. DDS ophalen → Float64 time[time = N]
#   2. ASCII laatste element [N-1:1:N-1]

SOCIB_DODC = "https://thredds.socib.es/thredds/dodsC"
SOCIB_CAT  = "https://thredds.socib.es/thredds/catalog"

SOCIB_WAVE_DIRS = [
    "buoy_bahiadepalma-scb_wave006",
    "buoy_canaldeibiza-scb_wave007",
    "buoy_portocolom-scb_wave005",
    "buoy_soller-scb_wave004",
    "mobims_sonbou-scb_awac005",
]
SOCIB_WAVE_NAMES = {
    "buoy_bahiadepalma-scb_wave006": "Palma de Mallorca (golf)",
    "buoy_canaldeibiza-scb_wave007": "Ibiza Kanaal (golf)",
    "buoy_portocolom-scb_wave005":   "Porto Colom (golf)",
    "buoy_soller-scb_wave004":       "Soller (golf)",
    "mobims_sonbou-scb_awac005":     "Son Bou (golf)",
}

SOCIB_WIND_DIRS = [
    "buoy_bahiadepalma-scb_met029",
    "buoy_canaldeibiza-scb_met030",
    "buoy_soller-scb_met024",
    "buoy_portocolom-scb_met025",
]
SOCIB_WIND_NAMES = {
    "buoy_bahiadepalma-scb_met029": "Palma de Mallorca (wind)",
    "buoy_canaldeibiza-scb_met030": "Ibiza Kanaal (wind)",
    "buoy_soller-scb_met024":       "Soller (wind)",
    "buoy_portocolom-scb_met025":   "Porto Colom (wind)",
}

_socib_wave_paths = {}   # station_dir → nc urlPath
_socib_wind_paths = {}   # station_dir → nc urlPath
_socib_paths_time = 0


def _get_socib_latest_path(category, station_dir):
    """Fetch L1 catalog voor een SOCIB station en geef urlPath terug van de hoogste dep."""
    import re as _re
    try:
        import xml.etree.ElementTree as _ET_socib
        cat_url = f"{SOCIB_CAT}/mooring/{category}/{station_dir}/L1/catalog.xml"
        req = urllib.request.Request(cat_url, headers={"User-Agent": "RWS-Golfhoogte-Proxy/1.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            root = _ET_socib.fromstring(r.read())
        best_dep = -1; best_path = None
        for el in root.iter():
            tag = el.tag.split("}")[-1] if "}" in el.tag else el.tag
            if tag == "dataset":
                up = el.get("urlPath", "")
                if "_latest.nc" in up:
                    m = _re.search(r'dep(\d+)_', up)
                    if m and int(m.group(1)) > best_dep:
                        best_dep = int(m.group(1)); best_path = up
        return best_path
    except Exception as e:
        print(f"[SOCIB] catalog fout {station_dir}: {e}")
        return None


def _ensure_socib_paths():
    """Ververs de gecachede bestandspaden voor alle SOCIB stations (max 1× per 24 uur)."""
    global _socib_wave_paths, _socib_wind_paths, _socib_paths_time
    if _socib_wave_paths and (time.time() - _socib_paths_time) < 86400:
        return
    for d in SOCIB_WAVE_DIRS:
        p = _get_socib_latest_path("waves_recorder", d)
        if p:
            _socib_wave_paths[d] = p
    for d in SOCIB_WIND_DIRS:
        p = _get_socib_latest_path("weather_station", d)
        if p:
            _socib_wind_paths[d] = p
    _socib_paths_time = time.time()
    print(f"[SOCIB] Paths: {len(_socib_wave_paths)} wave, {len(_socib_wind_paths)} wind")


def _socib_parse_ascii(text):
    """Parset een OPeNDAP ASCII-response: scalars → str/float, arrays → lijst of float."""
    lines    = text.splitlines()
    in_data  = False
    parsed   = {}
    cur_key  = None
    cur_vals = []

    for line in lines:
        s = line.strip()
        # Wacht op scheidingslijn "---..."
        if not in_data:
            if s.startswith("---"):
                in_data = True
            continue

        if not s:
            if cur_key is not None and cur_vals:
                parsed[cur_key] = cur_vals[0] if len(cur_vals) == 1 else cur_vals
                cur_key = None; cur_vals = []
            continue

        if cur_key is not None:
            # Dataregel van huidige array
            try:
                for v in s.split(","):
                    v = v.strip()
                    if v:
                        cur_vals.append(float(v))
            except ValueError:
                pass
        elif "[" in s:
            # Array-header: "WAV_HEI_SIG[1]"
            if cur_key is not None and cur_vals:
                parsed[cur_key] = cur_vals[0] if len(cur_vals) == 1 else cur_vals
                cur_vals = []
            cur_key = s.split("[")[0].strip()
        elif ", " in s:
            # Scalar: "LAT, 39.498883" of "station_name, \"...\""
            k, v = s.split(", ", 1)
            parsed[k.strip()] = v.strip().strip('"')

    if cur_key is not None and cur_vals:
        parsed[cur_key] = cur_vals[0] if len(cur_vals) == 1 else cur_vals
    return parsed


def _socib_fetch_station_wave(station_dir):
    """Haal actuele WAV_HEI_SIG op van één SOCIB golfboei via THREDDS OPeNDAP."""
    import re as _re
    nc_path = _socib_wave_paths.get(station_dir)
    if not nc_path:
        return None
    naam = SOCIB_WAVE_NAMES.get(station_dir, station_dir)
    try:
        dds_url = f"{SOCIB_DODC}/{nc_path}.dds"
        with urllib.request.urlopen(
            urllib.request.Request(dds_url, headers={"User-Agent": "RWS-Golfhoogte-Proxy/1.0"}),
            timeout=10
        ) as r:
            dds = r.read().decode()
        m = _re.search(r'Float64 time\[time = (\d+)\]', dds)
        if not m: return None
        n = int(m.group(1)); last = n - 1

        data_url = (f"{SOCIB_DODC}/{nc_path}.ascii?"
                    f"LAT,LON,station_name,time[{last}:1:{last}],WAV_HEI_SIG[{last}:1:{last}]")
        with urllib.request.urlopen(
            urllib.request.Request(data_url, headers={"User-Agent": "RWS-Golfhoogte-Proxy/1.0"}),
            timeout=10
        ) as r:
            text = r.read().decode()

        p   = _socib_parse_ascii(text)
        lat = float(p.get("LAT", "nan"))
        lon = float(p.get("LON", "nan"))
        hs_raw = p.get("WAV_HEI_SIG")
        hs  = float(hs_raw) if hs_raw is not None else float("nan")
        ts_posix = float(p.get("time", 0))

        if not (-90 <= lat <= 90 and -180 <= lon <= 180): return None
        if not (0 < hs < 30): return None
        obs_dt = datetime.utcfromtimestamp(ts_posix)
        if (datetime.utcnow() - obs_dt).total_seconds() > 12 * 3600: return None

        return {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [round(lon, 4), round(lat, 4)]},
            "properties": {
                "code":       f"socib.{station_dir}",
                "naam":       naam,
                "hm0_m":      round(hs, 2),
                "tijdstip":   obs_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "bron":       "SOCIB",
                "socib_path": nc_path,
            },
        }
    except Exception as e:
        print(f"[SOCIB wave] {station_dir}: {e}")
        return None


def fetch_socib_data():
    """Haal golfdata op van SOCIB THREDDS OPeNDAP (Middellandse Zee, Balearen)."""
    _ensure_socib_paths()
    with ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(_socib_fetch_station_wave, SOCIB_WAVE_DIRS))
    features = [r for r in results if r is not None]
    print(f"[SOCIB] {len(features)} golfstations geladen")
    return features


def _socib_fetch_station_wind(station_dir):
    """Haal actuele WIN_SPE + WIN_DIR op van één SOCIB weerstation via THREDDS OPeNDAP."""
    import re as _re
    nc_path = _socib_wind_paths.get(station_dir)
    if not nc_path:
        return None
    naam = SOCIB_WIND_NAMES.get(station_dir, station_dir)
    try:
        dds_url = f"{SOCIB_DODC}/{nc_path}.dds"
        with urllib.request.urlopen(
            urllib.request.Request(dds_url, headers={"User-Agent": "RWS-Golfhoogte-Proxy/1.0"}),
            timeout=10
        ) as r:
            dds = r.read().decode()
        m = _re.search(r'Float64 time\[time = (\d+)\]', dds)
        if not m: return None
        n = int(m.group(1)); last = n - 1

        data_url = (f"{SOCIB_DODC}/{nc_path}.ascii?"
                    f"LAT,LON,station_name,time[{last}:1:{last}],"
                    f"WIN_SPE[{last}:1:{last}],WIN_DIR[{last}:1:{last}]")
        with urllib.request.urlopen(
            urllib.request.Request(data_url, headers={"User-Agent": "RWS-Golfhoogte-Proxy/1.0"}),
            timeout=10
        ) as r:
            text = r.read().decode()

        p    = _socib_parse_ascii(text)
        lat  = float(p.get("LAT", "nan"))
        lon  = float(p.get("LON", "nan"))
        spd  = p.get("WIN_SPE")
        wdir = p.get("WIN_DIR")
        ts_posix = float(p.get("time", 0))

        if not (-90 <= lat <= 90 and -180 <= lon <= 180): return None
        wind_ms = round(float(spd), 1) if spd is not None else None
        if wind_ms is None or not (0 <= wind_ms <= 60): return None
        wind_dir = int(round(float(wdir))) % 360 if wdir is not None else None

        obs_dt = datetime.utcfromtimestamp(ts_posix)
        if (datetime.utcnow() - obs_dt).total_seconds() > 6 * 3600: return None

        return {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [round(lon, 4), round(lat, 4)]},
            "properties": {
                "code":       f"socib.wind.{station_dir}",
                "naam":       naam,
                "wind_ms":    wind_ms,
                "wind_dir":   wind_dir,
                "tijdstip":   obs_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "bron":       "SOCIB",
                "socib_path": nc_path,
            },
        }
    except Exception as e:
        print(f"[SOCIB wind] {station_dir}: {e}")
        return None


def fetch_socib_wind_data():
    """Haal winddata op van SOCIB THREDDS OPeNDAP (Middellandse Zee, Balearen)."""
    _ensure_socib_paths()
    with ThreadPoolExecutor(max_workers=6) as ex:
        results = list(ex.map(_socib_fetch_station_wind, SOCIB_WIND_DIRS))
    features = [r for r in results if r is not None]
    print(f"[SOCIB wind] {len(features)} stations geladen")
    return features


def fetch_socib_wave_history(nc_path, naam, code):
    """Haal 24u golfgeschiedenis op van een SOCIB THREDDS bestand."""
    import re as _re
    try:
        with urllib.request.urlopen(
            urllib.request.Request(f"{SOCIB_DODC}/{nc_path}.dds",
                                   headers={"User-Agent": "RWS-Golfhoogte-Proxy/1.0"}),
            timeout=10
        ) as r:
            dds = r.read().decode()
        m = _re.search(r'Float64 time\[time = (\d+)\]', dds)
        if not m: return {"code": code, "naam": naam, "data": []}
        n = int(m.group(1))
        start = max(0, n - 96)   # 96 punten = 24h bij uurlijkse data

        data_url = (f"{SOCIB_DODC}/{nc_path}.ascii?"
                    f"time[{start}:1:{n-1}],WAV_HEI_SIG[{start}:1:{n-1}]")
        with urllib.request.urlopen(
            urllib.request.Request(data_url, headers={"User-Agent": "RWS-Golfhoogte-Proxy/1.0"}),
            timeout=15
        ) as r:
            text = r.read().decode()

        p     = _socib_parse_ascii(text)
        times = p.get("time", [])
        vals  = p.get("WAV_HEI_SIG", [])
        if isinstance(times, (int, float)): times = [times]
        if isinstance(vals,  (int, float)): vals  = [vals]

        cutoff = datetime.utcnow() - timedelta(hours=24)
        data   = []
        for ts_posix, hs in zip(times, vals):
            try:
                obs_dt = datetime.utcfromtimestamp(float(ts_posix))
                if obs_dt < cutoff or not (0 < float(hs) < 30): continue
                data.append({"t": obs_dt.strftime("%Y-%m-%dT%H:%M:%SZ"), "v": round(float(hs), 2)})
            except Exception:
                pass
        data.sort(key=lambda x: x["t"])
        return {"code": code, "naam": naam, "data": data}
    except Exception as e:
        print(f"[SOCIB wave history] {e}")
        return {"code": code, "naam": naam, "data": []}


def fetch_socib_wind_history(nc_path, naam, code):
    """Haal 24u windgeschiedenis op van een SOCIB THREDDS weerstation-bestand."""
    import re as _re
    try:
        with urllib.request.urlopen(
            urllib.request.Request(f"{SOCIB_DODC}/{nc_path}.dds",
                                   headers={"User-Agent": "RWS-Golfhoogte-Proxy/1.0"}),
            timeout=10
        ) as r:
            dds = r.read().decode()
        m = _re.search(r'Float64 time\[time = (\d+)\]', dds)
        if not m: return {"code": code, "naam": naam, "data": [], "dir_data": []}
        n = int(m.group(1))
        start = max(0, n - 200)  # meer punten voor mogelijk hogere frequentie

        data_url = (f"{SOCIB_DODC}/{nc_path}.ascii?"
                    f"time[{start}:1:{n-1}],"
                    f"WIN_SPE[{start}:1:{n-1}],WIN_DIR[{start}:1:{n-1}]")
        with urllib.request.urlopen(
            urllib.request.Request(data_url, headers={"User-Agent": "RWS-Golfhoogte-Proxy/1.0"}),
            timeout=15
        ) as r:
            text = r.read().decode()

        p     = _socib_parse_ascii(text)
        times = p.get("time", [])
        spds  = p.get("WIN_SPE", [])
        dirs  = p.get("WIN_DIR", [])
        if isinstance(times, (int, float)): times = [times]
        if isinstance(spds,  (int, float)): spds  = [spds]
        if isinstance(dirs,  (int, float)): dirs  = [dirs]

        cutoff   = datetime.utcnow() - timedelta(hours=24)
        data     = []
        dir_data = []
        for i, ts_posix in enumerate(times):
            try:
                obs_dt = datetime.utcfromtimestamp(float(ts_posix))
                if obs_dt < cutoff: continue
                t   = obs_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                spd = float(spds[i]) if i < len(spds) else None
                d   = float(dirs[i]) if i < len(dirs) else None
                if spd is not None and 0 <= spd <= 60:
                    data.append({"t": t, "v": round(spd, 1)})
                if d is not None:
                    dir_data.append({"t": t, "v": int(round(d)) % 360})
            except Exception:
                pass
        data.sort(key=lambda x: x["t"])
        dir_data.sort(key=lambda x: x["t"])
        return {"code": code, "naam": naam, "data": data, "dir_data": dir_data}
    except Exception as e:
        print(f"[SOCIB wind history] {e}")
        return {"code": code, "naam": naam, "data": [], "dir_data": []}


def fetch_cdip_data():

    """Fetch wave data from CDIP (Coastal Data Information Program) via THREDDS OPeNDAP."""
    global _cdip_cache, _cdip_time
    import re as _re

    now = time.time()
    if _cdip_cache is not None and (now - _cdip_time) < 3600:
        return _cdip_cache

    # Get station list from THREDDS catalog
    try:
        req = urllib.request.Request(CDIP_CATALOG, headers={"User-Agent": "RWS-Golfhoogte-Proxy/1.0"})
        r = urllib.request.urlopen(req, timeout=15)
        catalog_xml = r.read().decode()
        station_files = _re.findall(r'name="(\d+p1_rt\.nc)"', catalog_xml)
    except Exception as e:
        print(f"[CDIP] catalog fout: {e}")
        return _cdip_cache or []

    def fetch_station(stn_file):
        stn_id = stn_file.replace("_rt.nc", "")  # e.g. "028p1"
        try:
            # Step 1: get DDS to find array dimension size
            dds_url = f"{CDIP_ODAP}/{stn_file}.dds"
            req_dds = urllib.request.Request(dds_url, headers={"User-Agent": "RWS-Golfhoogte-Proxy/1.0"})
            r_dds = urllib.request.urlopen(req_dds, timeout=10)
            dds = r_dds.read().decode()
            m = _re.search(r'waveHs\[waveTime = (\d+)\]', dds)
            if not m:
                return None
            n = int(m.group(1))
            if n == 0:
                return None
            last = n - 1

            # Step 2: fetch last waveHs, waveTime, lat, lon
            data_url = (f"{CDIP_ODAP}/{stn_file}.ascii?"
                        f"waveHs[{last}:1:{last}],"
                        f"waveTime[{last}:1:{last}],"
                        f"metaDeployLatitude[0:1:0],"
                        f"metaDeployLongitude[0:1:0],"
                        f"metaStationName[0:1:0]")
            req_d = urllib.request.Request(data_url, headers={"User-Agent": "RWS-Golfhoogte-Proxy/1.0"})
            r_d = urllib.request.urlopen(req_d, timeout=10)
            text = r_d.read().decode()

            # Parse ASCII OPeNDAP response
            # Arrays: key[N] header then value on next line
            # Scalars: "keyname, value" on one line
            parsed = {}
            cur_key = None
            for line in text.splitlines():
                line = line.strip()
                if not line or line.startswith("Dataset") or line.startswith("}"):
                    cur_key = None
                    continue
                # Scalar inline format
                if ", " in line and not line.startswith("-") and "[" not in line:
                    k, v = line.split(", ", 1)
                    if "Latitude" in k:
                        parsed["lat"] = v
                    elif "Longitude" in k:
                        parsed["lon"] = v
                    elif "StationName" in k:
                        parsed["name"] = v.strip('"')
                    cur_key = None
                    continue
                # Array header
                if line.startswith("waveHs"):
                    cur_key = "hs"
                elif line.startswith("waveTime"):
                    cur_key = "ts"
                elif cur_key:
                    parsed[cur_key] = line.rstrip(",")
                    cur_key = None

            hs  = float(parsed.get("hs", "nan"))
            ts  = int(parsed.get("ts", 0))
            lat = float(parsed.get("lat", "nan"))
            lon = float(parsed.get("lon", "nan"))
            naam = parsed.get("name", stn_id).strip('"')

            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                return None
            if not (0 < hs < 30):
                return None
            # Max 24h staleness
            obs_dt = datetime.utcfromtimestamp(ts)
            age    = (datetime.utcnow() - obs_dt).total_seconds()
            if age > 86400:
                return None

            return {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [round(lon, 4), round(lat, 4)]},
                "properties": {
                    "code":     f"cdip.{stn_id}",
                    "naam":     naam,
                    "hm0_m":   round(hs, 2),
                    "tijdstip": obs_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "bron":    "CDIP",
                },
            }
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=20) as ex:
        results = list(ex.map(fetch_station, station_files))

    features = [r for r in results if r is not None]
    print(f"[CDIP] {len(features)} stations met verse data (van {len(station_files)} realtime stations)")
    _cdip_cache = features
    _cdip_time  = now
    return features


_refresh_lock = threading.Lock()

def _do_refresh():
    """Haal alle bronnen parallel op en sla op in cache."""
    global _cache, _cache_time, _stations

    if _stations is None:
        _stations = fetch_hm0_stations()

    # RWS, BSH, CEFAS, La Bouée, NDBC, FMI, CDIP en SOCIB parallel ophalen
    with ThreadPoolExecutor(max_workers=8) as ex:
        fut_rws     = ex.submit(fetch_latest_values, _stations)
        fut_bsh     = ex.submit(fetch_bsh_data)
        fut_cefas   = ex.submit(fetch_cefas_data)
        fut_labouee = ex.submit(fetch_labouee_data)
        fut_ndbc    = ex.submit(fetch_ndbc_data)
        fut_fmi     = ex.submit(fetch_fmi_data)
        fut_cdip    = ex.submit(fetch_cdip_data)
        fut_socib   = ex.submit(fetch_socib_data)

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

        try:
            rws_geojson["features"].extend(fut_ndbc.result())
        except Exception as e:
            print(f"[NDBC] Fout: {e}")

        try:
            rws_geojson["features"].extend(fut_fmi.result())
        except Exception as e:
            print(f"[FMI] Fout: {e}")

        try:
            rws_geojson["features"].extend(fut_cdip.result())
        except Exception as e:
            print(f"[CDIP] Fout: {e}")

        try:
            rws_geojson["features"].extend(fut_socib.result())
        except Exception as e:
            print(f"[SOCIB] Fout: {e}")

    rws_geojson["aantalStations"] = len(rws_geojson["features"])
    _cache      = rws_geojson
    _cache_time = time.time()
    print(f"[TOTAAL] {_cache['aantalStations']} stations (RWS + BSH + CEFAS + LaBouée + NDBC + FMI + CDIP + SOCIB)")


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

    # ── NDBC, LaBouée, CDIP en IMI temperatuur parallel ophalen ──────
    with ThreadPoolExecutor(max_workers=4) as ex:
        fut_ndbc_t  = ex.submit(_fetch_ndbc_temp)
        fut_lb_t    = ex.submit(_fetch_labouee_temp)
        fut_cdip_t  = ex.submit(_fetch_cdip_temp)
        fut_imi_t   = ex.submit(_fetch_imi_temp)
        for label, fut in [("NDBC temp", fut_ndbc_t), ("LaBouée temp", fut_lb_t),
                            ("CDIP temp", fut_cdip_t), ("IMI temp", fut_imi_t)]:
            try:
                res = fut.result()
                features.extend(res)
                print(f"[{label}] {len(res)} stations")
            except Exception as e:
                print(f"[{label}] Fout: {e}")

    _temp_cache = {"type": "FeatureCollection", "features": features,
                   "opgehaald": now.isoformat(), "aantalStations": len(features)}
    _temp_time  = time.time()
    print(f"[TEMP TOTAAL] {len(features)} stations")
    return _temp_cache


def _fetch_ndbc_temp():
    """Zeewatertemperatuur (WTMP, col 18) uit NDBC latest_obs.txt (al in geheugen via fetch_ndbc_data)."""
    # Haal bestand opnieuw op (apart gecached via _ndbc_wind_features al in memory,
    # maar WTMP niet opgeslagen — snel opnieuw ophalen, file is al gecached op OS-niveau)
    req = urllib.request.Request(NDBC_URL, headers={"User-Agent": "Mozilla/5.0 (compatible; ZeedataProxy/1.0)"})
    with urllib.request.urlopen(req, timeout=20) as r:
        lines = r.read().decode("utf-8", errors="replace").splitlines()

    now = datetime.now(timezone.utc)
    features = []
    for line in lines:
        if line.startswith("#") or not line.strip():
            continue
        cols = line.split()
        if len(cols) < 19:
            continue
        if cols[18] == "MM":
            continue
        try:
            lat  = float(cols[1]); lon = float(cols[2])
            wtmp = round(float(cols[18]), 1)
            if not (-2 < wtmp < 35):
                continue
            tijdstip = datetime(int(cols[3]), int(cols[4]), int(cols[5]),
                                int(cols[6]), int(cols[7]), tzinfo=timezone.utc)
            if (now - tijdstip).total_seconds() > 6 * 3600:
                continue
            stn = cols[0]
        except (ValueError, IndexError):
            continue
        features.append({"type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "code":     f"ndbc.temp.{stn.lower()}",
                "naam":     f"NDBC {stn}",
                "temp_c":   wtmp,
                "tijdstip": tijdstip.isoformat(),
                "bron":     "NDBC/NOAA",
            }})
    return features


def _fetch_labouee_temp():
    """Zeewatertemperatuur uit LaBouée buoys (water_temp_c veld)."""
    features = []
    for slug, (naam, lat, lon) in LABOUEE_STATIONS.items():
        try:
            url = f"{LABOUEE_BASE}/{slug}/latest.json"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read())
            if data.get("status") != "ok":
                continue
            reading = data.get("latest_reading", {})
            temp_c  = reading.get("water_temp_c")
            if temp_c is None or float(temp_c) <= 0:
                continue
            temp_c = round(float(temp_c), 1)
            if not (-2 < temp_c < 35):
                continue
            tijdstip = reading.get("measured_at", "")
            features.append({"type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {
                    "code":     f"labouee.temp.{slug}",
                    "naam":     naam,
                    "temp_c":   temp_c,
                    "tijdstip": tijdstip,
                    "bron":     "LaBouee",
                }})
        except Exception:
            continue
    return features


def _fetch_cdip_temp():
    """Zeewatertemperatuur uit CDIP stations via THREDDS OPeNDAP (sstSeaSurfaceTemperature)."""
    import re as _re
    try:
        req = urllib.request.Request(CDIP_CATALOG, headers={"User-Agent": "RWS-Golfhoogte-Proxy/1.0"})
        r   = urllib.request.urlopen(req, timeout=15)
        station_files = _re.findall(r'name="(\d+p1_rt\.nc)"', r.read().decode())
    except Exception as e:
        print(f"[CDIP temp] catalog fout: {e}")
        return []

    def fetch_stn(stn_file):
        stn_id = stn_file.replace("_rt.nc", "")
        try:
            dds_url = f"{CDIP_ODAP}/{stn_file}.dds"
            r_dds   = urllib.request.urlopen(urllib.request.Request(dds_url, headers={"User-Agent": "RWS-Golfhoogte-Proxy/1.0"}), timeout=10)
            dds     = r_dds.read().decode()
            m = _re.search(r'sstSeaSurfaceTemperature\[sstTime = (\d+)\]', dds)
            if not m: return None
            n = int(m.group(1)); last = n - 1
            # Also get lat/lon from waveHs section
            mw = _re.search(r'waveHs\[waveTime = (\d+)\]', dds)
            wlast = int(mw.group(1)) - 1 if mw else 0

            data_url = (f"{CDIP_ODAP}/{stn_file}.ascii?"
                        f"sstSeaSurfaceTemperature[{last}:1:{last}],"
                        f"sstTime[{last}:1:{last}],"
                        f"metaDeployLatitude[0:1:0],"
                        f"metaDeployLongitude[0:1:0],"
                        f"metaStationName[0:1:0]")
            r_d  = urllib.request.urlopen(urllib.request.Request(data_url, headers={"User-Agent": "RWS-Golfhoogte-Proxy/1.0"}), timeout=10)
            text = r_d.read().decode()

            parsed = {}; cur_key = None
            for line in text.splitlines():
                line = line.strip()
                if not line or line.startswith("Dataset") or line.startswith("}"): cur_key = None; continue
                if ", " in line and "[" not in line:
                    k, v = line.split(", ", 1)
                    if "Latitude" in k:   parsed["lat"]  = v
                    elif "Longitude" in k: parsed["lon"]  = v
                    elif "StationName" in k: parsed["name"] = v.strip('"')
                    cur_key = None; continue
                if line.startswith("sstSeaSurfaceTemperature"): cur_key = "sst"
                elif line.startswith("sstTime"):                  cur_key = "ts"
                elif cur_key: parsed[cur_key] = line.rstrip(","); cur_key = None

            sst  = float(parsed.get("sst", "nan"))
            ts   = int(parsed.get("ts",  0))
            lat  = float(parsed.get("lat", "nan"))
            lon  = float(parsed.get("lon", "nan"))
            naam = parsed.get("name", stn_id)

            if not (-2 < sst < 35): return None
            if not (-90 <= lat <= 90 and -180 <= lon <= 180): return None
            age = (datetime.utcnow() - datetime.utcfromtimestamp(ts)).total_seconds()
            if age > 86400: return None

            return {"type": "Feature",
                "geometry": {"type": "Point", "coordinates": [round(lon, 4), round(lat, 4)]},
                "properties": {
                    "code":     f"cdip.temp.{stn_id}",
                    "naam":     naam,
                    "temp_c":   round(sst, 1),
                    "tijdstip": datetime.utcfromtimestamp(ts).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "bron":     "CDIP",
                }}
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=20) as ex:
        results = list(ex.map(fetch_stn, station_files))
    return [r for r in results if r is not None]


def _fetch_imi_temp():
    """Zeewatertemperatuur uit Irish Marine Institute ERDDAP (IWBNetwork)."""
    from datetime import datetime, timedelta
    start = (datetime.utcnow() - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
    url = (f"https://erddap.marine.ie/erddap/tabledap/IWBNetwork.json"
           f"?time,latitude,longitude,station_id,SeaTemperature"
           f"&time%3E={start}&orderByMax(%22station_id,time%22)")
    try:
        req  = urllib.request.Request(url, headers={"User-Agent": "RWS-Golfhoogte-Proxy/1.0"})
        r    = urllib.request.urlopen(req, timeout=20)
        data = json.loads(r.read())
        rows = data["table"]["rows"]
    except Exception as e:
        print(f"[IMI temp] fetch fout: {e}")
        return []

    features = []
    for row in rows:
        ts_str, lat, lon, naam, temp = row
        if temp is None or lat is None or lon is None:
            continue
        temp_c = round(float(temp), 1)
        if not (-2 < temp_c < 35):
            continue
        features.append({"type": "Feature",
            "geometry": {"type": "Point", "coordinates": [round(float(lon), 4), round(float(lat), 4)]},
            "properties": {
                "code":     f"imi.temp.{naam.replace(' ', '_')}",
                "naam":     naam,
                "temp_c":   temp_c,
                "tijdstip": ts_str,
                "bron":     "IMI",
            }})
    return features


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
    """Haal één batch windwaarnemingen op (snelheid + richting parallel)."""
    loc_list = [{"Code": s["Code"]} for s in batch]

    def call(grootheid, eenheid):
        try:
            return rws_post("/ONLINEWAARNEMINGENSERVICES/OphalenLaatsteWaarnemingen", {
                "AquoPlusWaarnemingMetadataLijst": [{"AquoMetadata": {
                    "Compartiment": {"Code": "LT"},
                    "Eenheid":      {"Code": eenheid},
                    "Grootheid":    {"Code": grootheid},
                }}],
                "LocatieLijst": loc_list,
            })
        except Exception:
            return {}

    def best_per_loc(resp):
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
        return best

    with ThreadPoolExecutor(max_workers=2) as ex:
        fut_spd = ex.submit(call, "WINDSHD", "m/s")
        fut_dir = ex.submit(call, "WINDRTG", "graad")
        best_spd = best_per_loc(fut_spd.result())
        best_dir = best_per_loc(fut_dir.result())

    # Richting per loc_code opslaan
    dir_by_loc = {}
    for loc_code, w in best_dir.items():
        metingen = w.get("MetingenLijst") or []
        waarde   = (metingen[0].get("Meetwaarde") or {}).get("Waarde_Numeriek") if metingen else None
        if waarde is not None:
            dir_by_loc[loc_code] = int(round(waarde)) % 360

    features = []
    for loc_code, w in best_spd.items():
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
                "wind_dir": dir_by_loc.get(loc_code),
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

        print(f"[WIND] {len(features)} RWS stations geladen")
    except Exception as e:
        print(f"[WIND] Fout: {e}")

    # NDBC windstations toevoegen (gevuld door fetch_ndbc_data in _do_refresh)
    ndbc_wind = _ndbc_wind_features
    features.extend(ndbc_wind)
    print(f"[WIND] +{len(ndbc_wind)} NDBC windstations → totaal {len(features)}")

    # SOCIB windstations toevoegen (Middellandse Zee)
    try:
        socib_wind = fetch_socib_wind_data()
        features.extend(socib_wind)
        print(f"[WIND] +{len(socib_wind)} SOCIB windstations → totaal {len(features)}")
    except Exception as e:
        print(f"[SOCIB wind] Fout: {e}")

    _wind_cache = {"type": "FeatureCollection", "features": features,
                   "opgehaald": now.isoformat(), "aantalStations": len(features)}
    _wind_time  = time.time()
    return _wind_cache


def fetch_wind_history(rws_code, naam):
    """Haal 24u windsnelheid + richting parallel op van RWS."""
    now   = datetime.now(timezone.utc)
    start = now - timedelta(hours=24)
    fmt   = "%Y-%m-%dT%H:%M:%S.000+00:00"
    periode = {"Begindatumtijd": start.strftime(fmt), "Einddatumtijd": now.strftime(fmt)}
    locatie = {"Code": rws_code}

    def call(grootheid, eenheid):
        return rws_post("/ONLINEWAARNEMINGENSERVICES/OphalenWaarnemingen", {
            "AquoPlusWaarnemingMetadata": {"AquoMetadata": {
                "Compartiment": {"Code": "LT"},
                "Eenheid":      {"Code": eenheid},
                "Grootheid":    {"Code": grootheid},
            }},
            "Locatie": locatie,
            "Periode": periode,
        })

    with ThreadPoolExecutor(max_workers=2) as ex:
        fut_spd = ex.submit(call, "WINDSHD", "m/s")
        fut_dir = ex.submit(call, "WINDRTG", "graad")
        resp_spd = fut_spd.result()
        resp_dir = fut_dir.result()

    def extract(resp, transform=None):
        waarnemingen = resp.get("WaarnemingenLijst") or []
        if not waarnemingen:
            return []
        best = max(waarnemingen, key=lambda w: len(w.get("MetingenLijst") or []))
        out = []
        for m in (best.get("MetingenLijst") or []):
            waarde   = (m.get("Meetwaarde") or {}).get("Waarde_Numeriek")
            tijdstip = m.get("Tijdstip")
            if waarde is not None and tijdstip:
                v = transform(waarde) if transform else round(waarde, 1)
                out.append({"t": tijdstip, "v": v})
        out.sort(key=lambda x: x["t"])
        return out

    data     = extract(resp_spd)
    dir_data = extract(resp_dir, transform=lambda v: int(round(v)) % 360)
    return {"code": f"rws.wind.{rws_code.lower()}", "naam": naam, "data": data, "dir_data": dir_data}


# ── Nederland landsgrens (PDOK) ──────────────────────────────────────────────

_nl_border_cache = None
_nl_border_time  = 0

def get_nl_border():
    global _nl_border_cache, _nl_border_time
    if _nl_border_cache and (time.time() - _nl_border_time) < 86400:
        return _nl_border_cache
    urls = [
        "https://service.pdok.nl/kadaster/bestuurlijkegebieden/wfs/v1_0"
        "?service=WFS&version=2.0.0&request=GetFeature"
        "&typeName=bestuurlijkegebieden:Landgebied"
        "&srsName=EPSG:4326&outputFormat=application/json",
    ]
    for url in urls:
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read().decode("utf-8"))
            print(f"[NL-border] Geladen ({len(str(data))} bytes)")
            _nl_border_cache = data
            _nl_border_time  = time.time()
            return data
        except Exception as e:
            print(f"[NL-border] Fout: {e}")
    return None

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

        # Sla offshore/zee-platforms over (Lichteiland, Europlatform, K13, etc.)
        naam_l = naam.lower()
        if any(kw in naam_l for kw in (
            "lichteiland", "europlatform", "k13", "meetpost", "platform",
            "roughness", "north sea", "noordzee",
        )):
            continue
        ts   = s.get("timestamp", now.isoformat())

        def fval(key):
            v = s.get(key)
            return round(float(v), 2) if v is not None and v != "" else None

        def ival(key):
            v = s.get(key)
            return int(v) if v is not None and v != "" else None

        # Sla stations zonder temperatuurdata over
        temp = fval("temperature")
        if temp is None:
            continue

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
                "ta":   temp,
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
                if code.startswith("socib.wind."):
                    # SOCIB weerstation wind history via THREDDS OPeNDAP
                    socib_path = None; naam_wh = code
                    cached_wnd = get_wind_data()
                    for feat in cached_wnd.get("features", []):
                        if feat["properties"].get("code") == code:
                            socib_path = feat["properties"].get("socib_path")
                            naam_wh    = feat["properties"].get("naam", naam_wh)
                            break
                    if socib_path:
                        data = fetch_socib_wind_history(socib_path, naam_wh, code)
                    else:
                        data = {"code": code, "naam": naam_wh, "data": [], "dir_data": []}
                elif code.startswith("ndbc.wind."):
                    station_id = code[len("ndbc.wind."):]
                    data = fetch_ndbc_wind_history(station_id)
                else:
                    # code = "rws.wind.{loc_code}" → strip prefix
                    rws_code = code[9:].upper() if code.startswith("rws.wind.") else code.upper()
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

        # ── /api/nl-border ───────────────────────────────────────────────
        elif path == "/api/nl-border":
            data = get_nl_border()
            if data:
                body = json.dumps(data, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type",   "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control",  "public, max-age=86400")
                self.send_cors()
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(503)
                self.send_cors()
                self.end_headers()

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
                if code.startswith("socib.") and not code.startswith("socib.wind."):
                    # SOCIB golfstation history via THREDDS OPeNDAP
                    socib_path = None; naam_h = code
                    cached = get_data()
                    for feat in cached.get("features", []):
                        if feat["properties"].get("code") == code:
                            socib_path = feat["properties"].get("socib_path")
                            naam_h     = feat["properties"].get("naam", naam_h)
                            break
                    if socib_path:
                        data = fetch_socib_wave_history(socib_path, naam_h, code)
                    else:
                        data = {"code": code, "naam": naam_h, "data": []}
                elif code.startswith("cefas."):
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
                elif code.startswith("ndbc."):
                    station_id = code[5:].upper()
                    data = fetch_ndbc_history(station_id)
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
                                     "index.html")
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

    import socket as _socket
    class DualStackServer(HTTPServer):
        """Luistert op IPv4 én IPv6 zodat Safari via localhost verbinding maakt."""
        address_family = _socket.AF_INET6
        def server_bind(self):
            self.socket.setsockopt(_socket.IPPROTO_IPV6, _socket.IPV6_V6ONLY, 0)
            super().server_bind()

    # Server direct starten zodat Safari meteen verbinding kan maken
    server = DualStackServer(("::", PORT), Handler)
    print(f"[SERVER] Luistert op http://localhost:{PORT}/ (IPv4 + IPv6)\n")

    # Prewarm + achtergrond-refresh in aparte thread — blokkeert de server niet
    def _background_loop():
        # Fase 1: waves snel ophalen zodat de server meteen reageert
        print("[CACHE] Fase 1: waves ophalen…")
        try:
            _refresh_waves()
            print("[CACHE] Waves klaar.\n")
        except Exception as e:
            print(f"[CACHE] Waves fout: {e}\n")
        # Fase 2: temp + wind parallel (langzamer vanwege RWS catalog + CDIP)
        print("[CACHE] Fase 2: temp + wind + KNMI ophalen…")
        try:
            with ThreadPoolExecutor(max_workers=3) as ex:
                ft    = ex.submit(get_temp_data)
                fwnd  = ex.submit(get_wind_data)
                fknmi = ex.submit(get_knmi_data)
                ft.result(); fwnd.result()
                try: fknmi.result()
                except Exception as e: print(f"[KNMI] Prewarm mislukt: {e}")
            print("[CACHE] Alles klaar.\n")
        except Exception as e:
            print(f"[CACHE] Fout bij vooraf laden: {e}\n")
        # Daarna elke 9 minuten herhalen
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

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nProxy gestopt.")
