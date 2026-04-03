#!/usr/bin/env python3
"""
Haalt actuele golfhoogte-data op van RWS, BSH en CEFAS en slaat deze op
als statische JSON-bestanden die door GitHub Pages geserveerd worden.
Wordt elke 10 minuten aangeroepen door GitHub Actions.
"""

import json
import os
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── Directories ───────────────────────────────────────────────────────────────

DATA_DIR    = Path("data")
HISTORY_DIR = DATA_DIR / "history"
DATA_DIR.mkdir(exist_ok=True)
HISTORY_DIR.mkdir(exist_ok=True)

# ── API-basisadressen ─────────────────────────────────────────────────────────

RWS_BASE      = "https://ddapi20-waterwebservices.rijkswaterstaat.nl"
CEFAS_API     = "https://wavenet-api.cefas.co.uk/api"
CEFAS_HEADERS = {
    "Accept":    "application/json",
    "User-Agent": "Mozilla/5.0",
    "Origin":    "https://wavenet.cefas.co.uk",
    "Referer":   "https://wavenet.cefas.co.uk/",
}
BSH_URL = "https://www2.bsh.de/aktdat/seegang/Seegang_MARNET_FINO_RAVE.txt"


# ── Hulpfuncties ─────────────────────────────────────────────────────────────

def code_to_filename(code):
    """Zet stationscode om naar veilige bestandsnaam (bijv. bsh.hel → bsh-hel)."""
    return code.replace(".", "-").replace("/", "-").replace(" ", "_")


def rws_post(path, body):
    data = json.dumps(body).encode("utf-8")
    req  = urllib.request.Request(
        RWS_BASE + path,
        data    = data,
        method  = "POST",
        headers = {
            "Content-Type": "application/json",
            "Accept":       "application/json",
            "User-Agent":   "RWS-Golfhoogte/2.0",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def load_history(code):
    """Laad bestaande geschiedenis uit bestand (of geef lege lijst terug)."""
    path = HISTORY_DIR / f"{code_to_filename(code)}.json"
    if path.exists():
        try:
            saved = json.loads(path.read_text())
            return saved.get("data", [])
        except Exception:
            pass
    return []


def save_history(code, naam, data_points):
    """Sla geschiedenis op, gesorteerd en beperkt tot de afgelopen 25 uur."""
    now    = datetime.now(timezone.utc)
    cutoff = (now - timedelta(hours=25)).isoformat()
    pruned = sorted(
        [pt for pt in data_points if pt["t"] >= cutoff],
        key=lambda x: x["t"],
    )
    path = HISTORY_DIR / f"{code_to_filename(code)}.json"
    path.write_text(json.dumps(
        {"code": code, "naam": naam, "data": pruned},
        ensure_ascii=False,
    ))
    return pruned


def append_to_history(code, naam, tijdstip, hm0_m):
    """Voeg één meting toe aan de geschiedenis (geen duplicaten op tijdstip)."""
    if tijdstip is None or hm0_m is None:
        return
    existing = load_history(code)
    timestamps = {pt["t"] for pt in existing}
    if tijdstip not in timestamps:
        existing.append({"t": tijdstip, "v": hm0_m})
    save_history(code, naam, existing)


# ── RWS: catalogus en laatste waarnemingen ────────────────────────────────────

INLAND_KEYWORDS = [
    "ijsselmeer", "markermeer", "markerwaard", "markerwadden",
    "slotermeer", "woudsend",
]
SHIP_CODES = {
    "texel.lichtschip", "penrod", "seanpplatform",
    "petten.meetraai3", "ijmuiden.5a", "ijgeul.2.boei",
    "noordzee.boei.b75n", "noordwijk.meetpost", "texel",
    "q1.1", "d15",
}

def is_excluded(code, naam):
    code_l = (code or "").lower()
    naam_l = (naam or "").lower()
    if code_l in SHIP_CODES:
        return True
    return any(kw in code_l or kw in naam_l for kw in INLAND_KEYWORDS)


def fetch_rws():
    print("[RWS] Catalogus ophalen…")
    catalog = rws_post(
        "/METADATASERVICES/OphalenCatalogus",
        {"CatalogusFilter": {
            "Grootheden": True, "Eenheden": True,
            "Compartimenten": True, "ProcesTypes": True, "Groeperingen": True,
        }},
    )

    hm0_meta_ids = {
        m["AquoMetadata_MessageID"]
        for m in catalog.get("AquoMetadataLijst", [])
        if m.get("Grootheid", {}).get("Code") == "Hm0"
    }
    hm0_loc_ids = {
        rel["Locatie_MessageID"]
        for rel in catalog.get("AquoMetadataLocatieLijst", [])
        if rel.get("AquoMetaData_MessageID") in hm0_meta_ids
    }
    stations = [
        loc for loc in catalog.get("LocatieLijst", [])
        if loc.get("Locatie_MessageID") in hm0_loc_ids
        and loc.get("Lat") is not None and loc.get("Lon") is not None
    ]
    print(f"[RWS] {len(stations)} stations gevonden")

    # Laatste waarnemingen ophalen (batches van 20)
    BATCH = 20
    waarnemingen = []
    for i in range(0, len(stations), BATCH):
        batch = stations[i:i + BATCH]
        resp  = rws_post(
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
        waarnemingen.extend(resp.get("WaarnemingenLijst", []))

    station_map = {s["Code"]: s for s in stations}
    now     = datetime.now(timezone.utc)
    features = []

    # Dedupliceer per station (recentste meting wint)
    best = {}
    for w in waarnemingen:
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
        station = station_map.get(loc_code) or w.get("Locatie") or {}
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

        # 48-uurs filter
        if tijdstip:
            try:
                dt = datetime.fromisoformat(tijdstip)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                if (now - dt).total_seconds() > 48 * 3600:
                    continue
            except Exception:
                pass

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "code": loc_code, "naam": naam,
                "hm0_m": hm0_m, "tijdstip": tijdstip,
                "status": None, "kwaliteit": None,
            },
        })
        append_to_history(loc_code, naam, tijdstip, hm0_m)

    print(f"[RWS] {len(features)} stations na filters")
    return features


# ── BSH MARNET ────────────────────────────────────────────────────────────────

BSH_STATIONS = {
    "HEL": ("Helgoland-Süd",   54.1750,  7.8840),
    "HEO": ("Helgoland-Nord",  54.1870,  7.9070),
    "LTH": ("Helgoland LT",    54.1500,  7.9970),
    "BUD": ("Butendiek",       54.9920,  7.7430),
    "DBU": ("Deutsche Bucht",  54.1770,  6.3280),
    "NO1": ("NordseeOne",      54.4345,  6.6220),
    "NOR": ("Nordergründe",    53.7390,  8.3160),
    "ELB": ("Elbe",            54.0028,  8.1017),
    "NOO": ("NordseOst",       54.4340,  6.6580),
}

def fetch_bsh():
    req = urllib.request.Request(BSH_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=15) as r:
        content = r.read().decode("utf-8", errors="replace")

    now      = datetime.now(timezone.utc)
    features = []

    for line in content.strip().splitlines():
        parts = line.split()
        if len(parts) < 4 or parts[0] == "Typ":
            continue
        ort = parts[1]
        if ort not in BSH_STATIONS:
            continue

        naam, lat, lon = BSH_STATIONS[ort]
        code = f"bsh.{ort.lower()}"

        try:
            hm0 = round(float(parts[3]), 2)
        except ValueError:
            hm0 = None

        try:
            ts_dt    = datetime.strptime(parts[2], "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
            if (now - ts_dt).total_seconds() > 48 * 3600:
                continue
            tijdstip = ts_dt.isoformat()
        except ValueError:
            tijdstip = None

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "code": code, "naam": naam,
                "hm0_m": hm0, "tijdstip": tijdstip,
                "status": None, "kwaliteit": None, "bron": "BSH",
            },
        })
        append_to_history(code, naam, tijdstip, hm0)

    print(f"[BSH] {len(features)} stations")
    return features


# ── CEFAS WaveNet ─────────────────────────────────────────────────────────────

def fetch_cefas():
    req = urllib.request.Request(
        f"{CEFAS_API}/Map/Current", headers=CEFAS_HEADERS)
    with urllib.request.urlopen(req, timeout=20) as r:
        data = json.loads(r.read().decode("utf-8"))

    now      = datetime.now(timezone.utc)
    features = []

    for f in data.get("features", []):
        props  = f.get("properties", {})
        coords = f.get("geometry", {}).get("coordinates", [])
        if len(coords) < 2:
            continue

        lon, lat = coords[0], coords[1]
        if lon < -10 or lat < 49:
            continue

        station_id = props.get("id", "")
        source     = props.get("source", "INT")
        naam       = props.get("title", station_id)
        tijdstip_s = props.get("timestamp", "")
        code       = f"cefas.{station_id.lower()}"

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
        hm0_vals = hm0_info.get("values", [])
        try:
            hm0_m = round(float(hm0_vals[0]), 2) if hm0_vals and hm0_vals[0] else None
        except (ValueError, IndexError):
            hm0_m = None

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "code": code, "naam": naam,
                "hm0_m": hm0_m, "tijdstip": tijdstip_s,
                "status": None, "kwaliteit": None,
                "bron": "CEFAS",
                "cefas_id": station_id, "cefas_source": source,
            },
        })

        # CEFAS geschiedenis: haal volledige 24u op via hun API
        fetch_cefas_history(station_id, source, code, naam)

    print(f"[CEFAS] {len(features)} stations")
    return features


def fetch_cefas_history(station_id, source, code, naam):
    """Haal 24-uursgeschiedenis op van CEFAS Detail/Results API."""
    now   = datetime.now(timezone.utc)
    start = (now - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S")
    end   = now.strftime("%Y-%m-%dT%H:%M:%S")
    url   = (f"{CEFAS_API}/Detail/Results/{station_id}/{source}"
             f"?showForecast=false&dateFrom={start}&dateTo={end}")
    try:
        req = urllib.request.Request(url, headers=CEFAS_HEADERS)
        with urllib.request.urlopen(req, timeout=15) as r:
            rows = json.loads(r.read().decode("utf-8"))

        data = []
        for row in rows:
            if row.get("isForecast"):
                continue
            ts  = row.get("timestamp", "")
            hm0 = next(
                (x.get("value") for x in row.get("results", [])
                 if x.get("identifier") == "Hm0"),
                None,
            )
            if ts and hm0:
                try:
                    data.append({"t": ts, "v": round(float(hm0), 2)})
                except ValueError:
                    pass

        data.sort(key=lambda x: x["t"])
        save_history(code, naam, data)
    except Exception as e:
        print(f"[CEFAS history] {station_id}: {e}")


# ── Hoofdprogramma ────────────────────────────────────────────────────────────

def main():
    now      = datetime.now(timezone.utc)
    features = []

    try:
        features.extend(fetch_rws())
    except Exception as e:
        print(f"[RWS] Fout: {e}")

    try:
        features.extend(fetch_bsh())
    except Exception as e:
        print(f"[BSH] Fout: {e}")

    try:
        features.extend(fetch_cefas())
    except Exception as e:
        print(f"[CEFAS] Fout: {e}")

    geojson = {
        "type":           "FeatureCollection",
        "features":       features,
        "opgehaald":      now.isoformat(),
        "aantalStations": len(features),
    }

    (DATA_DIR / "waves.json").write_text(
        json.dumps(geojson, ensure_ascii=False, indent=None),
    )

    print(f"[KLAAR] {len(features)} stations opgeslagen in data/waves.json")


if __name__ == "__main__":
    main()
