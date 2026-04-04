#!/usr/bin/env python3
"""
Haalt actuele golfhoogte- en temperatuurdata op van RWS, BSH en CEFAS en slaat
deze op als statische JSON-bestanden die door GitHub Pages geserveerd worden.
Wordt elke 10 minuten aangeroepen door GitHub Actions.
"""

import json
import os
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── Directories ───────────────────────────────────────────────────────────────

DATA_DIR      = Path("data")
HISTORY_DIR   = DATA_DIR / "history"
TEMP_HIST_DIR = DATA_DIR / "temp-history"
DATA_DIR.mkdir(exist_ok=True)
HISTORY_DIR.mkdir(exist_ok=True)
TEMP_HIST_DIR.mkdir(exist_ok=True)

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


def load_temp_history(code):
    path = TEMP_HIST_DIR / f"{code_to_filename(code)}.json"
    if path.exists():
        try:
            return json.loads(path.read_text()).get("data", [])
        except Exception:
            pass
    return []


def save_temp_history(code, naam, data_points):
    now    = datetime.now(timezone.utc)
    cutoff = (now - timedelta(hours=25)).isoformat()
    pruned = sorted(
        [pt for pt in data_points if pt["t"] >= cutoff],
        key=lambda x: x["t"],
    )
    path = TEMP_HIST_DIR / f"{code_to_filename(code)}.json"
    path.write_text(json.dumps({"code": code, "naam": naam, "data": pruned}, ensure_ascii=False))
    return pruned


def append_to_temp_history(code, naam, tijdstip, temp_c):
    if tijdstip is None or temp_c is None:
        return
    existing   = load_temp_history(code)
    timestamps = {pt["t"] for pt in existing}
    if tijdstip not in timestamps:
        existing.append({"t": tijdstip, "v": temp_c})
    save_temp_history(code, naam, existing)


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


def fetch_rws(catalog):
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


# ── RWS: zeewatertemperatuur ──────────────────────────────────────────────────

# Binnenlandse en kust-stations uitsluiten (zelfde logica als Hm0)
TEMP_INLAND = [
    "ijsselmeer", "markermeer", "markerwaard", "markerwadden", "slotermeer",
    "woudsend", "waddenzee", "grevelingen", "veerse", "volkerak", "haringvliet",
    "hollands diep", "lek", "waal", "rijn", "maas", "ijssel", "zwarte meer",
    "randmeer", "veluwemeer", "eemmeer", "gooimeer", "almere", "strand",
    "zwembad", "badstrand", "recreatie", "triathlon", "bosbaan",
]

def is_temp_excluded(code, naam):
    code_l = (code or "").lower()
    naam_l = (naam or "").lower()
    return any(kw in code_l or kw in naam_l for kw in TEMP_INLAND)


def fetch_rws_temp(catalog):
    """Haal zeewatertemperatuur op van RWS-stations op de Noordzee."""
    meta      = catalog.get("AquoMetadataLijst", [])
    locs      = catalog.get("LocatieLijst", [])
    meta_locs = catalog.get("AquoMetadataLocatieLijst", [])

    # Vind T + OW metadata IDs
    temp_meta_ids = {
        m["AquoMetadata_MessageID"] for m in meta
        if m.get("Grootheid", {}).get("Code") == "T"
        and m.get("Compartiment", {}).get("Code") == "OW"
    }

    temp_loc_ids = {
        r["Locatie_MessageID"] for r in meta_locs
        if r.get("AquoMetaData_MessageID") in temp_meta_ids
    }

    # Alleen offshore stations: Noordzee-bounding box, geen binnenwateren
    stations = [
        l for l in locs
        if l.get("Locatie_MessageID") in temp_loc_ids
        and l.get("Lat") is not None and l.get("Lon") is not None
        and 2.0 < l["Lon"] < 9.5
        and 51.0 < l["Lat"] < 56.5
        and not is_temp_excluded(l.get("Code", ""), l.get("Naam", ""))
    ]

    print(f"[RWS temp] {len(stations)} offshore stations gevonden")

    # Waarnemingen ophalen in batches
    BATCH = 20
    now   = datetime.now(timezone.utc)
    features = []
    station_map = {s["Code"]: s for s in stations}

    for i in range(0, len(stations), BATCH):
        batch = stations[i:i + BATCH]
        try:
            resp = rws_post(
                "/ONLINEWAARNEMINGENSERVICES/OphalenLaatsteWaarnemingen",
                {
                    "AquoPlusWaarnemingMetadataLijst": [{
                        "AquoMetadata": {
                            "Compartiment": {"Code": "OW"},
                            "Eenheid":      {"Code": "oC"},
                            "Grootheid":    {"Code": "T"},
                        }
                    }],
                    "LocatieLijst": [{"Code": s["Code"]} for s in batch],
                },
            )
        except Exception as e:
            print(f"[RWS temp] batch {i} fout: {e}")
            continue

        # Dedupliceer per station (recentste wint)
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

            # Sla negatieve of onrealistische waarden over
            if temp_c is not None and not (-2 < temp_c < 35):
                continue

            code = f"rws.temp.{loc_code.lower()}"
            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {
                    "code": code, "naam": naam,
                    "temp_c": temp_c, "tijdstip": tijdstip,
                    "bron": "RWS",
                },
            })
            append_to_temp_history(code, naam, tijdstip, temp_c)

    print(f"[RWS temp] {len(features)} stations na filters")
    return features


# ── CEFAS WaveNet ─────────────────────────────────────────────────────────────

def fetch_cefas(data, now):
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


def fetch_cefas_temp(cefas_data, now):
    """Haal zeewatertemperatuur op uit de al-opgehaalde CEFAS Map/Current data."""
    features = []
    for f in cefas_data.get("features", []):
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
        code       = f"cefas.temp.{station_id.lower()}"

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

        temp_info = props.get("results", {}).get("TEMP", {})
        temp_vals = temp_info.get("values", [])
        try:
            temp_c = round(float(temp_vals[0]), 1) if temp_vals and temp_vals[0] else None
        except (ValueError, IndexError):
            temp_c = None

        if temp_c is None:
            continue

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "code": code, "naam": naam,
                "temp_c": temp_c, "tijdstip": tijdstip_s,
                "bron": "CEFAS",
                "cefas_id": station_id, "cefas_source": source,
            },
        })
        append_to_temp_history(code, naam, tijdstip_s, temp_c)

    print(f"[CEFAS temp] {len(features)} stations")
    return features


def fetch_cefas_history(station_id, source, code, naam):
    """Haal 24-uursgeschiedenis (Hm0 + TEMP) op van CEFAS Detail/Results API."""
    now   = datetime.now(timezone.utc)
    start = (now - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S")
    end   = now.strftime("%Y-%m-%dT%H:%M:%S")
    url   = (f"{CEFAS_API}/Detail/Results/{station_id}/{source}"
             f"?showForecast=false&dateFrom={start}&dateTo={end}")
    try:
        req = urllib.request.Request(url, headers=CEFAS_HEADERS)
        with urllib.request.urlopen(req, timeout=15) as r:
            rows = json.loads(r.read().decode("utf-8"))

        wave_data = []
        temp_data = []
        for row in rows:
            if row.get("isForecast"):
                continue
            ts      = row.get("timestamp", "")
            results = row.get("results", [])
            hm0  = next((x.get("value") for x in results if x.get("identifier") == "Hm0"),  None)
            temp = next((x.get("value") for x in results if x.get("identifier") == "TEMP"), None)
            if ts and hm0 is not None:
                try:
                    wave_data.append({"t": ts, "v": round(float(hm0), 2)})
                except ValueError:
                    pass
            if ts and temp is not None:
                try:
                    temp_data.append({"t": ts, "v": round(float(temp), 1)})
                except ValueError:
                    pass

        wave_data.sort(key=lambda x: x["t"])
        save_history(code, naam, wave_data)

        # Sla ook temperatuurgeschiedenis op als die beschikbaar is
        if temp_data:
            temp_data.sort(key=lambda x: x["t"])
            temp_code = f"cefas.temp.{station_id.lower()}"
            save_temp_history(temp_code, naam, temp_data)

    except Exception as e:
        print(f"[CEFAS history] {station_id}: {e}")


# ── Hoofdprogramma ────────────────────────────────────────────────────────────

def main():
    now = datetime.now(timezone.utc)

    # ── Catalogus één keer ophalen (gedeeld door Hm0 én temperatuur) ──
    print("[RWS] Catalogus ophalen…")
    try:
        catalog = rws_post("/METADATASERVICES/OphalenCatalogus", {"CatalogusFilter": {
            "Grootheden": True, "Eenheden": True,
            "Compartimenten": True, "ProcesTypes": True, "Groeperingen": True,
        }})
    except Exception as e:
        print(f"[RWS] Catalogus fout: {e}")
        catalog = {}

    # ── CEFAS Map/Current één keer ophalen (gedeeld door golven én temperatuur) ──
    cefas_raw = {}
    try:
        req = urllib.request.Request(f"{CEFAS_API}/Map/Current", headers=CEFAS_HEADERS)
        with urllib.request.urlopen(req, timeout=20) as r:
            cefas_raw = json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print(f"[CEFAS] Map/Current fout: {e}")

    # ── Golfhoogtes ──────────────────────────────────────────────────
    wave_features = []
    try:
        wave_features.extend(fetch_rws(catalog))
    except Exception as e:
        print(f"[RWS golf] Fout: {e}")
    try:
        wave_features.extend(fetch_bsh())
    except Exception as e:
        print(f"[BSH] Fout: {e}")
    try:
        wave_features.extend(fetch_cefas(cefas_raw, now))
    except Exception as e:
        print(f"[CEFAS golf] Fout: {e}")

    waves_geojson = {
        "type": "FeatureCollection", "features": wave_features,
        "opgehaald": now.isoformat(), "aantalStations": len(wave_features),
    }
    (DATA_DIR / "waves.json").write_text(json.dumps(waves_geojson, ensure_ascii=False))
    print(f"[KLAAR golf] {len(wave_features)} stations → data/waves.json")

    # ── Temperatuur ───────────────────────────────────────────────────
    temp_features = []
    try:
        temp_features.extend(fetch_rws_temp(catalog))
    except Exception as e:
        print(f"[RWS temp] Fout: {e}")
    try:
        temp_features.extend(fetch_cefas_temp(cefas_raw, now))
    except Exception as e:
        print(f"[CEFAS temp] Fout: {e}")

    temp_geojson = {
        "type": "FeatureCollection", "features": temp_features,
        "opgehaald": now.isoformat(), "aantalStations": len(temp_features),
    }
    (DATA_DIR / "temp.json").write_text(json.dumps(temp_geojson, ensure_ascii=False))
    print(f"[KLAAR temp] {len(temp_features)} stations → data/temp.json")


if __name__ == "__main__":
    main()
