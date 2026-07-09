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
import random as _random
import collections
import queue as _queue
import gzip as _gzip
import re
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, parse_qs, urlencode

# ── Minimale WebSocket client (alleen stdlib — geen pip nodig) ────────────────
import socket as _socket
import ssl as _ssl
import base64 as _base64
import hashlib as _hashlib
import struct as _struct

class _WSError(Exception): pass

class _WS:
    """Minimale WebSocket client over TLS, genoeg voor lightningmaps.org."""
    GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

    def __init__(self, host, port=443, path="/", origin=None, timeout=15):
        raw = _socket.create_connection((host, port), timeout=timeout)
        ctx = _ssl.create_default_context()
        self._sock = ctx.wrap_socket(raw, server_hostname=host)
        self._sock.settimeout(timeout)
        key = _base64.b64encode(bytes(_random.getrandbits(8) for _ in range(16))).decode()
        req = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            f"Upgrade: websocket\r\n"
            f"Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            f"Sec-WebSocket-Version: 13\r\n"
            + (f"Origin: {origin}\r\n" if origin else "")
            + "User-Agent: Mozilla/5.0\r\n\r\n"
        )
        self._sock.sendall(req.encode())
        resp = b""
        while b"\r\n\r\n" not in resp:
            chunk = self._sock.recv(4096)
            if not chunk:
                raise _WSError("Verbinding gesloten tijdens handshake")
            resp += chunk
        if b"101" not in resp.split(b"\r\n")[0]:
            raise _WSError(f"Handshake mislukt: {resp[:200]}")
        expected = _base64.b64encode(
            _hashlib.sha1((key + self.GUID).encode()).digest()
        ).decode()
        if expected not in resp.decode(errors="ignore"):
            raise _WSError("Sec-WebSocket-Accept ongeldig")
        self._buf = b""

    def settimeout(self, t):
        self._sock.settimeout(t)

    def recv(self):
        """Geeft de payload terug van het volgende tekst-frame."""
        while True:
            while len(self._buf) < 2:
                d = self._sock.recv(4096)
                if not d:
                    raise _WSError("Verbinding verbroken")
                self._buf += d
            b0, b1 = self._buf[0], self._buf[1]
            opcode = b0 & 0x0F
            masked = b1 & 0x80
            length = b1 & 0x7F
            hdr = 2
            if length == 126:
                while len(self._buf) < hdr + 2: self._buf += self._sock.recv(4096)
                length = _struct.unpack_from(">H", self._buf, hdr)[0]; hdr += 2
            elif length == 127:
                while len(self._buf) < hdr + 8: self._buf += self._sock.recv(4096)
                length = _struct.unpack_from(">Q", self._buf, hdr)[0]; hdr += 8
            if masked:
                while len(self._buf) < hdr + 4: self._buf += self._sock.recv(4096)
                mask = self._buf[hdr:hdr+4]; hdr += 4
            while len(self._buf) < hdr + length: self._buf += self._sock.recv(4096)
            payload = self._buf[hdr:hdr+length]
            self._buf = self._buf[hdr+length:]
            if masked:
                payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
            if opcode == 8:
                raise _WSError("Server sloot verbinding (close frame)")
            if opcode == 9:  # ping → pong
                self.send_raw(b"", opcode=10); continue
            if opcode in (1, 2):  # text of binary
                return payload.decode("utf-8", errors="replace")
            # continuation / pong / overig → negeren

    def send(self, text):
        self.send_raw(text.encode("utf-8"), opcode=1)

    def send_raw(self, payload, opcode=1):
        mask_key = bytes(_random.getrandbits(8) for _ in range(4))
        masked = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))
        ln = len(payload)
        if ln < 126:
            hdr = bytes([0x80 | opcode, 0x80 | ln])
        elif ln < 65536:
            hdr = bytes([0x80 | opcode, 0xFE]) + _struct.pack(">H", ln)
        else:
            hdr = bytes([0x80 | opcode, 0xFF]) + _struct.pack(">Q", ln)
        self._sock.sendall(hdr + mask_key + masked)

    def close(self):
        try: self._sock.close()
        except Exception: pass

_WS_OK = True  # altijd True, eigen implementatie

PORT      = int(os.environ.get("PORT", 3001))
RWS_BASE  = "https://ddapi20-waterwebservices.rijkswaterstaat.nl"
CEFAS_URL = "https://wavenet-api.cefas.co.uk/api/Map/Current"
CACHE_S      = 10 * 60       # 10 minuten cache
TEMP_CACHE_S = 24 * 60 * 60  # 24 uur cache voor zeewatertemperatuur

BUIENRADAR_URL  = "https://data.buienradar.nl/2.0/feed/json"
METAR_CACHE_S   = 30 * 60   # 30 minuten cache

_metar_cache       = None
_metar_time        = 0
_coastal_stations  = None   # dict icao → (naam, lat, lon, elev_m) voor kust-luchthavens
_EMPTY_METAR       = {"type": "FeatureCollection", "features": [], "aantalStations": 0, "opgehaald": "", "laden": True}

# ── Bliksem (Blitzortung WebSocket — server-side relay) ──────────────────────
_BLIKSEM_MAX      = 10000   # max strikes in buffer
_bliksem_deque    = collections.deque(maxlen=_BLIKSEM_MAX)
_bliksem_lock     = threading.Lock()
_BLIKSEM_MAX_AGE  = 60 * 60  # seconden — bewaar 60 min zodat reload direct het uur toont

# SSE broadcast: set van queues, één per verbonden browser client
_bliksem_clients      = set()
_bliksem_clients_lock = threading.Lock()
_bliksem_last_ts      = 0   # timestamp laatste ontvangen strike (voor diagnostiek)
_bliksem_total        = 0   # totaal ontvangen strikes (voor diagnostiek)

def _bliksem_push(lat, lon, ts_ms):
    """Sla op in buffer en push naar alle SSE-clients."""
    global _bliksem_last_ts, _bliksem_total
    ts_s = ts_ms / 1000.0
    entry = (round(ts_s, 3), lat, lon)
    with _bliksem_lock:
        _bliksem_deque.append(entry)
    _bliksem_last_ts = ts_ms
    _bliksem_total  += 1
    sse_msg = json.dumps({"ts": round(ts_s, 3), "lat": lat, "lon": lon}).encode() + b"\n"
    with _bliksem_clients_lock:
        for q in list(_bliksem_clients):
            try:
                q.put_nowait(sse_msg)
            except _queue.Full:
                pass

_LMAPS_SERVERS = ["live.lightningmaps.org", "live2.lightningmaps.org"]
_LMAPS_VERSION = 24

def _bliksem_bg():
    """Verbindt met lightningmaps.org WebSocket relay en buffert strikes."""
    server_idx = 0
    while True:
        ws = None
        server = _LMAPS_SERVERS[server_idx % len(_LMAPS_SERVERS)]
        try:
            print(f"[BLIKSEM] Verbinden met {server}")
            ws = _WS(
                host=server, port=443, path="/",
                origin="https://www.lightningmaps.org",
                timeout=15,
            )
            init_msg = json.dumps({
                "v": _LMAPS_VERSION, "i": {}, "s": False,
                "x": 0, "w": 0, "tx": 0, "tw": 1,
                "a": 4, "z": 3, "b": True, "h": "",
                "l": 0, "t": 0, "from_lightningmaps_org": True,
                "p": [90, 180, -90, -180],
            })
            ws.send(init_msg)
            print("[BLIKSEM] Verbonden, wacht op strokes…")
            ws.settimeout(30)
            while True:
                try:
                    raw = ws.recv()
                    if not raw:
                        continue
                    d = json.loads(raw)
                    if "k" in d:
                        k_resp = json.dumps({"k": (d["k"] * 3604) % 7081 * int(time.time() * 1000) / 100})
                        ws.send(k_resp)
                    strokes = d.get("strokes") or []
                    for s in strokes:
                        lat = s.get("lat")
                        lon = s.get("lon")
                        ts  = s.get("time")  # milliseconden
                        if lat is not None and lon is not None and ts:
                            _bliksem_push(float(lat), float(lon), int(ts))
                except TimeoutError:
                    try: ws.send(json.dumps({"v": _LMAPS_VERSION, "t": 0}))
                    except Exception: break
                except Exception:
                    break
        except Exception as e:
            print(f"[BLIKSEM] Verbindingsfout: {e}")
        finally:
            try:
                if ws: ws.close()
            except Exception:
                pass
        server_idx += 1
        time.sleep(5)

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


# ── Meetnet Vlaamse Banken (MVB): Belgische kust- en offshore boeien ─────────
#
# Bron:  Agentschap Maritieme Dienstverlening en Kust, Afdeling Kust (Vlaanderen)
# API:   https://api.meetnetvlaamsebanken.be/V2/
# Auth:  OAuth2 password grant → env vars MVB_USERNAME + MVB_PASSWORD
#
# Beschikbare parameters (codes uit catalog):
#   HM0  — significante golfhoogte (cm)  → omzetten naar m voor golftab
#   WVS  — windsnelheid scalair (m/s)    → windtab
#   WRS  — windrichting scalair (°)       → windtab
#   WATTMP / TW — zeewatertemperatuur (°C) → temperatuurtab (code verschilt per station)
#
# De catalog wordt eenmalig gefetcht en 24u gecached. currentData geeft de
# meest recente meetwaarden voor alle dataset-ID's.

_MVB_API    = "https://api.meetnetvlaamsebanken.be"
_MVB_USER   = os.environ.get("MVB_USERNAME", "")
_MVB_PASS   = os.environ.get("MVB_PASSWORD", "")

_mvb_token      = None
_mvb_token_exp  = 0.0
_mvb_token_lock = threading.Lock()

_mvb_catalog      = None
_mvb_catalog_time = 0.0

_mvb_wind_bg = []   # windfeatures gevuld door fetch_mvb_data() → gebruikt door get_wind_data()
_mvb_temp_bg = []   # tempfeatures gevuld door fetch_mvb_data() → gebruikt door _refresh_temp_bg()


def _mvb_get_token():
    """Geeft een geldig Bearer-token terug; hernieuwt automatisch bij verlopen."""
    global _mvb_token, _mvb_token_exp
    with _mvb_token_lock:
        if _mvb_token and time.time() < _mvb_token_exp - 60:
            return _mvb_token
        if not _MVB_USER or not _MVB_PASS:
            return None
        body = urlencode({"grant_type": "password",
                          "username":   _MVB_USER,
                          "password":   _MVB_PASS}).encode()
        req = urllib.request.Request(
            f"{_MVB_API}/Token", data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            resp = json.loads(r.read())
        _mvb_token     = resp["access_token"]
        _mvb_token_exp = time.time() + int(resp.get("expires_in", 3600))
        print(f"[MVB] Nieuw token verkregen (geldig {resp.get('expires_in', 3600)}s)")
        return _mvb_token


def _mvb_get(path):
    """GET-request naar MVB API met Bearer-token."""
    token = _mvb_get_token()
    if not token:
        return None
    req = urllib.request.Request(
        f"{_MVB_API}{path}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read())


def _mvb_get_catalog():
    """Haal MVB catalog op (gecached 24u): locaties + parameters + datasets."""
    global _mvb_catalog, _mvb_catalog_time
    if _mvb_catalog and (time.time() - _mvb_catalog_time) < 86400:
        return _mvb_catalog
    data = _mvb_get("/V2/catalog")
    if data:
        _mvb_catalog      = data
        _mvb_catalog_time = time.time()
        nloc = len(data.get("Locations") or [])
        nds  = len(data.get("DataSets")  or data.get("Datasets") or [])
        print(f"[MVB] Catalog: {nloc} locaties, {nds} datasets")
    return _mvb_catalog


def fetch_mvb_data():
    """Haal actuele MVB-data op: golf (HM0), wind (WVS/WRS), temperatuur.
    Geeft golffeatures terug voor _do_refresh; slaat wind+temp op in globals."""
    global _mvb_wind_bg, _mvb_temp_bg

    if not _MVB_USER or not _MVB_PASS:
        return []

    try:
        catalog = _mvb_get_catalog()
    except Exception as e:
        print(f"[MVB] Catalog fout: {e}")
        return []
    if not catalog:
        return []

    # Locatie lookup: ID → {naam, lat, lon}
    loc_map = {}
    for loc in (catalog.get("Locations") or []):
        lid  = loc.get("ID") or loc.get("Id") or loc.get("id")
        lat  = loc.get("Latitude")  or loc.get("latitude")
        lon  = loc.get("Longitude") or loc.get("longitude")
        naam = loc.get("Name") or loc.get("name") or str(lid)
        if lid is not None and lat is not None and lon is not None:
            loc_map[lid] = {"naam": naam, "lat": float(lat), "lon": float(lon)}

    # Dataset lookup: dataset-ID → {loc_id, par_id}
    ds_map = {}
    for ds in (catalog.get("DataSets") or catalog.get("Datasets") or []):
        did    = ds.get("ID")          or ds.get("Id")          or ds.get("id")
        loc_id = ds.get("LocationID")  or ds.get("LocationId")  or ds.get("Location")
        par_id = ds.get("ParameterID") or ds.get("ParameterId") or ds.get("Parameter")
        if did is not None:
            ds_map[did] = {"loc_id": loc_id, "par_id": str(par_id or "").upper()}

    try:
        current = _mvb_get("/V2/currentData")
    except Exception as e:
        print(f"[MVB] currentData fout: {e}")
        return []
    if not current:
        return []

    values = current if isinstance(current, list) else (current.get("Values") or [])

    now          = datetime.now(timezone.utc)
    wave_features = []
    wind_by_loc   = {}
    temp_by_loc   = {}

    # Temperatuur-parametersynoniemen (exact code hangt af van catalogversie)
    TEMP_PARAMS = {"WATTMP", "TW", "WT", "WATERTEMP", "TEMP", "TWAT", "TWATER"}

    for val in values:
        did       = val.get("ID")        or val.get("Id")        or val.get("id")
        raw_value = val.get("Value")     or val.get("value")
        ts_str    = (val.get("Timestamp") or val.get("DateTime")
                     or val.get("Time")   or val.get("timestamp") or "")

        if raw_value is None:
            continue

        ds     = ds_map.get(did, {})
        loc_id = ds.get("loc_id")
        par_id = ds.get("par_id", "")
        loc    = loc_map.get(loc_id)
        if not loc:
            continue
        lat, lon, naam = loc["lat"], loc["lon"], loc["naam"]

        # Tijdstip parsen + staleness filter (48u)
        tijdstip = None
        if ts_str:
            try:
                dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                if (now - dt).total_seconds() > 48 * 3600:
                    continue
                tijdstip = dt.isoformat()
            except Exception:
                pass

        code_base = f"mvb.{naam.lower().replace(' ', '_').replace('-', '_')}"

        if par_id == "HM0":
            try:
                hm0 = round(float(raw_value) / 100.0, 2)   # cm → m
                if not (0 <= hm0 <= 25):
                    continue
                wave_features.append({
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [lon, lat]},
                    "properties": {
                        "code":      f"{code_base}.hm0",
                        "naam":      naam,
                        "hm0_m":    hm0,
                        "tijdstip":  tijdstip,
                        "status":    None,
                        "kwaliteit": None,
                        "bron":      "MVB",
                    },
                })
            except (ValueError, TypeError):
                pass

        elif par_id == "WVS":
            try:
                spd = round(float(raw_value), 1)
                if not (0 <= spd <= 60):
                    continue
                wd = wind_by_loc.setdefault(loc_id, {"naam": naam, "lat": lat, "lon": lon})
                wd["wind_ms"]  = spd
                wd.setdefault("tijdstip", tijdstip)
            except (ValueError, TypeError):
                pass

        elif par_id == "WRS":
            try:
                wd = wind_by_loc.setdefault(loc_id, {"naam": naam, "lat": lat, "lon": lon})
                wd["wind_dir"] = round(float(raw_value))
            except (ValueError, TypeError):
                pass

        elif par_id in TEMP_PARAMS:
            try:
                tc = round(float(raw_value), 1)
                if not (-2 <= tc <= 40):
                    continue
                temp_by_loc[loc_id] = {
                    "naam": naam, "lat": lat, "lon": lon,
                    "temp_c": tc, "tijdstip": tijdstip,
                }
            except (ValueError, TypeError):
                pass

    # Wind features
    wind_features = []
    for loc_id, w in wind_by_loc.items():
        if "wind_ms" not in w:
            continue
        cb = f"mvb.{w['naam'].lower().replace(' ', '_').replace('-', '_')}"
        wind_features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [w["lon"], w["lat"]]},
            "properties": {
                "code":     f"{cb}.wind",
                "rws_code": None,
                "naam":     w["naam"],
                "wind_ms":  w.get("wind_ms"),
                "wind_dir": w.get("wind_dir"),
                "tijdstip": w.get("tijdstip"),
                "bron":     "MVB",
            },
        })
    _mvb_wind_bg = wind_features

    # Temperatuur features
    temp_features = []
    for loc_id, t in temp_by_loc.items():
        cb = f"mvb.{t['naam'].lower().replace(' ', '_').replace('-', '_')}"
        temp_features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [t["lon"], t["lat"]]},
            "properties": {
                "code":     f"{cb}.temp",
                "naam":     t["naam"],
                "temp_c":   t["temp_c"],
                "tijdstip": t["tijdstip"],
                "bron":     "MVB",
            },
        })
    _mvb_temp_bg = temp_features

    print(f"[MVB] {len(wave_features)} golf / {len(wind_features)} wind / {len(temp_features)} temp")
    return wave_features


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

# ── Open-Meteo oceaangrid: modelzicht op vaste zeepunten wereldwijd ───────────
# Één API-call per batch van max 100 locaties (comma-separated lat/lon).
# Eenheid: meters → km. Bron: ERA5/ECMWF-model, elke 15 min bijgewerkt.
OCEAN_GRID = [
    # Europese zeeën
    ("Noordzee Zuid",      52.0,   3.5), ("Noordzee Midden",   56.0,   4.0),
    ("Noordzee Noord",     58.5,   2.0), ("Engelse Kanaal",    50.0,  -1.5),
    ("Ierse Zee",          53.5,  -5.0), ("Keltische Zee",     50.5,  -8.0),
    ("Noorse Zee",         67.0,   1.0), ("Barentszzee West",  73.0,  20.0),
    ("Barentszzee Oost",   73.0,  42.0), ("Witte Zee",         66.0,  33.0),
    ("Baltische Zee",      57.0,  18.0), ("Finse Golf",        60.0,  26.0),
    ("Botnische Golf",     63.5,  21.0), ("Skagerrak",         57.8,   9.0),
    ("Biskaje",            45.5,  -5.5), ("Golf van Biscaje",  47.0,  -8.0),
    # Middellandse en aangrenzende zeeën
    ("Liguriëzee",         43.5,   8.5), ("Adriatische Zee",   43.0,  14.5),
    ("Ionische Zee",       38.0,  19.0), ("Egeïsche Zee",      38.5,  24.5),
    ("West-Med",           38.0,   5.0), ("Oost-Med",          34.5,  30.0),
    ("Tyrrheense Zee",     40.5,  12.5), ("Zwarte Zee",        43.0,  32.0),
    ("Golf van Tunis",     37.0,  11.0), ("Alboran",           35.8,  -3.5),
    # Atlantisch
    ("Noord-Atlantisch",   50.0, -25.0), ("Mid-Atlantisch",    40.0, -35.0),
    ("Azoren-gebied",      38.5, -28.0), ("Tropisch N-Atl.",   20.0, -30.0),
    ("Golf van Mexico",    24.0, -90.0), ("Caraïben Oost",     15.0, -63.0),
    ("Caraïben West",      17.0, -83.0), ("Bermuda-gebied",    32.0, -65.0),
    ("Labrador Zee",       56.0, -53.0), ("Groenland Zee",     70.0, -12.0),
    ("IJszee",             78.0,  -5.0), ("Equatoriaal Atl.",   2.0, -15.0),
    ("Zuid-Atl. Noord",   -15.0, -15.0), ("Zuid-Atl. Midden",  -30.0, -20.0),
    ("Falkland Zee",      -50.0, -60.0), ("Scotia Zee",        -56.0, -45.0),
    # Indische Oceaan
    ("Arabische Zee",      15.0,  65.0), ("Golf van Aden",     12.0,  48.0),
    ("Indische Oceaan N",   8.0,  75.0), ("Indische Oceaan Mid",-10.0,  75.0),
    ("Indische Oceaan Z",  -30.0,  80.0), ("Bengaalse Golf",    12.0,  88.0),
    ("Mozambiquekanaal",  -18.0,  42.0), ("Madagaskar",        -25.0,  53.0),
    # Stille Oceaan
    ("N-Pacific West",     38.0, 155.0), ("N-Pacific Midden",  40.0,-165.0),
    ("N-Pacific Oost",     40.0,-135.0), ("Japan Zee",         40.0, 135.0),
    ("Filipijnenzee",      18.0, 130.0), ("Tropisch Pacific",  10.0, 165.0),
    ("Equatoriaal Pac.",    0.0,-140.0), ("Z-Pacific West",   -20.0, 170.0),
    ("Z-Pacific Midden",  -30.0,-120.0), ("Z-Pacific Oost",   -40.0, -90.0),
    ("Tasmanische Zee",   -38.0, 160.0), ("Koraalzee",        -18.0, 155.0),
    ("Timorzee",          -11.0, 127.0), ("Arafurazee",       -10.0, 136.0),
    # Zuidelijke Oceaan
    ("Z-Oceaan Atl.",     -55.0,  -5.0), ("Z-Oceaan Indisch",  -55.0,  85.0),
    ("Z-Oceaan Pacific",  -55.0,-140.0), ("Drake Passage",     -58.0, -65.0),
    # Extra Arctisch / Noord-Pacific
    ("Beringzee",          58.0,-175.0), ("Ochotskzee",        52.0, 147.0),
    ("Golf van Alaska",    55.0,-150.0), ("Hudson Bay",        60.0, -85.0),
]

_ocean_vis_cache = None
_ocean_vis_time  = 0

def fetch_ocean_visibility():
    """Haalt modelzicht op voor ~70 vaste oceaanpunten via Open-Meteo (1 verzoek)."""
    global _ocean_vis_cache, _ocean_vis_time
    now = time.time()
    if _ocean_vis_cache is not None and (now - _ocean_vis_time) < 1800:
        return _ocean_vis_cache

    lats = ",".join(str(lat) for _, lat, _ in OCEAN_GRID)
    lons = ",".join(str(lon) for _, _, lon in OCEAN_GRID)
    url  = (f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={lats}&longitude={lons}"
            f"&current=visibility&timezone=UTC&forecast_days=1")
    req = urllib.request.Request(url, headers={"User-Agent": "RWS-Golfhoogte-Proxy/1.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        resp = json.loads(r.read().decode())

    # resp is een lijst als meerdere locaties, anders een dict
    if isinstance(resp, dict):
        resp = [resp]

    features = []
    for i, station_resp in enumerate(resp):
        naam, lat, lon = OCEAN_GRID[i]
        vis_m = (station_resp.get("current") or {}).get("visibility")
        if vis_m is None:
            continue
        vis_km = round(vis_m / 1000, 1)
        ts = (station_resp.get("current") or {}).get("time", "")
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "code":        f"openmeteo.vis.{i}",
                "naam":        naam,
                "stationname": naam,
                "vv":          vis_km,
                "tijdstip":    ts,
                "bron":        "Open-Meteo (model)",
            },
        })

    print(f"[Ocean VIS] {len(features)} oceaanpunten geladen")
    _ocean_vis_cache = features
    _ocean_vis_time  = now
    return features

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
_ndbc_vis_features  = []   # gevuld door fetch_ndbc_data(), gebruikt door /api/visibility
_ocean_vis_features = []   # gevuld door _refresh_ocean_vis_bg(), gebruikt door /api/visibility

def fetch_ndbc_data():
    req = urllib.request.Request(
        NDBC_URL,
        headers={"User-Agent": "Mozilla/5.0 (compatible; ZeedataProxy/1.0)"},
    )
    with urllib.request.urlopen(req, timeout=20) as r:
        lines = r.read().decode("utf-8", errors="replace").splitlines()

    global _ndbc_wind_features, _ndbc_vis_features
    now           = datetime.now(timezone.utc)
    wave_features = []
    wind_features = []
    vis_features  = []

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

        # Zicht (VIS, kolom 20, in zeemijlen → km)
        try:
            vis_km = None if len(cols) <= 20 or cols[20] == "MM" \
                     else round(float(cols[20]) * 1.852, 1)
        except (ValueError, IndexError):
            vis_km = None

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

        if vis_km is not None:
            vis_features.append({
                "type": "Feature",
                "geometry": geom,
                "properties": {
                    "code":        f"ndbc.vis.{station_id.lower()}",
                    "naam":        naam,
                    "stationname": naam,
                    "vv":          vis_km,
                    "tijdstip":    tijdstip,
                    "bron":        "NDBC/NOAA",
                },
            })

    _ndbc_wind_features = wind_features
    _ndbc_vis_features  = vis_features
    print(f"[NDBC] {len(wave_features)} golfstations, {len(wind_features)} windstations, {len(vis_features)} zichtstations geladen")
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
_temp_cache     = None   # legacy — wordt niet meer gebruikt voor /api/temp
_temp_time      = 0
_temp_bg        = None   # achtergrond-cache voor /api/temp (24u TTL)
_temp_bg_time   = 0
_rws_temp_hist  = {}   # code → {tijdstip_iso: temp_c}  (ring buffer, net als BSH)
_knmi_temp_hist = {}   # code → {tijdstip_iso: temp_c}  (ring buffer voor KNMI luchttemp)


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


def _record_knmi_temp(code, tijdstip, temp_c):
    """Sla KNMI luchttemperatuur op in ring buffer (max 25 uur)."""
    if tijdstip is None or temp_c is None:
        return
    if code not in _knmi_temp_hist:
        _knmi_temp_hist[code] = {}
    _knmi_temp_hist[code][tijdstip] = temp_c
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=25)).isoformat()
    _knmi_temp_hist[code] = {
        ts: v for ts, v in _knmi_temp_hist[code].items() if ts >= cutoff
    }


def get_knmi_temp_history(code):
    """Geef 24-uursgeschiedenis terug: ring buffer + bestanden als fallback."""
    buf    = _knmi_temp_hist.get(code, {})
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
    data   = {ts: v for ts, v in buf.items() if ts[:19] + "Z" >= cutoff and v is not None}

    # Lees opgeslagen bestand via GitHub CDN (gevuld door GitHub Actions elke 10 min)
    fname = code.replace(".", "-").replace("/", "-") + ".json"
    try:
        url = f"{GITHUB_RAW}/data/temp-history/{fname}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            saved = json.loads(r.read()).get("data", [])
        for pt in saved:
            ts = pt["t"]
            if ts[:19] + "Z" >= cutoff and ts not in data:
                data[ts] = pt["v"]
    except Exception:
        pass

    result = sorted([{"t": ts, "v": v} for ts, v in data.items()], key=lambda x: x["t"])
    return {"code": code, "naam": code, "data": result}



CDIP_THREDDS  = "https://thredds.cdip.ucsd.edu/thredds"
CDIP_CATALOG  = CDIP_THREDDS + "/catalog/cdip/realtime/catalog.xml"
CDIP_ODAP     = CDIP_THREDDS + "/dodsC/cdip/realtime"
_cdip_cache   = None
_cdip_time    = 0
_cdip_bg      = []   # achtergrond-cache (nooit blokkerend voor _do_refresh)


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
_socib_wave_bg    = []   # achtergrond-cache: golven (gevuld buiten _do_refresh)
_socib_wind_bg    = []   # achtergrond-cache: wind


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
    """Ververs de gecachede bestandspaden voor alle SOCIB stations (max 1× per 24 uur).
    Alle catalog-lookups parallel om blokkering te voorkomen."""
    global _socib_wave_paths, _socib_wind_paths, _socib_paths_time
    if _socib_wave_paths and (time.time() - _socib_paths_time) < 86400:
        return
    all_dirs = (
        [("waves_recorder", d) for d in SOCIB_WAVE_DIRS] +
        [("weather_station", d) for d in SOCIB_WIND_DIRS]
    )
    with ThreadPoolExecutor(max_workers=min(len(all_dirs), 3)) as ex:
        futs = {(cat, d): ex.submit(_get_socib_latest_path, cat, d) for cat, d in all_dirs}
    for (cat, d), fut in futs.items():
        try:
            p = fut.result(timeout=15)
            if p:
                if cat == "waves_recorder":
                    _socib_wave_paths[d] = p
                else:
                    _socib_wind_paths[d] = p
        except Exception:
            pass
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
    with ThreadPoolExecutor(max_workers=3) as ex:
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
    with ThreadPoolExecutor(max_workers=3) as ex:
        results = list(ex.map(_socib_fetch_station_wind, SOCIB_WIND_DIRS))
    features = [r for r in results if r is not None]
    print(f"[SOCIB wind] {len(features)} stations geladen")
    return features


def _refresh_cdip_bg():
    """Vul _cdip_bg vanuit CDIP THREDDS (kan 50s duren, blokkeert _do_refresh NIET)."""
    global _cdip_bg
    try:
        data = fetch_cdip_data()
        _cdip_bg = data
        print(f"[CDIP bg] {len(data)} stations geladen")
    except Exception as e:
        print(f"[CDIP bg] Fout: {e}")


def _refresh_socib_bg():
    """Vul _socib_wave_bg en _socib_wind_bg vanuit SOCIB THREDDS.
    Roept _ensure_socib_paths() aan (kan traag zijn) en slaat resultaat op
    in de achtergrond-caches. Blokkeert _do_refresh() NIET."""
    global _socib_wave_bg, _socib_wind_bg
    try:
        waves = fetch_socib_data()
        _socib_wave_bg = waves
        print(f"[SOCIB bg] {len(waves)} golfstations geladen")
    except Exception as e:
        print(f"[SOCIB bg wave] Fout: {e}")
    try:
        wind = fetch_socib_wind_data()
        _socib_wind_bg = wind
        print(f"[SOCIB bg] {len(wind)} windstations geladen")
    except Exception as e:
        print(f"[SOCIB bg wind] Fout: {e}")


def _refresh_ocean_vis_bg():
    """Vul _ocean_vis_features vanuit Open-Meteo (modelzicht voor oceaanpunten)."""
    global _ocean_vis_features
    try:
        features = fetch_ocean_visibility()
        _ocean_vis_features = features
        print(f"[Ocean VIS bg] {len(features)} oceaanpunten geladen")
    except Exception as e:
        print(f"[Ocean VIS bg] Fout: {e}")


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

    from concurrent.futures import wait as _wait
    with ThreadPoolExecutor(max_workers=2) as ex:
        futs = [ex.submit(fetch_station, sf) for sf in station_files]
        _wait(futs, timeout=50)       # max 50s totaal; daarna pending gecanceld via with
    results = []
    for f in futs:
        if f.done():
            try:
                results.append(f.result())
            except Exception:
                pass

    features = [r for r in results if r is not None]
    print(f"[CDIP] {len(features)} stations met verse data (van {len(station_files)} realtime stations)")
    _cdip_cache = features
    _cdip_time  = now
    return features


_refresh_lock = threading.Lock()

def _do_refresh():
    """Haal snelle bronnen parallel op (max ~35s) en sla op in cache.
    CDIP en SOCIB komen uit de achtergrond-cache zodat _do_refresh nooit blokkeert."""
    global _cache, _cache_time, _stations

    if _stations is None:
        try:
            _stations = fetch_hm0_stations()
        except Exception as e:
            print(f"[RWS] Catalogus mislukt: {e}")
            _stations = []

    from concurrent.futures import wait as _wait
    with ThreadPoolExecutor(max_workers=3) as ex:
        fut_rws     = ex.submit(fetch_latest_values, _stations)
        fut_bsh     = ex.submit(fetch_bsh_data)
        fut_cefas   = ex.submit(fetch_cefas_data)
        fut_labouee = ex.submit(fetch_labouee_data)
        fut_ndbc    = ex.submit(fetch_ndbc_data)
        fut_fmi     = ex.submit(fetch_fmi_data)
        fut_mvb     = ex.submit(fetch_mvb_data)
        # Wacht max 35s op alle snelle bronnen; daarna doorgaan met wat klaar is
        _wait([fut_rws, fut_bsh, fut_cefas, fut_labouee, fut_ndbc, fut_fmi, fut_mvb], timeout=35)
    # with-blok: resterende taken gecanceld, lopende threads netjes afgewacht

    try:
        waarnemingen = fut_rws.result() if fut_rws.done() else []
    except Exception as e:
        print(f"[RWS] Fout: {e}")
        waarnemingen = []
    rws_geojson = build_geojson(_stations, waarnemingen)

    for fut, label in [(fut_bsh, "BSH"), (fut_cefas, "CEFAS"),
                       (fut_labouee, "LaBouée"), (fut_ndbc, "NDBC"),
                       (fut_fmi, "FMI"), (fut_mvb, "MVB")]:
        if fut.done():
            try:
                rws_geojson["features"].extend(fut.result())
            except Exception as e:
                print(f"[{label}] Fout: {e}")
        else:
            print(f"[{label}] Timeout — overgeslagen")

    # CDIP en SOCIB uit achtergrond-cache (nooit blokkerend)
    rws_geojson["features"].extend(_cdip_bg)
    rws_geojson["features"].extend(_socib_wave_bg)

    rws_geojson["aantalStations"] = len(rws_geojson["features"])
    _cache      = rws_geojson
    _cache_time = time.time()
    print(f"[TOTAAL] {_cache['aantalStations']} stations (RWS+BSH+CEFAS+LaBouée+NDBC+FMI+MVB+CDIP+SOCIB)")


_EMPTY_WAVES = {
    "type": "FeatureCollection",
    "features": [],
    "aantalStations": 0,
    "opgehaald": "",
    "laden": True,
}

def get_data():
    global _cache, _cache_time

    if _cache and (time.time() - _cache_time) < CACHE_S:
        return _cache

    # Probeer lock te krijgen zonder te blokkeren (max 2 seconden).
    # Als de achtergrond-thread al aan het verversen is, geef meteen terug
    # zodat Render de verbinding niet verbreekt.
    acquired = _refresh_lock.acquire(timeout=2)
    if not acquired:
        # Refresh loopt al — geef verouderde cache of lege GeoJSON terug
        return _cache if _cache else _EMPTY_WAVES

    try:
        if _cache and (time.time() - _cache_time) < CACHE_S:
            return _cache
        _do_refresh()
    finally:
        _refresh_lock.release()

    return _cache if _cache else _EMPTY_WAVES


def get_temp_data():
    """Geeft de achtergrond-cache terug. Blokkeert NIET — laadt via _refresh_temp_bg()."""
    return _temp_bg or {"type": "FeatureCollection", "features": [],
                        "aantalStations": 0, "opgehaald": "", "laden": True}


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

    with ThreadPoolExecutor(max_workers=4) as ex:
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


def _thin_by_grid(features, deg=4.0):
    """Houd maximaal 1 station per grid-cel van deg × deg graden.
    Prioriteit: stations met een geldige temp_c waarde, daarna meest recent."""
    seen = {}
    for f in features:
        coords = f.get("geometry", {}).get("coordinates", [])
        if len(coords) < 2:
            continue
        lon, lat = coords[0], coords[1]
        cell = (int(lat // deg), int(lon // deg))
        p = f.get("properties", {})
        temp_c = p.get("temp_c")
        tijdstip = p.get("tijdstip") or ""
        if cell not in seen:
            seen[cell] = f
        else:
            prev = seen[cell]
            prev_p = prev.get("properties", {})
            prev_temp = prev_p.get("temp_c")
            # Voorkeur: geldig temp boven None; anders: meest recent
            if prev_temp is None and temp_c is not None:
                seen[cell] = f
            elif prev_temp is not None and temp_c is not None:
                if (p.get("tijdstip") or "") > (prev_p.get("tijdstip") or ""):
                    seen[cell] = f
    return list(seen.values())


_TEMP_INLAND = [
    "ijsselmeer", "markermeer", "markerwaard", "markerwadden", "slotermeer",
    "woudsend", "waddenzee", "grevelingen", "veerse", "volkerak", "haringvliet",
    "hollands diep", "lek", "waal", "rijn", "maas", "ijssel", "zwarte meer",
    "randmeer", "veluwemeer", "eemmeer", "gooimeer", "almere", "strand",
    "zwembad", "badstrand", "recreatie", "triathlon", "bosbaan",
]

def _fetch_rws_temp():
    """RWS zeewatertemperatuur (Noordzee-stations). Eén catalogusverzoek + parallelle batches."""
    now      = datetime.now(timezone.utc)
    features = []
    catalog  = rws_post("/METADATASERVICES/OphalenCatalogus", {"CatalogusFilter": {
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
                            for kw in _TEMP_INLAND)]

    BATCH = 20
    station_map  = {s["Code"]: s for s in stations}
    batches      = [stations[i:i + BATCH] for i in range(0, len(stations), BATCH)]

    def fetch_batch(batch):
        return rws_post("/ONLINEWAARNEMINGENSERVICES/OphalenLaatsteWaarnemingen", {
            "AquoPlusWaarnemingMetadataLijst": [{"AquoMetadata": {
                "Compartiment": {"Code": "OW"},
                "Eenheid":      {"Code": "oC"},
                "Grootheid":    {"Code": "T"},
            }}],
            "LocatieLijst": [{"Code": s["Code"]} for s in batch],
        })

    from concurrent.futures import wait as _wt
    ex_rws  = ThreadPoolExecutor(max_workers=min(len(batches), 3) or 1)
    futs    = [ex_rws.submit(fetch_batch, b) for b in batches]
    _wt(futs, timeout=20)
    ex_rws.shutdown(wait=False)

    for fut in futs:
        if not fut.done():
            continue
        try:
            resp = fut.result()
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
    return features


def _fetch_cefas_temp_list():
    """CEFAS zeewatertemperatuur voor NW-Europese stations."""
    now      = datetime.now(timezone.utc)
    features = []
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
    return features


def fetch_ocean_sst():
    """Zeewatertemperatuur (SST) voor ~70 oceaanpunten via Open-Meteo Marine API (1 verzoek)."""
    lats = ",".join(str(lat) for _, lat, _ in OCEAN_GRID)
    lons = ",".join(str(lon) for _, _, lon in OCEAN_GRID)
    url  = (f"https://marine-api.open-meteo.com/v1/marine"
            f"?latitude={lats}&longitude={lons}"
            f"&current=sea_surface_temperature&timezone=UTC")
    req  = urllib.request.Request(url, headers={"User-Agent": "RWS-Golfhoogte-Proxy/1.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        resp = json.loads(r.read().decode())
    if isinstance(resp, dict):
        resp = [resp]
    features = []
    ts_now = datetime.now(timezone.utc).isoformat()
    for i, station_resp in enumerate(resp):
        naam, lat, lon = OCEAN_GRID[i]
        cur  = station_resp.get("current") or {}
        sst  = cur.get("sea_surface_temperature")
        if sst is None:
            continue
        sst = round(float(sst), 1)
        if not (-2 < sst < 35):
            continue
        ts = cur.get("time", ts_now)
        features.append({"type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {"code": f"openmeteo.sst.{i}", "naam": naam,
                           "temp_c": sst, "tijdstip": ts, "bron": "Open-Meteo Marine"}})
    return features


def _refresh_temp_bg():
    """Vul _temp_bg parallel vanuit alle bronnen (24u TTL)."""
    global _temp_bg, _temp_bg_time
    if _temp_bg is not None and (time.time() - _temp_bg_time) < TEMP_CACHE_S:
        return  # Nog vers

    now = datetime.now(timezone.utc)

    from concurrent.futures import wait as _wait_t
    with ThreadPoolExecutor(max_workers=3) as ex:
        fut_rws   = ex.submit(_fetch_rws_temp)
        fut_cefas = ex.submit(_fetch_cefas_temp_list)
        fut_ndbc  = ex.submit(_fetch_ndbc_temp)
        fut_imi   = ex.submit(_fetch_imi_temp)
        fut_lb    = ex.submit(_fetch_labouee_temp)
        fut_sst   = ex.submit(fetch_ocean_sst)
        _wait_t([fut_rws, fut_cefas, fut_ndbc, fut_imi, fut_lb, fut_sst], timeout=45)
    # with-blok: resterende taken gecanceld, lopende threads netjes afgewacht

    features = []
    for label, fut, thin in [
        ("RWS temp",          fut_rws,   False),
        ("CEFAS temp",        fut_cefas, False),
        ("NDBC temp",         fut_ndbc,  True),
        ("IMI temp",          fut_imi,   False),
        ("LaBouée temp",      fut_lb,    False),
        ("Ocean SST",         fut_sst,   False),
    ]:
        if not fut.done():
            print(f"[{label}] Timeout — overgeslagen")
            continue
        try:
            res = fut.result()
            if thin:
                res = _thin_by_grid(res, deg=4.0)
            features.extend(res)
            print(f"[{label}] {len(res)} stations")
        except Exception as e:
            print(f"[{label}] Fout: {e}")

    # MVB zeewatertemperatuur (gevuld door fetch_mvb_data in _do_refresh)
    features.extend(_mvb_temp_bg)
    if _mvb_temp_bg:
        print(f"[MVB temp] {len(_mvb_temp_bg)} stations")

    _temp_bg = {"type": "FeatureCollection", "features": features,
                "opgehaald": now.isoformat(), "aantalStations": len(features)}
    _temp_bg_time = time.time()
    print(f"[TEMP bg TOTAAL] {len(features)} stations")


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
        with ThreadPoolExecutor(max_workers=min(len(batches), 3)) as ex:
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

    # SOCIB windstations uit achtergrond-cache (nooit blokkerend)
    features.extend(_socib_wind_bg)
    print(f"[WIND] +{len(_socib_wind_bg)} SOCIB windstations → totaal {len(features)}")

    # MVB windstations (gevuld door fetch_mvb_data in _do_refresh)
    features.extend(_mvb_wind_bg)
    print(f"[WIND] +{len(_mvb_wind_bg)} MVB windstations → totaal {len(features)}")

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

# ── Nederland provinciegrenzen – land only, sluit water uit (PDOK) ────────────

_nl_land_cache = None
_nl_land_time  = 0

def get_nl_land():
    """Haalt provinciegrenzen op: land-only polygonen, geen grote waterlichamen."""
    global _nl_land_cache, _nl_land_time
    if _nl_land_cache and (time.time() - _nl_land_time) < 86400:
        return _nl_land_cache
    url = (
        "https://service.pdok.nl/kadaster/bestuurlijkegebieden/wfs/v1_0"
        "?service=WFS&version=2.0.0&request=GetFeature"
        "&typeName=bestuurlijkegebieden:Provincie"
        "&srsName=EPSG:4326&outputFormat=application/json"
    )
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read().decode("utf-8"))
        print(f"[NL-land] {len(data.get('features', []))} provincies geladen")
        _nl_land_cache = data
        _nl_land_time  = time.time()
        return data
    except Exception as e:
        print(f"[NL-land] Fout: {e}")
        return None

# ── Nederland actuele waarnemingen (Buienradar/KNMI) ─────────────────────────

_knmi_cache        = None
_knmi_time         = 0
_knmi_vis_features = []   # alle KNMI-stations met vv (geen temp-eis, incl. offshore)
_vis_cache_ready   = False

def _load_coastal_stations():
    """Bouw eenmalig een dict van kust-ICAO codes vanuit OurAirports CSV (elev ≤ 10m / 33ft)."""
    global _coastal_stations
    if _coastal_stations is not None:
        return _coastal_stations
    import csv as _csv, io as _io
    url = "https://davidmegginson.github.io/ourairports-data/airports.csv"
    req = urllib.request.Request(url, headers={"User-Agent": "RWS-Golfhoogte-Proxy/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        text = r.read().decode("utf-8")
    reader = _csv.DictReader(_io.StringIO(text))
    result = {}
    for row in reader:
        icao = (row.get("icao_code") or row.get("gps_code") or "").strip()
        if len(icao) != 4:
            continue
        try:
            elev_ft = float(row["elevation_ft"]) if row.get("elevation_ft") else None
        except ValueError:
            continue
        if elev_ft is None or elev_ft > 33:   # 33 ft ≈ 10 m
            continue
        try:
            lat = float(row["latitude_deg"])
            lon = float(row["longitude_deg"])
        except (ValueError, KeyError):
            continue
        naam = row.get("name", icao)
        result[icao] = (naam, lat, lon, round(elev_ft * 0.3048, 1))
    _coastal_stations = result
    print(f"[METAR] {len(result)} kust-ICAO codes geladen (OurAirports)")
    return result


def _parse_metar_fc(raw):
    """Bepaal flight category uit METAR-tekst (eenvoudige heuristiek)."""
    import re as _re
    vis_m = None
    ceiling_ft = None
    # Zichtbaarheid (SM of meters)
    m = _re.search(r' (\d+)SM ', raw)
    if m:
        vis_m = float(m.group(1)) * 1609.34
    else:
        m = _re.search(r' (\d{4}) ', raw)
        if m:
            v = int(m.group(1))
            if v <= 9999:
                vis_m = float(v)
    if '9999' in raw or 'CAVOK' in raw:
        vis_m = 10000.0
    # Wolkenbasis
    for cloud in _re.finditer(r'(?:BKN|OVC)(\d{3})', raw):
        ft = int(cloud.group(1)) * 100
        if ceiling_ft is None or ft < ceiling_ft:
            ceiling_ft = ft
    if vis_m is None and ceiling_ft is None:
        return ''
    vis_sm = (vis_m / 1609.34) if vis_m is not None else 99
    ceil   = ceiling_ft if ceiling_ft is not None else 99999
    if vis_sm < 1 or ceil < 500:
        return 'LIFR'
    if vis_sm < 3 or ceil < 1000:
        return 'IFR'
    if vis_sm <= 5 or ceil <= 3000:
        return 'MVFR'
    return 'VFR'


def _do_fetch_metar():
    """Laad METARs van NOAA text-feed, sla op in _metar_cache. Mag blokkeren."""
    global _metar_cache, _metar_time
    # Stap 1: kust-stations lijst (gecacht na eerste keer)
    coastal = _load_coastal_stations()

    # Stap 2: NOAA actuele METAR text-feed (~300 KB, alle ~5000 stations)
    hour = datetime.now(timezone.utc).strftime("%H")
    url  = f"https://tgftp.nws.noaa.gov/data/observations/metar/cycles/{hour}Z.TXT"
    req  = urllib.request.Request(url, headers={"User-Agent": "RWS-Golfhoogte-Proxy/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        text = r.read().decode("utf-8", errors="replace")

    # Stap 3: parseer blokken → bewaar meest recente METAR per kust-ICAO
    import re as _re
    blocks = _re.split(r'\n(?=\d{4}/\d{2}/\d{2} \d{2}:\d{2})', text.strip())
    seen   = {}
    for block in blocks:
        lines = block.strip().splitlines()
        if len(lines) < 2:
            continue
        ts_line  = lines[0].strip()
        raw_line = lines[1].strip()
        m = _re.match(r'^([A-Z][A-Z0-9]{3}) \d{6}Z', raw_line)
        if not m:
            continue
        icao = m.group(1)
        if icao not in coastal:
            continue
        if icao not in seen or ts_line > seen[icao][1]:
            seen[icao] = (raw_line, ts_line)

    now_iso  = datetime.now(timezone.utc).isoformat()
    features = []
    for icao, (raw, ts) in seen.items():
        naam, lat, lon, elev_m = coastal[icao]
        fc = _parse_metar_fc(raw)
        features.append({"type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "code":     f"metar.{icao.lower()}",
                "icao":     icao,
                "naam":     naam,
                "raw":      raw,
                "tijdstip": ts,
                "fc":       fc,
                "elev":     elev_m,
                "bron":     "NOAA/NWS",
            }})

    _metar_cache = {"type": "FeatureCollection", "features": features,
                    "aantalStations": len(features), "opgehaald": now_iso}
    _metar_time  = time.time()
    print(f"[METAR] {len(features)} kuststations geladen")


def _refresh_metar_bg():
    """Vul/ververs _metar_cache in de achtergrond (blokkeert request-thread NIET)."""
    try:
        if _metar_cache and (time.time() - _metar_time) < METAR_CACHE_S:
            return
        _do_fetch_metar()
    except Exception as e:
        print(f"[METAR bg] Fout: {e}")


def get_metar_data():
    """Geeft de achtergrond-cache terug. Geeft laden:True als cache nog leeg is."""
    return _metar_cache or _EMPTY_METAR


# ── Radiosonde-soundings (University of Wyoming, WSGI) ─────────────────────────
# Stationslijst: IGRA2 (NOAA) → filtert op recent-actieve WMO-stations wereldwijd.
# Sounding-detail: University of Wyoming WSGI (hoge-resolutie BUFR-niveaus).
SONDE_IGRA_URL   = "https://www.ncei.noaa.gov/pub/data/igra/igra2-station-list.txt"
SONDE_WYO_URL    = "https://weather.uwyo.edu/wsgi/sounding"
SONDE_STAT_TTL   = 24 * 60 * 60   # stationslijst 24 uur cachen
SONDE_MIN_LASTYR = 2025           # alleen stations die in/na dit jaar rapporteerden

SONDE_STATIONS_FILE  = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                    "sounding-stations.json")

_sonde_stations      = None
_sonde_stations_time = 0.0
_sonde_lock          = threading.Lock()


def _sonde_clean_name(raw):
    """Maak IGRA-stationsnaam leesbaar: strip [..]-codes, nette hoofdletters."""
    name = re.sub(r"\[[^\]]*\]", "", raw).strip()
    name = re.sub(r"\s+", " ", name)
    parts = []
    for w in name.split(" "):
        parts.append(w.capitalize() if (w.isupper() and len(w) > 1) else w)
    return " ".join(parts)


def _parse_igra_stations(text):
    """Filter de IGRA2-stationslijst tot recent-actieve WMO-stations.

    We houden alleen WMO-stations (netwerk-teken 'M') over die recent nog
    rapporteerden; het 5-cijferige WMO-nummer werkt direct als Wyoming-id.
    """
    out = []
    for line in text.splitlines():
        if len(line) < 82 or line[2] != "M":        # alleen WMO-netwerkstations
            continue
        try:
            lastyr = int(line[77:81])
        except ValueError:
            continue
        if lastyr < SONDE_MIN_LASTYR:               # alleen recent-actief
            continue
        try:
            lat = float(line[12:20]); lon = float(line[21:30])
        except ValueError:
            continue
        if lat < -90 or lat > 90 or lon < -180 or lon > 180:
            continue
        wmo  = line[6:11]                            # 5-cijferig WMO-nummer (voorloopnullen behouden)
        name = _sonde_clean_name(line[41:71])
        out.append({"id": wmo, "name": name,
                    "lat": round(lat, 4), "lon": round(lon, 4)})
    return out


def _fetch_igra_stations():
    """Haal de IGRA2-stationslijst live op (fallback — kan traag zijn)."""
    req = urllib.request.Request(SONDE_IGRA_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=45) as r:
        text = r.read().decode("utf-8", "replace")
    return _parse_igra_stations(text)


def _load_sonde_stations():
    """Geef (gecached) de wereldwijde lijst radiosonde-stations terug.

    Leest bij voorkeur de gebundelde snapshot (sounding-stations.json) — snel en
    betrouwbaar. Alleen als die ontbreekt valt hij terug op een live IGRA-fetch.
    """
    global _sonde_stations, _sonde_stations_time
    with _sonde_lock:
        if _sonde_stations is not None:
            return _sonde_stations

    stns = None
    try:                                            # 1) gebundelde snapshot
        with open(SONDE_STATIONS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        stns = data["stations"] if isinstance(data, dict) else data
        if stns:
            print(f"[SONDE] {len(stns)} stations uit snapshot geladen")
    except Exception as e:
        print(f"[SONDE] snapshot niet beschikbaar ({e}) — live IGRA-fetch")

    if not stns:                                    # 2) fallback: live IGRA
        stns = _fetch_igra_stations()
        print(f"[SONDE] {len(stns)} stations live van IGRA geladen")

    with _sonde_lock:
        _sonde_stations      = stns
        _sonde_stations_time = time.time()
    return stns


def _sonde_latest_synoptic(offset=0):
    """Meest recente hoofd-sondeertijd (00/12 UTC) als 'YYYY-MM-DD HH:00:00'.

    offset schuift in stappen van 12 uur terug (negatief) of vooruit (positief).
    """
    now  = datetime.now(timezone.utc) - timedelta(hours=2)   # ~2 uur verwerkingslag
    hour = 12 if now.hour >= 12 else 0
    base = now.replace(hour=hour, minute=0, second=0, microsecond=0)
    base = base + timedelta(hours=12 * offset)
    return base.strftime("%Y-%m-%d %H:00:00")


def _sonde_parse(html, stn, datetime_str):
    """Parse Wyoming TEXT:LIST-HTML naar gestructureerde JSON."""
    name = ""
    m = html.find("<H3>")
    if m != -1:
        name = html[m + 4: html.find("</H3>", m)].strip()

    lat = lon = None
    li = html.find("Latitude:")
    if li != -1:
        seg = html[li:li + 120]
        mlat = re.search(r"Latitude:\s*(-?\d+(?:\.\d+)?)", seg)
        mlon = re.search(r"Longitude:\s*(-?\d+(?:\.\d+)?)", seg)
        if mlat: lat = float(mlat.group(1))
        if mlon: lon = float(mlon.group(1))

    valid = ""
    h1 = html.find("<H1>")
    if h1 != -1:
        valid = html[h1 + 4: html.find("</H1>", h1)].strip()

    # Eerste <PRE>-blok = de niveautabel (vaste kolommen van 7 tekens breed).
    levels = []
    p0 = html.find("<PRE>")
    if p0 != -1:
        block = html[p0 + 5: html.find("</PRE>", p0)]
        def _num(row, a, b):
            s = row[a:b].strip()
            if not s:
                return None
            try:
                return float(s)
            except ValueError:
                return None
        for row in block.splitlines():
            if len(row) < 21:
                continue
            p = _num(row, 0, 7)
            t = _num(row, 14, 21)
            if p is None or t is None:                # kop-/scheidingsregels overslaan
                continue
            levels.append({
                "p":   p,
                "h":   _num(row, 7, 14),
                "t":   t,
                "td":  _num(row, 21, 28),
                "dir": _num(row, 42, 49),
                "spd": _num(row, 49, 56),
            })

    # Tweede <PRE>-blok = afgeleide indices (CAPE, PW, LI, …) als label:waarde.
    indices = {}
    p1 = html.find("<PRE>", html.find("</PRE>", p0) + 1) if p0 != -1 else -1
    if p1 != -1:
        block2 = html[p1 + 5: html.find("</PRE>", p1)]
        for row in block2.splitlines():
            if ":" in row:
                k, v = row.rsplit(":", 1)
                k = k.strip(); v = v.strip()
                if k and v:
                    indices[k] = v

    # Wyoming's WSGI-endpoint levert het indices-blok niet meer, dus berekenen
    # we CAPE/CIN/LCL/LFC/EL/LI/SI/K-index/Totals/PW zelf uit het profiel.
    derived = {}
    try:
        derived = _sonde_compute_indices(levels)
    except Exception as exc:                          # nooit de sounding laten sneuvelen
        print(f"[sounding-indices] {stn}: {exc}")

    return {"id": stn, "name": name, "lat": lat, "lon": lon,
            "valid": valid, "datetime": datetime_str,
            "levels": levels, "indices": indices, "derived": derived}


def _sonde_compute_indices(levels):
    """Bereken afgeleide sounding-parameters uit het hoge-resolutie profiel.

    Levert een dict met numerieke waarden (SI-eenheden waar logisch):
      CAPE, CIN [J/kg]; LCL/LFC/EL druk [hPa] en hoogte [m]; LCL-temp [°C];
      LI, SI, KI, TT [°C]; PW [mm]. Sleutels ontbreken als ze onberekenbaar zijn.
    Oppervlaktepakket-methode met virtuele temperatuur; pseudoadiabatische
    opstijging (RK2) boven het LCL.
    """
    exp, log = _math.exp, _math.log
    Rd, cp, g, eps, Lv = 287.04, 1005.0, 9.80665, 0.622, 2.501e6
    kappa = Rd / cp

    def es(Tc):                                   # verzadigingsdampdruk [hPa]
        return 6.112 * exp(17.67 * Tc / (Tc + 243.5))
    def rsat(Tc, p):                              # verzadigingsmengverhouding [kg/kg]
        e = min(es(Tc), p * 0.999)
        return eps * e / (p - e)

    lv = [l for l in levels if l.get("p") and l.get("t") is not None]
    if len(lv) < 5:
        return {}
    lv = sorted(lv, key=lambda x: -x["p"])
    ps  = [l["p"] for l in lv]
    ts  = [l["t"] for l in lv]
    tds = [l.get("td") for l in lv]
    hs  = [l.get("h") for l in lv]

    def interp(pt, arr):                          # log-p lineaire interpolatie
        if pt >= ps[0]:  return arr[0]
        if pt <= ps[-1]: return arr[-1]
        for i in range(len(ps) - 1):
            if ps[i] >= pt >= ps[i + 1]:
                a, b = arr[i], arr[i + 1]
                if a is None or b is None:
                    return b if a is None else a
                f = (log(pt) - log(ps[i])) / (log(ps[i + 1]) - log(ps[i]))
                return a + f * (b - a)
        return None

    p0, T0c, Td0c = ps[0], ts[0], tds[0]
    if Td0c is None:
        return {}
    T0k, Td0k = T0c + 273.15, Td0c + 273.15
    r0 = rsat(Td0c, p0)

    out = {}

    # Lifting Condensation Level (Bolton 1980)
    Tlcl  = 1.0 / (1.0 / (Td0k - 56.0) + log(T0k / Td0k) / 800.0) + 56.0
    Plcl  = p0 * (Tlcl / T0k) ** (1.0 / kappa)
    theta = T0k * (1000.0 / p0) ** kappa
    zlcl  = interp(Plcl, hs)
    out["LCL_P"] = round(Plcl, 1)
    out["LCL_T"] = round(Tlcl - 273.15, 1)
    if zlcl is not None: out["LCL_Z"] = round(zlcl)

    # Fijn drukraster (oppervlak → 100 hPa of hoogste niveau)
    ptop = max(100.0, ps[-1])
    grid = []
    p = p0
    while p > ptop:
        grid.append(p); p -= 5.0
    grid.append(ptop)
    if ptop < Plcl < p0:
        grid.append(Plcl)
    grid = sorted(set(grid), reverse=True)

    # Pakkettemperatuur langs opstijging
    parcelTk = {}
    for gp in grid:                               # droogadiabatisch onder LCL
        if gp >= Plcl:
            parcelTk[gp] = theta * (gp / 1000.0) ** kappa
    Tk, p_prev = Tlcl, Plcl                        # pseudoadiabatisch boven LCL
    for gp in [q for q in grid if q < Plcl]:
        lnp1, lnp2 = log(p_prev), log(gp)
        dlnp = lnp2 - lnp1
        rs = rsat(Tk - 273.15, p_prev)
        k1 = (Rd * Tk + Lv * rs) / (cp + (Lv * Lv * rs * eps) / (Rd * Tk * Tk))
        Tmid = Tk + k1 * dlnp * 0.5
        pmid = exp(lnp1 + dlnp * 0.5)
        rs2 = rsat(Tmid - 273.15, pmid)
        k2 = (Rd * Tmid + Lv * rs2) / (cp + (Lv * Lv * rs2 * eps) / (Rd * Tmid * Tmid))
        Tk += k2 * dlnp
        parcelTk[gp] = Tk
        p_prev = gp

    pkeys = sorted(parcelTk.keys(), reverse=True)
    def parcel_at(pt):
        if pt >= pkeys[0]:  return parcelTk[pkeys[0]]
        if pt <= pkeys[-1]: return parcelTk[pkeys[-1]]
        for i in range(len(pkeys) - 1):
            if pkeys[i] >= pt >= pkeys[i + 1]:
                a, b = parcelTk[pkeys[i]], parcelTk[pkeys[i + 1]]
                f = (log(pt) - log(pkeys[i])) / (log(pkeys[i + 1]) - log(pkeys[i]))
                return a + f * (b - a)
        return None

    # Virtuele temperatuur van pakket en omgeving op het raster
    Tvp, Tve = {}, {}
    for gp in grid:
        Tpk = parcelTk[gp]; Tpc = Tpk - 273.15
        rp = r0 if gp >= Plcl else rsat(Tpc, gp)
        Tvp[gp] = Tpk * (1.0 + 0.608 * rp)
        Tec = interp(gp, ts); Tdc = interp(gp, tds)
        re = rsat(Tdc, gp) if Tdc is not None else 0.0
        Tve[gp] = (Tec + 273.15) * (1.0 + 0.608 * re)

    # Drijfvermogen op het raster (oppervlak → top)
    gs = sorted(grid, reverse=True)
    b  = [Rd * (Tvp[p] - Tve[p]) for p in gs]

    # Index van het LCL in het raster (Plcl is aan het raster toegevoegd)
    lcl_i = 0
    for i in range(len(gs)):
        if gs[i] <= Plcl:
            lcl_i = i; break
    # LFC = laagste niveau ≥ LCL met positief drijfvermogen. Drijft het pakket al
    # op het LCL positief (vochtige (sub)tropische lucht), dan ligt het LFC op het
    # LCL zelf — anders bij de eerste overgang naar positief drijfvermogen erboven.
    lfc_i = None
    if b[lcl_i] > 0:
        lfc_i = lcl_i
    else:
        for i in range(lcl_i + 1, len(gs)):
            if b[i] > 0 and b[i - 1] <= 0:
                lfc_i = i; break
    # EL = hoogste overgang van positief naar negatief boven het LFC
    el_i = None
    if lfc_i is not None:
        for i in range(lfc_i + 1, len(gs)):
            if b[i] <= 0 and b[i - 1] > 0:
                el_i = i
        if el_i is None:
            el_i = len(gs) - 1

    cape = cin = 0.0
    if lfc_i is not None:
        for i in range(lfc_i, el_i):                 # CAPE: positieve arbeid LFC→EL
            bavg = 0.5 * (b[i] + b[i + 1])
            if bavg > 0:
                cape += bavg * log(gs[i] / gs[i + 1])
        for i in range(0, lfc_i):                    # CIN: negatieve arbeid opp.→LFC
            bavg = 0.5 * (b[i] + b[i + 1])
            if bavg < 0:
                cin += bavg * log(gs[i] / gs[i + 1])
    out["CAPE"] = round(cape)
    out["CIN"]  = round(cin)
    if lfc_i is not None:
        lfc_p = gs[lfc_i]
        out["LFC_P"] = round(lfc_p, 1)
        zl = interp(lfc_p, hs)
        if zl is not None: out["LFC_Z"] = round(zl)
        if el_i is not None and cape > 0:
            el_p = gs[el_i]
            out["EL_P"] = round(el_p, 1)
            ze = interp(el_p, hs)
            if ze is not None: out["EL_Z"] = round(ze)

    # Standaardindices op vaste drukvlakken
    T850, Td850 = interp(850, ts), interp(850, tds)
    T700, Td700 = interp(700, ts), interp(700, tds)
    T500        = interp(500, ts)
    if p0 >= 850 and None not in (T850, Td850, T700, Td700, T500):
        out["KI"] = round((T850 - T500) + Td850 - (T700 - Td700), 1)
        out["TT"] = round((T850 - T500) + (Td850 - T500), 1)
    if T500 is not None:
        pp500 = parcel_at(500.0)
        if pp500 is not None:
            out["LI"] = round(T500 - (pp500 - 273.15), 1)
        if p0 >= 850 and T850 is not None and Td850 is not None:
            out["SI"] = round(T500 - _sonde_lift_temp(850.0, T850, Td850, 500.0), 1)

    # Precipitable water [mm]
    pw = 0.0
    for i in range(len(lv) - 1):
        if tds[i] is None or tds[i + 1] is None:
            continue
        r1 = rsat(tds[i], ps[i]); r2 = rsat(tds[i + 1], ps[i + 1])
        pw += 0.5 * (r1 + r2) * (ps[i] - ps[i + 1])
    out["PW"] = round(pw * 100.0 / g, 1)

    # Opstijgende-luchtbel-temperatuur [[p, °C], …] voor CAPE/CIN-arcering in de Skew-T
    out["parcel"] = [[round(gp, 1), round(parcelTk[gp] - 273.15, 2)]
                     for gp in sorted(parcelTk.keys(), reverse=True)]

    return out


def _sonde_lift_temp(p_start, T_start_c, Td_start_c, p_end):
    """Til een pakket (p_start, T, Td) pseudoadiabatisch naar p_end; geef T [°C]."""
    exp, log = _math.exp, _math.log
    Rd, cp, eps, Lv = 287.04, 1005.0, 0.622, 2.501e6
    kappa = Rd / cp
    def rsat(Tc, p):
        e = min(6.112 * exp(17.67 * Tc / (Tc + 243.5)), p * 0.999)
        return eps * e / (p - e)
    Tk, Tdk = T_start_c + 273.15, Td_start_c + 273.15
    Tl = 1.0 / (1.0 / (Tdk - 56.0) + log(Tk / Tdk) / 800.0) + 56.0
    Pl = p_start * (Tl / Tk) ** (1.0 / kappa)
    theta = Tk * (1000.0 / p_start) ** kappa
    if p_end >= Pl:
        return theta * (p_end / 1000.0) ** kappa - 273.15
    Tk = Tl
    steps = max(1, int((Pl - p_end) / 5.0))
    lnp1, lnp2 = log(Pl), log(p_end)
    dl = (lnp2 - lnp1) / steps
    for s in range(steps):
        p_here = exp(lnp1 + dl * s)
        rs = rsat(Tk - 273.15, p_here)
        k1 = (Rd * Tk + Lv * rs) / (cp + (Lv * Lv * rs * eps) / (Rd * Tk * Tk))
        Tmid = Tk + k1 * dl * 0.5
        pmid = exp(lnp1 + dl * (s + 0.5))
        rs2 = rsat(Tmid - 273.15, pmid)
        k2 = (Rd * Tmid + Lv * rs2) / (cp + (Lv * Lv * rs2 * eps) / (Rd * Tmid * Tmid))
        Tk += k2 * dl
    return Tk - 273.15


def _fetch_sounding(stn, datetime_str):
    """Haal één sounding op bij Wyoming WSGI en parse hem.

    Wyoming geeft HTTP 400/404 als er voor die tijd geen sounding is; dat
    behandelen we als 'geen data' (lege niveaus) i.p.v. een fout.
    """
    q   = urlencode({"datetime": datetime_str, "id": stn, "type": "TEXT:LIST"})
    req = urllib.request.Request(f"{SONDE_WYO_URL}?{q}",
                                 headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            html = r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        if e.code in (400, 404):
            return {"id": stn, "name": "", "lat": None, "lon": None,
                    "valid": "", "datetime": datetime_str,
                    "levels": [], "indices": {}, "derived": {}}
        raise
    return _sonde_parse(html, stn, datetime_str)


def fetch_knmi_data():
    """Haalt actuele waarnemingen op van Buienradar (elke 10 min, geen API key)."""
    global _knmi_vis_features, _vis_cache_ready
    req = urllib.request.Request(
        BUIENRADAR_URL,
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        data = json.loads(r.read().decode("utf-8"))

    stations = data.get("actual", {}).get("stationmeasurements", [])
    now = datetime.now(timezone.utc)

    features    = []   # temp/wind-laag: vereist temperatuur, geen offshore platforms
    vis_list    = []   # zicht-laag: alle stations met vv (ook offshore, geen temp-eis)

    for s in stations:
        lat = s.get("lat")
        lon = s.get("lon")
        if lat is None or lon is None:
            continue
        if lat < 50.5 or lat > 55.5 or lon < 2.5 or lon > 8.0:
            continue

        naam   = s.get("stationname", "").replace("Meetstation ", "")
        naam_l = naam.lower()
        ts     = s.get("timestamp", now.isoformat())
        sid    = str(s.get("stationid", ""))

        def fval(key, _s=s):
            v = _s.get(key)
            return round(float(v), 2) if v is not None and v != "" else None

        def ival(key, _s=s):
            v = _s.get(key)
            return int(v) if v is not None and v != "" else None

        temp   = fval("temperature")
        vv_m   = fval("visibility")
        vv     = round(vv_m / 1000, 2) if vv_m is not None else None

        # ── Zicht-laag: alle stations met zichtdata, ook offshore platforms ──
        if vv is not None:
            vis_list.append({
                "type":     "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {
                    "code":    f"knmi.vis.{sid}",
                    "naam":    naam,
                    "bron":    "Buienradar/KNMI",
                    "tijdstip": ts,
                    "vv":      vv,
                    "ta":      temp,
                },
            })

        # ── Temp/wind-laag: geen offshore platforms, temperatuur verplicht ──
        if any(kw in naam_l for kw in (
            "lichteiland", "europlatform", "k13", "meetpost", "platform",
            "roughness", "north sea", "noordzee",
        )):
            continue
        if temp is None:
            continue

        dd_raw = ival("winddirectiondegrees")
        dd     = None if dd_raw in (0, 990) else dd_raw
        rh     = fval("rainFallLastHour")

        code = f"knmi.vis.{sid}"
        _record_knmi_temp(code, ts, temp)
        features.append({
            "type":     "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "code":       code,
                "naam":       naam,
                "station_id": sid,
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

    _knmi_vis_features = vis_list
    _vis_cache_ready   = True
    print(f"[KNMI/BR] {len(features)} temp-stations, {len(vis_list)} zicht-stations geladen")
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

import math as _math

def _sanitize(obj):
    """Vervang NaN/Infinity diep in een dict/list door None (geldige JSON)."""
    if isinstance(obj, float):
        return None if not _math.isfinite(obj) else obj
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    return obj

def _safe_json(obj):
    """json.dumps die NaN/Infinity vervangt door null."""
    return json.dumps(_sanitize(obj), ensure_ascii=False)


class Handler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        addr = self.client_address[0] if self.client_address else "?"
        print(f"[HTTP] {addr} – {fmt % args}")

    def send_cors(self):
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _accepts_gzip(self):
        return "gzip" in self.headers.get("Accept-Encoding", "")

    def _send_json(self, data_bytes, max_age=0):
        """Stuur JSON met optionele gzip-compressie en cache-headers."""
        if self._accepts_gzip() and len(data_bytes) > 500:
            body = _gzip.compress(data_bytes, compresslevel=6)
            self.send_header("Content-Encoding", "gzip")
        else:
            body = data_bytes
        ct = max_age if isinstance(max_age, str) else (
            f"public, max-age={max_age}" if max_age > 0 else "no-cache, no-store")
        self.send_header("Content-Type",   "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control",  ct)
        self.send_cors()
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_cors()
        self.end_headers()

    def do_GET(self):
        path = self.path.split("?")[0]

        # ── /api/ping ─────────────────────────────────────────────────────
        if path == "/api/ping":
            body = b'{"ok":true}'
            self.send_response(200)
            self.send_header("Content-Type",   "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control",  "no-store")
            self.send_cors()
            self.end_headers()
            self.wfile.write(body)
            return

        # ── /api/bliksem-status  (diagnostiek) ───────────────────────────
        elif path == "/api/bliksem-status":
            with _bliksem_lock:
                buf_size = len(_bliksem_deque)
            with _bliksem_clients_lock:
                sse_clients = len(_bliksem_clients)
            info = {
                "ws_ok":       _WS_OK,
                "buf_size":    buf_size,
                "sse_clients": sse_clients,
                "last_strike": _bliksem_last_ts,
                "total_recv":  _bliksem_total,
                "server_ts":   time.time(),
            }
            body = json.dumps(info).encode()
            self.send_response(200)
            self.send_header("Content-Type",   "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control",  "no-store")
            self.send_cors()
            self.end_headers()
            self.wfile.write(body)
            return

        # ── /bliksem-debug ───────────────────────────────────────────────
        if path == "/bliksem-debug":
            body = b"""<!DOCTYPE html>
<html><head><title>Bliksem WS Test</title></head>
<body style="background:#111;color:#eee;font-family:monospace;padding:20px">
<h2>Blitzortung WebSocket Test</h2>
<div id="log"></div>
<script>
const log = document.getElementById('log');
function add(msg, color) {
  const d = document.createElement('div');
  d.style.color = color || '#eee';
  d.textContent = new Date().toISOString().slice(11,19) + ' ' + msg;
  log.prepend(d);
}
let count = 0;
for (const host of ['ws1.blitzortung.org','ws2.blitzortung.org']) {
  add('Probeer ' + host + '...', '#aaa');
  const ws = new WebSocket('wss://' + host);
  ws.onopen = () => {
    add('OPEN ' + host + ' protocol="' + ws.protocol + '"', '#4f4');
    ws.send(JSON.stringify({west:-180,east:180,north:90,south:-90}));
    add('Subscriptie verstuurd', '#4af');
  };
  ws.onmessage = (e) => {
    count++;
    add('DATA #' + count + ': ' + String(e.data).slice(0,150), '#ff4');
  };
  ws.onerror = () => add('FOUT ' + host, '#f44');
  ws.onclose = (e) => add('CLOSE ' + host + ' code=' + e.code, '#f84');
}
</script></body></html>"""
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        # ── /api/status ─────────────────────────────────────────────────────
        elif path == "/api/status":
            lock_free = _refresh_lock.acquire(blocking=False)
            if lock_free:
                _refresh_lock.release()
            status = {
                "cache_stations": _cache["aantalStations"] if _cache else 0,
                "cache_age_s":    round(time.time() - _cache_time) if _cache_time else None,
                "refresh_running": not lock_free,
                "socib_wave_bg":  len(_socib_wave_bg),
                "socib_wind_bg":  len(_socib_wind_bg),
                "wind_stations":  _wind_cache["aantalStations"] if _wind_cache else 0,
                "temp_bg_stations": _temp_bg["aantalStations"] if _temp_bg else 0,
                "metar_stations": _metar_cache["aantalStations"] if _metar_cache else 0,
                "coastal_stations_loaded": _coastal_stations is not None,
                "version": "2026-05-26-metar",
            }
            body = json.dumps(status).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type",   "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_cors()
            self.end_headers()
            self.wfile.write(body)

        # ── /api/waves ──────────────────────────────────────────────────────
        elif path == "/api/waves":
            try:
                data = get_data()
                self.send_response(200)
                self._send_json(_safe_json(data).encode("utf-8"))
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
                self.send_response(200)
                self._send_json(_safe_json(data).encode("utf-8"))
            except Exception as exc:
                print(f"[FOUT temp] {exc}")
                body = json.dumps({"error": str(exc)}).encode("utf-8")
                self.send_response(500)
                self.send_header("Content-Type",   "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_cors()
                self.end_headers()
                self.wfile.write(body)

        # ── /api/visibility ──────────────────────────────────────────────
        elif path == "/api/visibility":
            try:
                if not _vis_cache_ready:
                    self.send_response(200)
                    self._send_json(json.dumps({"type": "FeatureCollection", "features": [],
                                                "aantalStations": 0, "laden": True}).encode())
                    return
                features = _knmi_vis_features + _ndbc_vis_features + _ocean_vis_features
                data = {
                    "type": "FeatureCollection",
                    "features": features,
                    "aantalStations": len(features),
                    "opgehaald": datetime.now(timezone.utc).isoformat(),
                }
                self.send_response(200)
                self._send_json(_safe_json(data).encode("utf-8"))
            except Exception as exc:
                body = json.dumps({"error": str(exc)}).encode("utf-8")
                self.send_response(500)
                self.send_header("Content-Type",   "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_cors()
                self.end_headers()
                self.wfile.write(body)

        # ── /api/visibility-history?code=... ────────────────────────────
        elif path == "/api/visibility-history":
            params = parse_qs(urlparse(self.path).query)
            code   = (params.get("code") or [None])[0]
            try:
                if not code:
                    raise ValueError("code parameter verplicht")

                # Zoek lat/lon op in alle zicht-caches
                all_vis = _knmi_vis_features + _ndbc_vis_features + _ocean_vis_features
                lat = lon = None
                for f in all_vis:
                    if f["properties"].get("code") == code:
                        coords = f["geometry"]["coordinates"]
                        lon, lat = coords[0], coords[1]
                        break

                if lat is None:
                    raise ValueError(f"Station '{code}' niet gevonden")

                # Open-Meteo uurlijkse zicht voor afgelopen 24 uur
                url = (f"https://api.open-meteo.com/v1/forecast"
                       f"?latitude={lat}&longitude={lon}"
                       f"&hourly=visibility&past_days=1&forecast_days=1"
                       f"&timezone=UTC")
                req = urllib.request.Request(
                    url, headers={"User-Agent": "RWS-Golfhoogte-Proxy/1.0"})
                with urllib.request.urlopen(req, timeout=10) as r:
                    om = json.loads(r.read().decode())

                now_utc = datetime.now(timezone.utc)
                cutoff  = now_utc - timedelta(hours=24)
                times   = om.get("hourly", {}).get("time", [])
                vis_raw = om.get("hourly", {}).get("visibility", [])

                data_pts = []
                for t_str, v_m in zip(times, vis_raw):
                    if v_m is None:
                        continue
                    try:
                        dt = datetime.fromisoformat(t_str).replace(tzinfo=timezone.utc)
                    except Exception:
                        continue
                    if dt < cutoff or dt > now_utc:
                        continue
                    data_pts.append({"t": dt.isoformat(), "v": round(v_m / 1000, 1)})

                data = {"code": code, "data": data_pts}
                self.send_response(200)
                self._send_json(_safe_json(data).encode("utf-8"))
            except Exception as exc:
                print(f"[FOUT visibility-history] {exc}")
                body = json.dumps({"error": str(exc)}).encode()
                self.send_response(500 if "niet gevonden" not in str(exc) else 404)
                self.send_header("Content-Type",   "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_cors()
                self.end_headers()
                self.wfile.write(body)

        # ── /api/wind ────────────────────────────────────────────────────
        elif path == "/api/wind":
            try:
                data = get_wind_data()
                self.send_response(200)
                self._send_json(_safe_json(data).encode("utf-8"))
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
                self.send_response(200)
                self._send_json(_safe_json(data).encode("utf-8"))
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
                self.send_response(200)
                self._send_json(_safe_json(data).encode("utf-8"))
            except Exception as exc:
                print(f"[FOUT knmi] {exc}")
                body = json.dumps({"error": str(exc)}).encode("utf-8")
                self.send_response(500)
                self.send_header("Content-Type",   "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_cors()
                self.end_headers()
                self.wfile.write(body)

        # ── /api/nl-land (provinciegrenzen, land only) ───────────────────
        elif path == "/api/nl-land":
            data = get_nl_land()
            if data:
                body = _safe_json(data).encode("utf-8")
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

        # ── /api/nl-border ───────────────────────────────────────────────
        elif path == "/api/nl-border":
            data = get_nl_border()
            if data:
                body = _safe_json(data).encode("utf-8")
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
                self.send_response(200)
                self._send_json(_safe_json(data).encode("utf-8"))
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
        # ── /api/metar ───────────────────────────────────────────────────
        elif path == "/api/metar":
            try:
                data = get_metar_data()
                self.send_response(200)
                self._send_json(_safe_json(data).encode("utf-8"))
            except Exception as exc:
                print(f"[FOUT metar] {exc}")
                body = json.dumps({"error": str(exc)}).encode()
                self.send_response(500)
                self.send_header("Content-Type",   "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_cors()
                self.end_headers()
                self.wfile.write(body)

        # ── /api/sounding-stations  (wereldwijde radiosonde-stations) ────
        elif path == "/api/sounding-stations":
            try:
                stns = _load_sonde_stations()
                self.send_response(200)
                self._send_json(
                    json.dumps({"stations": stns, "count": len(stns)}).encode("utf-8"),
                    max_age=3600)
            except Exception as exc:
                print(f"[FOUT sounding-stations] {exc}")
                body = json.dumps({"error": str(exc)}).encode()
                self.send_response(500)
                self.send_header("Content-Type",   "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_cors()
                self.end_headers()
                self.wfile.write(body)

        # ── /api/sounding  (één sounding, hoge-resolutie BUFR-niveaus) ───
        elif path == "/api/sounding":
            qs  = parse_qs(urlparse(self.path).query)
            stn = (qs.get("id", [""])[0] or "").strip()
            dt  = (qs.get("datetime", [""])[0] or "").strip()
            if not stn:
                body = json.dumps({"error": "id ontbreekt"}).encode()
                self.send_response(400)
                self.send_header("Content-Type",   "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_cors()
                self.end_headers()
                self.wfile.write(body)
                return
            if not dt:
                dt = _sonde_latest_synoptic()
            try:
                data = _fetch_sounding(stn, dt)
                self.send_response(200)
                self._send_json(_safe_json(data).encode("utf-8"), max_age=300)
            except Exception as exc:
                print(f"[FOUT sounding] {stn} {dt}: {exc}")
                body = json.dumps({"error": str(exc)}).encode()
                self.send_response(500)
                self.send_header("Content-Type",   "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_cors()
                self.end_headers()
                self.wfile.write(body)

        # ── /api/bliksem ─────────────────────────────────────────────────
        elif path == "/api/bliksem":
            cutoff = time.time() - _BLIKSEM_MAX_AGE
            with _bliksem_lock:
                recent = [(ts, lat, lon) for ts, lat, lon in _bliksem_deque if ts >= cutoff]
            features = [
                {"type": "Feature",
                 "geometry": {"type": "Point", "coordinates": [lon, lat]},
                 "properties": {"ts": ts}}
                for ts, lat, lon in recent
            ]
            data = {
                "type":         "FeatureCollection",
                "features":     features,
                "aantalStrikes": len(features),
                "serverTs":     time.time(),
                "wsOk":         _WS_OK,
            }
            self.send_response(200)
            self._send_json(_safe_json(data).encode("utf-8"), max_age=0)

        # ── /api/bliksem-stream  (SSE — live push) ───────────────────────
        elif path == "/api/bliksem-stream":
            # Stuur eerst alle strikes uit het laatste uur als batch
            cutoff = time.time() - _BLIKSEM_MAX_AGE
            with _bliksem_lock:
                history = [(ts, lat, lon) for ts, lat, lon in _bliksem_deque if ts >= cutoff]

            self.send_response(200)
            self.send_header("Content-Type",  "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("X-Accel-Buffering", "no")
            self.send_cors()
            self.end_headers()

            try:
                # Schrijf historische buffer als eerste event
                batch = [{"ts": ts, "lat": lat, "lon": lon} for ts, lat, lon in history]
                line = ("data: " + json.dumps({"batch": batch}) + "\n\n").encode()
                self.wfile.write(line)
                self.wfile.flush()

                # Registreer als SSE-client
                q = _queue.Queue(maxsize=200)
                with _bliksem_clients_lock:
                    _bliksem_clients.add(q)
                try:
                    while True:
                        try:
                            msg = q.get(timeout=20)
                            self.wfile.write(b"data: " + msg + b"\n")
                            self.wfile.flush()
                        except _queue.Empty:
                            # Keep-alive comment
                            self.wfile.write(b": ka\n\n")
                            self.wfile.flush()
                except Exception:
                    pass
                finally:
                    with _bliksem_clients_lock:
                        _bliksem_clients.discard(q)
            except Exception:
                pass

        # ── /api/metar-detail?id=ICAO ────────────────────────────────────
        elif path == "/api/metar-detail":
            params = parse_qs(urlparse(self.path).query)
            icao   = (params.get("id") or [None])[0]
            try:
                if not icao:
                    raise ValueError("id parameter verplicht")
                icao = icao.upper()

                from concurrent.futures import wait as _wait_m

                def _fetch_metar_raw():
                    url = (f"https://aviationweather.gov/api/data/metar"
                           f"?ids={icao}&format=json&hours=24")
                    req = urllib.request.Request(
                        url, headers={"User-Agent": "RWS-Golfhoogte-Proxy/1.0"})
                    with urllib.request.urlopen(req, timeout=15) as r:
                        return json.loads(r.read().decode("utf-8"))

                def _fetch_taf_raw():
                    url = (f"https://aviationweather.gov/api/data/taf"
                           f"?ids={icao}&format=json")
                    req = urllib.request.Request(
                        url, headers={"User-Agent": "RWS-Golfhoogte-Proxy/1.0"})
                    with urllib.request.urlopen(req, timeout=15) as r:
                        return json.loads(r.read().decode("utf-8"))

                with ThreadPoolExecutor(max_workers=2) as ex_m:
                    fm = ex_m.submit(_fetch_metar_raw)
                    ft = ex_m.submit(_fetch_taf_raw)
                    _wait_m([fm, ft], timeout=20)

                metars = fm.result() if fm.done() else []
                tafs   = ft.result() if ft.done() else []

                # Meest recente METAR eerst
                metars_sorted = sorted(
                    [m for m in (metars if isinstance(metars, list) else []) if m.get("rawOb")],
                    key=lambda m: m.get("reportTime", ""), reverse=True)
                taf_raw = ""
                for t in (tafs if isinstance(tafs, list) else []):
                    taf_raw = t.get("rawTAF") or t.get("tafText") or ""
                    if taf_raw:
                        break

                data = {
                    "icao":   icao,
                    "metars": [{"raw": m.get("rawOb", ""), "time": m.get("reportTime", ""),
                                "fc":  m.get("flightCategory", "")} for m in metars_sorted],
                    "taf":    taf_raw,
                }
                self.send_response(200)
                self._send_json(_safe_json(data).encode("utf-8"))
            except Exception as exc:
                print(f"[FOUT metar-detail] {exc}")
                body = json.dumps({"error": str(exc)}).encode()
                self.send_response(500)
                self.send_header("Content-Type",   "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_cors()
                self.end_headers()
                self.wfile.write(body)

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
                elif code.startswith("knmi.vis."):
                    # KNMI: in-memory ring buffer (gevuld bij elke fetch_knmi_data aanroep)
                    naam = code
                    cached = _knmi_cache
                    if cached:
                        for feat in cached.get("features", []):
                            if feat["properties"].get("code") == code:
                                naam = feat["properties"].get("naam", code)
                                break
                    data = get_knmi_temp_history(code)
                    data["naam"] = naam
                else:
                    # RWS: in-memory ring buffer (groeit elke 10 min)
                    data = get_rws_temp_history(code)
                self.send_response(200)
                self._send_json(_safe_json(data).encode("utf-8"))
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
            html_path = os.path.join(os.path.dirname(__file__), "index.html")
            body = None
            # Lokaal bestand heeft altijd voorrang (geen GitHub CDN-cache issues)
            try:
                with open(html_path, "rb") as f:
                    body = f.read()
            except FileNotFoundError:
                pass
            # Fallback: GitHub (alleen als lokaal bestand ontbreekt, bijv. op Render zonder deploy)
            if body is None:
                GITHUB_HTML = ("https://raw.githubusercontent.com/"
                               "awillemse-dev/golfhoogtes-noordzee/main/index.html")
                try:
                    req = urllib.request.Request(
                        GITHUB_HTML, headers={"User-Agent": "RWS-Golfhoogte-Proxy/1.0"})
                    with urllib.request.urlopen(req, timeout=8) as r:
                        body = r.read()
                except Exception:
                    pass
            if body is None:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"index.html niet gevonden")
                return
            self.send_response(200)
            self.send_header("Content-Type",   "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control",  "no-store")
            self.end_headers()
            self.wfile.write(body)

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
    print()

    def _refresh_waves():
        with _refresh_lock:
            _do_refresh()

    import socket as _socket
    _req_sem = threading.Semaphore(20)  # max 20 gelijktijdige requests

    class DualStackServer(ThreadingMixIn, HTTPServer):
        """Multi-threaded server op IPv4 + IPv6 — elke request in eigen thread."""
        daemon_threads  = True
        block_on_close  = False   # wacht niet op handler-threads bij afsluiten
        address_family  = _socket.AF_INET6
        def server_bind(self):
            self.socket.setsockopt(_socket.IPPROTO_IPV6, _socket.IPV6_V6ONLY, 0)
            super().server_bind()
        def process_request(self, request, client_address):
            """Begrens gelijktijdige requests zodat RAM niet overloopt."""
            if not _req_sem.acquire(blocking=False):
                # Server vol: verbinding direct sluiten
                try:
                    self.shutdown_request(request)
                except Exception:
                    pass
                return
            t = threading.Thread(
                target=self._handle_and_release,
                args=(request, client_address),
                daemon=True,
            )
            t.start()
        def _handle_and_release(self, request, client_address):
            try:
                self.finish_request(request, client_address)
            except Exception:
                self.handle_error(request, client_address)
            finally:
                self.shutdown_request(request)
                _req_sem.release()

    # Server direct starten zodat Safari meteen verbinding kan maken
    server = DualStackServer(("::", PORT), Handler)
    print(f"[SERVER] Luistert op http://localhost:{PORT}/ (IPv4 + IPv6)\n")

    # Prewarm + achtergrond-refresh in aparte thread — blokkeert de server niet
    def _background_loop():
        import gc as _gc

        # Fase 0: geschiedenis pre-seeden (blokkeerde vroeger het opstarten)
        print("[BSH] Geschiedenis pre-seeden vanuit GitHub…")
        try:
            _seed_bsh_history()
        except Exception as e:
            print(f"[BSH seed] Fout: {e}")
        print("[LaBouée] Geschiedenis pre-seeden vanuit GitHub…")
        try:
            _seed_labouee_history()
        except Exception as e:
            print(f"[LaBouée seed] Fout: {e}")
        print()

        # Fase 1: waves snel laden (RWS + BSH + CEFAS + LaBouée + NDBC + FMI, max 35s)
        print("[CACHE] Fase 1: waves ophalen (snel, zonder CDIP/SOCIB)…")
        try:
            _refresh_waves()
            print("[CACHE] Waves klaar.\n")
        except Exception as e:
            print(f"[CACHE] Waves fout: {e}\n")

        # Fase 2: CDIP + SOCIB + temp + wind + oceaanzicht — SEQUENTIEEL om RAM te sparen.
        # Elke taak spawnt intern al meerdere threads; parallel draaien verveelvoudigt dat.
        print("[CACHE] Fase 2: alle bronnen sequentieel laden…")
        _fase2_taken = (
            _refresh_temp_bg, get_knmi_data, _refresh_metar_bg,
            get_wind_data, _refresh_ocean_vis_bg, _refresh_socib_bg, _refresh_cdip_bg,
        )
        for _taak in _fase2_taken:
            try:
                _taak()
            except Exception as e:
                print(f"[CACHE] {_taak.__name__} mislukt: {e}")
            _gc.collect()   # direct na elke taak geheugen vrijgeven
        # Waves opnieuw cachen nu CDIP + SOCIB gevuld zijn
        try:
            _refresh_waves()
        except Exception as e:
            print(f"[CACHE] Waves na fase 2 mislukt: {e}")
        _gc.collect()
        print("[CACHE] Alles klaar.\n")

        # Daarna elke 9 minuten herhalen — ook sequentieel
        while True:
            time.sleep(9 * 60)
            print("[CACHE] Achtergrond-refresh gestart…")
            for _taak in _fase2_taken:
                try:
                    _taak()
                except Exception as e:
                    print(f"[CACHE] {_taak.__name__} mislukt: {e}")
                _gc.collect()   # direct na elke taak geheugen vrijgeven
            try:
                _refresh_waves()
            except Exception as e:
                print(f"[CACHE] Waves refresh mislukt: {e}")
            _gc.collect()
            print("[CACHE] Achtergrond-refresh klaar.")

    threading.Thread(target=_background_loop, daemon=True).start()
    threading.Thread(target=_bliksem_bg, daemon=True).start()

    def _self_ping():
        """Ping zichzelf elke 14 min zodat Render free tier niet slaap valt."""
        render_url = os.environ.get("RENDER_EXTERNAL_URL", "")
        time.sleep(60)  # wacht tot server klaar is
        while True:
            if render_url:
                try:
                    urllib.request.urlopen(f"{render_url}/api/ping", timeout=10)
                    print("[PING] Render self-ping OK")
                except Exception as e:
                    print(f"[PING] Self-ping fout: {e}")
            time.sleep(14 * 60)

    if os.environ.get("RENDER"):
        threading.Thread(target=_self_ping, daemon=True).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nProxy gestopt.")
