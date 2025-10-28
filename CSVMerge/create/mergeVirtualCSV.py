import os, csv, re, json, hashlib
from pathlib import Path
from datetime import datetime

# ===== Pfade =====
base = Path(r"D:\CSVMerge")
input_dir  = base / "10_createVirtualCSV"
output_dir = base / "20_VirtualCSVToSQL"
temp_dir   = base / "temp"
done_file  = temp_dir / "done.txt"

encoding_read  = "latin1"
encoding_write = "utf-8"
delimiter = ';'

output_dir.mkdir(parents=True, exist_ok=True)
temp_dir.mkdir(parents=True, exist_ok=True)
input_dir.mkdir(parents=True, exist_ok=True)

# ===== Helfer (mit Unmerge identisch halten) =====
_ws_re = re.compile(r"\s+")
def to_float_or_none(s):
    if s is None: return None
    s = str(s).strip().replace(',', '.')
    try: return float(s)
    except ValueError: return None
def norm_text(t: str) -> str:
    t = str(t).strip().upper()
    t = _ws_re.sub(" ", t)
    return t
def find_merkmal_code(cells):
    for c in cells:
        m = re.search(r'\bV0\d{3}(?:\.\d{3})?\b', str(c).strip(), flags=re.IGNORECASE)
        if m: return m.group(0).upper()
    return None
def is_zeitbaustein_row(cells):
    return any(str(c).strip().upper() == 'ZEITBAUSTEIN' for c in cells)
def rightmost_time_index(cells):
    units = {"IM", "MIN", "MINUTEN"}
    for i in range(len(cells)-1, 0, -1):
        if to_float_or_none(cells[i]) is not None and norm_text(cells[i-1]) in units:
            return i
    for i in range(len(cells)-1, -1, -1):
        if to_float_or_none(cells[i]) is not None:
            return i
    return None
def is_09xx(code):
    if not code: return False
    m = re.match(r'V0(\d{3})', code, flags=re.IGNORECASE)
    return bool(m and 900 <= int(m.group(1)) <= 999)
def make_row_key(cells, sap_len):
    parts = []
    upto = min(sap_len, len(cells))
    for i in range(upto):
        raw = str(cells[i])
        if raw and to_float_or_none(raw) is None:
            parts.append(norm_text(raw))
    return "||".join(parts)

# ===== Einlesen =====
csv_files = sorted([f for f in os.listdir(input_dir) if f.lower().endswith(".csv")])
if not csv_files:
    raise SystemExit(f"⚠️ Keine CSV-Dateien in {input_dir}")

datasets = []
for name in csv_files[:3]:  # max 3
    p = input_dir / name
    with open(p, encoding=encoding_read, newline='') as f:
        rows = list(csv.reader(f, delimiter=delimiter))
    meta = rows[:4]
    hdr_i = next((i for i, r in enumerate(rows) if r and str(r[0]).startswith("SAP-Arbeitsvorgang")), None)
    if hdr_i is None:
        print(f"⚠️ {p.name}: kein 'SAP-Arbeitsvorgang' – übersprungen.")
        continue
    header = rows[hdr_i]
    data   = [r + [''] * (len(header) - len(r)) for r in rows[hdr_i+1:]]
    datasets.append(dict(file=p, meta=meta, header=header, rows=data))
if not datasets:
    raise SystemExit("⚠️ Keine verwertbaren CSVs.")

sap_header = datasets[0]['header'][:]
sap_len = len(sap_header)

# ===== IDs & Metadaten =====
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
h = hashlib.sha256("||".join([d['file'].name for d in datasets]).encode("utf-8")).hexdigest()[:8]
merge_id = f"{ts}_{h}"

reconstruct_names = []  # für Unmerge-Dateinamen
fa_ids = []            # nur Fertigungsauftrag-Nummer (für Anzeige in Spalten)
done_fa = []
for ds in datasets:
    meta_dict = {r[0]: r[1] for r in ds['meta'] if len(r) >= 2}
    material_id = meta_dict.get("Material-Id", "")
    fertigungsauftrag = meta_dict.get("Fertigungsauftrag", "")
    msn = meta_dict.get("MSN", "")
    done_fa.append(fertigungsauftrag)
    fa_ids.append(fertigungsauftrag)
    reconstruct_names.append(f"{fertigungsauftrag}_{material_id}_{msn}_{ts}")

# ===== Vereinigung + Aggregation =====
template_by_key = {}  # key -> exemplarische SAP-Zeile
present_by_key  = {}  # key -> set({0,1,2})
agg_by_key      = {}  # key -> {'sum':..., 'max':..., 'is_09xx':bool}
raw_sidecar     = {}  # key -> {'code':..., 'time_idx':int|None, 'raw':{i:val}, 'present':{i:True}}

for di, ds in enumerate(datasets):
    for r in ds['rows']:
        key = make_row_key(r, sap_len) or ("ROW::" + "||".join(norm_text(c) for c in r[:sap_len] if str(c).strip()))
        template_by_key.setdefault(key, list(r))
        present_by_key.setdefault(key, set()).add(di)
        if is_zeitbaustein_row(r):
            code  = find_merkmal_code(r)
            v_idx = rightmost_time_index(r)
            if v_idx is not None:
                v = to_float_or_none(r[v_idx]) or 0.0
                a = agg_by_key.setdefault(key, {'sum':0.0,'max':0.0,'is_09xx':is_09xx(code)})
                a['sum'] += v
                if v > a['max']: a['max'] = v
                a['is_09xx'] = a['is_09xx'] or is_09xx(code)
                sc = raw_sidecar.setdefault(key, {"code": code, "time_idx": v_idx, "raw": {}, "present": {}})
                if sc.get("time_idx") is None: sc["time_idx"] = v_idx
                sc["raw"][di] = v

# ===== Virtuelle Zeilen (eine pro key) =====
extra_cols = [
    "Fertigungsauftrag_1","Fertigungsauftrag_2","Fertigungsauftrag_3",
    "OriginalSAP_Fertigungsauftrag_1","OriginalSAP_Fertigungsauftrag_2","OriginalSAP_Fertigungsauftrag_3"
]
final_header = sap_header + extra_cols
virtual_rows = []

for key, tmpl in template_by_key.items():
    row = list(tmpl)
    # ZEITBAUSTEIN aggregiert einsetzen
    if is_zeitbaustein_row(row):
        a = agg_by_key.get(key)
        if a:
            v_idx = raw_sidecar.get(key, {}).get("time_idx", rightmost_time_index(row))
            if v_idx is not None:
                repl = a['max'] if a['is_09xx'] else a['sum']
                row[v_idx] = str(int(repl)) if float(repl).is_integer() else str(repl)

    # FA-Spalten: pro Zeile nur dort füllen, wo die Zeile im jeweiligen Auftrag vorkam
    fert_col = ["","",""]
    sap_col  = ["","",""]   # ungenutzt → leer lassen
    prs = present_by_key.get(key, set())
    for i in range(min(3, len(fa_ids))):
        if i in prs:
            fert_col[i] = fa_ids[i]

    virtual_rows.append(row + fert_col + sap_col)

# ===== Schreiben =====
output_filename = datasets[0]['file'].name  # Name der 1. CSV
output_path = output_dir / output_filename
with open(output_path, "w", encoding=encoding_write, newline="") as f:
    w = csv.writer(f, delimiter=delimiter)
    for m in datasets[0]['meta']:
        w.writerow(m + [""] * (len(final_header) - len(m)))
    w.writerow(final_header)
    w.writerows(virtual_rows)

with open(done_file, "w", encoding="utf-8") as df:
    df.write("\n".join(done_fa))

# Sidecar fertigstellen
for key, prs in present_by_key.items():
    entry = raw_sidecar.setdefault(key, {"code": None, "time_idx": None, "raw": {}, "present": {}})
    for i in prs:
        entry["present"][i] = True

payload = {
    "merge_id": merge_id,
    "created": ts,
    "output_file": output_filename,
    "sap_header_len": sap_len,
    "reconstruct_names": reconstruct_names,
    "fa_ids": fa_ids,  # rein informativ
    "data": raw_sidecar
}
sidecar_name = f"rawstore_{output_filename}__{merge_id}.json"
sidecar_path = temp_dir / sidecar_name
with open(sidecar_path, "w", encoding="utf-8") as jf:
    json.dump(payload, jf, ensure_ascii=False)

print(f"🧩 Sidecar gespeichert: {sidecar_path}")

# Quellen löschen
for ds in datasets:
    try: os.remove(ds['file'])
    except Exception: pass

print(f"✅ Virtuelle CSV erstellt: {output_path}  (Zeilen: {len(virtual_rows)})")
