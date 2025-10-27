import os
import csv
import re
import json
from datetime import datetime

# === Pfade / Einstellungen ====================================================
input_dir = r"D:\CSVMerge\10_createVirtualCSV"
output_dir = r"D:\CSVMerge\20_VirtualCSVToSQL"
temp_dir = r"D:\CSVMerge\temp"
done_file_path = os.path.join(temp_dir, "done.txt")
encoding_read = "latin1"
encoding_write = "utf-8"
delimiter = ';'

os.makedirs(output_dir, exist_ok=True)
os.makedirs(temp_dir, exist_ok=True)

# === Helfer ===================================================================
def to_float_or_none(s):
    if s is None:
        return None
    s = str(s).strip().replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return None

def find_merkmal_code(cells):
    for c in cells:
        m = re.search(r'\bV0\d{3}(?:\.\d{3})?\b', str(c).strip(), flags=re.IGNORECASE)
        if m:
            return m.group(0).upper()
    return None

def is_zeitbaustein_row(cells):
    return any(str(c).strip().upper() == 'ZEITBAUSTEIN' for c in cells)

def rightmost_numeric_index(cells):
    for i in range(len(cells) - 1, -1, -1):
        if to_float_or_none(cells[i]) is not None:
            return i
    return None

def is_09xx(merkmal_code):
    if not merkmal_code:
        return False
    m = re.match(r'V0(\d{3})', merkmal_code, flags=re.IGNORECASE)
    return bool(m and 900 <= int(m.group(1)) <= 999)

def make_row_key(cells, sap_len):
    """
    Stabiler Schlüssel pro ZEITBAUSTEIN-Zeile:
    - nutzt alle NICHT-numerischen Zellen links vom ersten FA-Block (SAP-Teil)
    - Groß-/Kleinschreibung egal, Whitespace getrimmt
    Dadurch bleibt der Key identisch, auch wenn der Zahlenwert (rechts) summiert/maximiert wurde.
    """
    parts = []
    for i in range(min(sap_len, len(cells))):
        t = str(cells[i]).strip()
        if t and to_float_or_none(t) is None:
            parts.append(t.upper())
    return "||".join(parts)

# === Sammellisten =============================================================
all_rows = []
done_fertigungsauftraege = []
meta_lines_first = []
sap_header = []
timestamp = datetime.now().strftime("%Y%m%d_%H%M")

csv_files = sorted([f for f in os.listdir(input_dir) if f.lower().endswith('.csv')])
if not csv_files:
    print("⚠️ Keine CSV-Dateien im Quellordner gefunden.")
    raise SystemExit

staging_rows = []   # {row_pad, file_index, fa_value}
first_file_seen = False

# === Dateien einlesen & puffern ==============================================
for index, csv_file in enumerate(csv_files):
    file_path = os.path.join(input_dir, csv_file)

    with open(file_path, encoding=encoding_read, newline='') as f:
        lines = list(csv.reader(f, delimiter=delimiter))

    meta_lines = lines[:4]
    sap_data_start = 0
    local_header = []
    for i, row in enumerate(lines):
        if row and str(row[0]).startswith("SAP-Arbeitsvorgang"):
            local_header = row
            sap_data_start = i + 1
            break
    sap_data = lines[sap_data_start:]

    meta = {r[0]: r[1] for r in meta_lines if len(r) >= 2}
    material_id = meta.get("Material-Id", "")
    fertigungsauftrag = meta.get("Fertigungsauftrag", "")
    msn = meta.get("MSN", "")
    done_fertigungsauftraege.append(fertigungsauftrag)

    fa_value = f"{fertigungsauftrag}_{material_id}_{msn}_{timestamp}"

    if not first_file_seen:
        meta_lines_first = meta_lines
        sap_header = local_header[:]
        first_file_seen = True

    if index < 3:
        for row in sap_data:
            row_pad = row + [''] * (len(local_header) - len(row))
            staging_rows.append({'row_pad': row_pad, 'file_index': index, 'fa_value': fa_value})
    else:
        print(f"⚠️ Mehr als 3 Dateien – '{csv_file}' wird nicht berücksichtigt.")

    os.remove(file_path)

# === Zeitbausteine aggregieren & RAW je Auftrag in Sidecar sammeln ===========
agg = {}              # merkmal_code -> {'sum': float, 'max': float}
rows_info = []        # für spätere Ausgabe
raw_sidecar = {}      # row_key -> { "code": merkmal_code, "raw": {0: val0,1: val1,2: val2} }

sap_len = len(sap_header)

for item in staging_rows:
    rp = item['row_pad']
    idx = item['file_index']
    code = find_merkmal_code(rp)
    is_zeit = is_zeitbaustein_row(rp)
    v_idx = rightmost_numeric_index(rp)
    raw_val = to_float_or_none(rp[v_idx]) if (is_zeit and v_idx is not None) else None

    rows_info.append({
        'row_pad': rp,
        'file_index': idx,
        'fa_value': item['fa_value'],
        'code': code,
        'is_zeit': is_zeit,
        'val_idx': v_idx
    })

    if is_zeit and code and v_idx is not None:
        v = raw_val or 0.0
        agg.setdefault(code, {'sum': 0.0, 'max': 0.0})
        agg[code]['sum'] += v
        if v > agg[code]['max']:
            agg[code]['max'] = v

        # --- Sidecar: Rohwerte je Auftrag, pro Zeile (Key ohne Zahlenzelle) ---
        row_key = make_row_key(rp, sap_len)
        entry = raw_sidecar.setdefault(row_key, {"code": code, "raw": {}})
        entry["raw"][idx] = v

# === Header (ohne neue sichtbare Spalten!) ===================================
extra_cols = [
    "Fertigungsauftrag_1", "Fertigungsauftrag_2", "Fertigungsauftrag_3",
    "OriginalSAP_Fertigungsauftrag_1", "OriginalSAP_Fertigungsauftrag_2", "OriginalSAP_Fertigungsauftrag_3"
]
final_header = sap_header + extra_cols

# === Finale Zeilen schreiben ==================================================
for info in rows_info:
    rp = info['row_pad']
    idx = info['file_index']
    fa_value = info['fa_value']

    # ZEITBAUSTEIN: Aggregat sichtbar machen (Summe vs. Max)
    if info['is_zeit'] and info['code'] and info['val_idx'] is not None:
        a = agg.get(info['code'])
        if a:
            repl_val = a['max'] if is_09xx(info['code']) else a['sum']
            rp[info['val_idx']] = str(int(repl_val)) if float(repl_val).is_integer() else str(repl_val)

    # Zusatzspalten füllen
    fert_col = ["", "", ""]
    sap_col  = ["", "", ""]
    fert_col[idx] = fa_value
    sap_col[idx]  = rp[0] if rp else ""

    all_rows.append(rp + fert_col + sap_col)

# === Ausgeben ================================================================
output_filename = csv_files[0]
output_path = os.path.join(output_dir, output_filename)

with open(output_path, mode='w', encoding=encoding_write, newline='') as f:
    w = csv.writer(f, delimiter=delimiter)
    for meta in meta_lines_first:
        w.writerow(meta + [""] * (len(final_header) - len(meta)))
    w.writerow(final_header)
    w.writerows(all_rows)

# done.txt
with open(done_file_path, 'w', encoding='utf-8') as f:
    f.write("\n".join(done_fertigungsauftraege))

# --- Sidecar speichern (unsichtbar fürs PPS) ---------------------------------
sidecar_path = os.path.join(temp_dir, f"rawstore_{output_filename}.json")
with open(sidecar_path, "w", encoding="utf-8") as jf:
    json.dump({
        "created": timestamp,
        "output_file": output_filename,
        "sap_header_len": sap_len,
        "data": raw_sidecar
    }, jf, ensure_ascii=False)

print(f"✅ Zusammengeführt in: {output_path}")
print(f"🧩 Sidecar gespeichert: {sidecar_path}")
print(f"🧹 Quell-Dateien gelöscht.")
print(f"📄 done.txt geschrieben mit {len(done_fertigungsauftraege)} Aufträgen.")
