import os, csv, re, json, glob
from pathlib import Path

# ===== Pfade =====
base = Path(r"D:\CSVMerge")
input_dir  = base / "30_unmergeVirtualCSV"
output_dir = base / "40_unmergedCSVs"
temp_dir   = base / "temp"
done_unmerge_path = temp_dir / "unmergeDone.txt"

encoding = "utf-8"
delimiter = ';'

output_dir.mkdir(parents=True, exist_ok=True)
temp_dir.mkdir(parents=True, exist_ok=True)
input_dir.mkdir(parents=True, exist_ok=True)

# ===== Helfer (identisch zum Merge) =====
_ws_re = re.compile(r"\s+")
def extract_text_value(s: str) -> str:
    if s is None: return ""
    s = str(s).strip()
    if s.startswith('="') and s.endswith('"') and len(s) >= 4: s = s[2:-1]
    if s.startswith("'") and len(s) > 1: s = s[1:]
    return s.strip()
def normalize_op_number(v: str) -> str:
    v = extract_text_value(v)
    return v.zfill(4) if v.isdigit() else v
def to_float_or_none(s):
    if s is None: return None
    s = extract_text_value(s).replace(",", ".")
    try: return float(s)
    except ValueError: return None
def fmt_num(x: float) -> str:
    return str(int(x)) if float(x).is_integer() else str(x)
def norm_text(t: str) -> str:
    t = str(t).strip().upper()
    t = _ws_re.sub(" ", t)
    return t
def is_zeitbaustein_row(cells):
    return any(str(c).strip().upper() == "ZEITBAUSTEIN" for c in cells)
def rightmost_time_index(cells):
    units = {"IM", "MIN", "MINUTEN"}
    for i in range(len(cells)-1, 0, -1):
        if to_float_or_none(cells[i]) is not None and norm_text(cells[i-1]) in units:
            return i
    for i in range(len(cells)-1, -1, -1):
        if to_float_or_none(cells[i]) is not None:
            return i
    return None
def make_row_key(cells, sap_len):
    parts = []
    upto = min(sap_len, len(cells))
    for i in range(upto):
        raw = str(cells[i])
        if raw and to_float_or_none(raw) is None:
            parts.append(norm_text(raw))
    return "||".join(parts)
def sanitize_filename(name: str, max_len: int = 150) -> str:
    name = extract_text_value(name)
    name = re.sub(r'[<>:"/\\|?*]', "_", name).strip(" .")
    return (name or "unmerged")[:max_len]
def find_latest_sidecar(prefix: str):
    pats = [
        str(temp_dir / f"rawstore_{prefix}__*.json"),  # neu (mit merge_id)
        str(temp_dir / f"rawstore_{prefix}.json"),     # alt
    ]
    candidates = []
    for pat in pats:
        candidates.extend(glob.glob(pat))
    if not candidates:
        return None
    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return Path(candidates[0])

# ===== Start =====
csv_files = [f for f in os.listdir(input_dir) if f.lower().endswith(".csv")]
if not csv_files:
    raise SystemExit(f"⚠️ Keine CSV-Dateien in {input_dir}")

reconstructed_files = []

for file_name in csv_files:
    virt_path = input_dir / file_name

    sidecar_path = find_latest_sidecar(file_name)
    if not sidecar_path:
        raise SystemExit(f"❌ Sidecar nicht gefunden für {file_name} in {temp_dir}")

    with open(sidecar_path, "r", encoding="utf-8") as jf:
        sc = json.load(jf)
    side = sc.get("data", {})
    sap_len = sc.get("sap_header_len", None)
    reconstruct_names = sc.get("reconstruct_names", ["A1","A2","A3"])

    with open(virt_path, encoding=encoding, newline="") as f:
        rows = list(csv.reader(f, delimiter=delimiter))
    if len(rows) < 6:
        try: os.remove(virt_path)
        except Exception: pass
        continue

    meta_lines = rows[:4]
    header_line = rows[4]
    data_rows = rows[5:]

    try:
        f1_idx = header_line.index("Fertigungsauftrag_1")
    except ValueError:
        raise SystemExit("❌ Erwartete FA-Spalten nicht gefunden.")
    base_header = header_line[:f1_idx]
    if sap_len is None: sap_len = f1_idx

    out = {0: [], 1: [], 2: []}

    for r in data_rows:
        sap_row = r[:f1_idx]
        if sap_row:
            sap_row[0] = normalize_op_number(sap_row[0])  # OP vierstellig

        key = make_row_key(sap_row, sap_len)
        entry = side.get(key)
        if not entry:
            continue

        present = entry.get("present", {})
        time_idx = entry.get("time_idx", None)
        raw_map  = entry.get("raw", {})

        for b in (0,1,2):
            if not present.get(str(b), present.get(b, False)):
                continue
            row_copy = list(sap_row)
            if is_zeitbaustein_row(row_copy):
                raw_val = raw_map.get(str(b), raw_map.get(b, None))
                if raw_val is not None:
                    if time_idx is not None and time_idx < len(row_copy):
                        try:
                            rv = float(raw_val); row_copy[time_idx] = fmt_num(rv)
                        except Exception:
                            row_copy[time_idx] = str(raw_val)
                    else:
                        v_idx = rightmost_time_index(row_copy)
                        if v_idx is not None:
                            try:
                                rv = float(raw_val); row_copy[v_idx] = fmt_num(rv)
                            except Exception:
                                row_copy[v_idx] = str(raw_val)
            out[b].append(row_copy)

    # Schreiben mit Original-Namen aus Sidecar
    for i in (0,1,2):
        if out[i]:
            name = sanitize_filename(reconstruct_names[i]) + ".csv"
            out_path = output_dir / name
            with open(out_path, "w", encoding=encoding, newline="") as f_out:
                w = csv.writer(f_out, delimiter=delimiter)
                w.writerows(meta_lines)
                w.writerow(base_header)
                w.writerows(out[i])
            reconstructed_files.append(name)
            print(f"✅ Datei wiederhergestellt: {out_path}")

    try:
        os.remove(virt_path)
        print(f"🗑️ Datei gelöscht: {virt_path}")
    except Exception:
        pass

with open(done_unmerge_path, "w", encoding="utf-8", newline="") as f_done:
    f_done.write("\n".join(reconstructed_files))
print(f"📄 unmergeDone.txt erstellt: {done_unmerge_path}")
