import os
import csv
import re
import json

# Pfade
input_dir = r"D:\CSVMerge\30_unmergeVirtualCSV"
output_dir = r"D:\CSVMerge\40_unmergedCSVs"
temp_dir = r"D:\CSVMerge\temp"
done_unmerge_path = os.path.join(temp_dir, "unmergeDone.txt")
encoding = "utf-8"
delimiter = ";"

os.makedirs(output_dir, exist_ok=True)
os.makedirs(temp_dir, exist_ok=True)

# ---------- Helfer ----------
def extract_text_value(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    if s.startswith('="') and s.endswith('"') and len(s) >= 4:
        s = s[2:-1]
    if s.startswith("'") and len(s) > 1:
        s = s[1:]
    return s.strip()

def normalize_op_number(v: str) -> str:
    v = extract_text_value(v)
    return v.zfill(4) if v.isdigit() else v

def to_float_or_none(s):
    if s is None:
        return None
    s = extract_text_value(s).replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None

def fmt_num(x: float) -> str:
    return str(int(x)) if float(x).is_integer() else str(x)

def is_zeitbaustein_row(cells):
    return any(str(c).strip().upper() == "ZEITBAUSTEIN" for c in cells)

def find_merkmal_code(cells):
    for c in cells:
        m = re.search(r"\bV0\d{3}(?:\.\d{3})?\b", str(c).strip(), flags=re.IGNORECASE)
        if m:
            return m.group(0).upper()
    return None

def make_row_key(cells, sap_len):
    parts = []
    for i in range(min(sap_len, len(cells))):
        t = str(cells[i]).strip()
        if t and to_float_or_none(t) is None:
            parts.append(t.upper())
    return "||".join(parts)

# ---------- Start ----------
csv_files = [f for f in os.listdir(input_dir) if f.lower().endswith(".csv")]
if not csv_files:
    print("⚠️ Keine CSV-Dateien im Eingangsordner gefunden.")
    raise SystemExit

reconstructed_files = []

for file_name in csv_files:
    input_path = os.path.join(input_dir, file_name)

    # Sidecar laden (muss zum Dateinamen passen)
    sidecar_path = os.path.join(temp_dir, f"rawstore_{file_name}.json")
    if not os.path.isfile(sidecar_path):
        print(f"❌ Sidecar nicht gefunden für {file_name}: {sidecar_path}")
        # Du kannst hier 'continue' machen oder ohne Restore arbeiten:
        # continue
    sidecar = None
    if os.path.isfile(sidecar_path):
        with open(sidecar_path, "r", encoding="utf-8") as jf:
            sidecar = json.load(jf)
    side_data = sidecar["data"] if sidecar else {}
    sap_len_from_sidecar = sidecar.get("sap_header_len", None) if sidecar else None

    # Merge-CSV lesen
    try:
        with open(input_path, encoding=encoding, newline="") as f:
            rows = list(csv.reader(f, delimiter=delimiter))
    except Exception as e:
        print(f"❌ Datei {file_name} konnte nicht gelesen werden: {e}")
        continue

    if len(rows) < 6:
        print(f"⚠️ Datei {file_name} ist zu kurz – übersprungen.")
        try:
            os.remove(input_path)
            print(f"🗑️ Datei gelöscht (zu kurz): {input_path}")
        except Exception as e:
            print(f"❌ Fehler beim Löschen: {e}")
        continue

    meta_lines = rows[:4]
    header_line = rows[4]
    data_rows = rows[5:]

    # Indizes: FA-Spalten
    try:
        f1_idx = header_line.index("Fertigungsauftrag_1")
        f2_idx = header_line.index("Fertigungsauftrag_2")
        f3_idx = header_line.index("Fertigungsauftrag_3")
    except ValueError:
        print(f"❌ Datei {file_name} hat nicht das erwartete Format – übersprungen.")
        try:
            os.remove(input_path)
            print(f"🗑️ Datei gelöscht (falsches Format): {input_path}")
        except Exception as e:
            print(f"❌ Fehler beim Löschen: {e}")
        continue

    sap_len = f1_idx if sap_len_from_sidecar is None else sap_len_from_sidecar
    base_header = header_line[:f1_idx]

    def row_bucket(row):
        for i, idx in enumerate([f1_idx, f2_idx, f3_idx]):
            if idx < len(row) and extract_text_value(row[idx]):
                return i
        return None

    output_data = {0: [], 1: [], 2: []}
    auftrag_names = ["", "", ""]

    for r in data_rows:
        if len(r) <= f1_idx:
            r = r + [""] * (f1_idx + 1 - len(r))

        b = row_bucket(r)
        if b is None:
            continue

        # Dateiname aus FA-Spalte
        fa_name = extract_text_value(r[[f1_idx, f2_idx, f3_idx][b]])
        if not auftrag_names[b]:
            name = re.sub(r'[<>:"/\\|?*]', "_", fa_name).strip(" .") or "unmerged"
            auftrag_names[b] = name

        # SAP-Teil
        orig_row = r[:f1_idx]

        # 1) OP-Nummer 4-stellig
        if orig_row:
            orig_row[0] = normalize_op_number(orig_row[0])

        # 2) Zeitbaustein exakt wiederherstellen (aus Sidecar)
        if side_data and is_zeitbaustein_row(orig_row):
            key = make_row_key(orig_row, sap_len)
            entry = side_data.get(key)
            if entry:
                # passenden Auftrag holen
                raw_map = entry.get("raw", {})
                raw_val = raw_map.get(str(b), None)  # falls als str gespeichert
                if raw_val is None:
                    raw_val = raw_map.get(b, None)
                if raw_val is not None:
                    # Zeitwertzelle (rechte numerische im SAP-Teil) setzen
                    val_idx = None
                    for i in range(len(orig_row) - 1, -1, -1):
                        if to_float_or_none(orig_row[i]) is not None:
                            val_idx = i
                            break
                    if val_idx is not None:
                        try:
                            rv = float(raw_val)
                            orig_row[val_idx] = fmt_num(rv)
                        except Exception:
                            # falls im Sidecar als String liegt, einfach übernehmen
                            orig_row[val_idx] = str(raw_val)

        output_data[b].append(orig_row)

    # Schreiben je Auftrag
    for i in range(3):
        name = auftrag_names[i]
        rows_out = output_data[i]
        if name and rows_out:
            out_path = os.path.join(output_dir, f"{name}.csv")
            with open(out_path, "w", encoding=encoding, newline="") as f_out:
                w = csv.writer(f_out, delimiter=delimiter)
                w.writerows(meta_lines)
                w.writerow(base_header)
                w.writerows(rows_out)
            reconstructed_files.append(name)
            print(f"✅ Datei wiederhergestellt: {out_path}")

    # Eingabe löschen
    try:
        os.remove(input_path)
        print(f"🗑️ Datei gelöscht: {input_path}")
    except Exception as e:
        print(f"❌ Fehler beim Löschen der Datei {input_path}: {e}")

# done_unmerge.txt
with open(done_unmerge_path, "w", encoding="utf-8", newline="") as f_done:
    f_done.write("\n".join(reconstructed_files))
print(f"📄 unmergeDone.txt erstellt: {done_unmerge_path}")
