import os
import csv
import re

# Ordnerpfade
input_dir = r"D:\CSVMerge\30_unmergeVirtualCSV"
output_dir = r"D:\CSVMerge\40_unmergedCSVs"
temp_dir = r"D:\CSVMerge\temp"
done_unmerge_path = os.path.join(temp_dir, "unmergeDone.txt")
encoding = "utf-8"

# Zielordner sicherstellen
os.makedirs(output_dir, exist_ok=True)
os.makedirs(temp_dir, exist_ok=True)

def extract_text_value(s: str) -> str:
    """
    Entfernt typische Excel-/CSV-Konstrukte:
    - ="0050" -> 0050
    - '0050   -> 0050 (Excel-Text-Präfix)
    - Trimmt Whitespace
    """
    if s is None:
        return ""
    s = str(s).strip()
    if s.startswith('="') and s.endswith('"') and len(s) >= 4:
        s = s[2:-1]
    if s.startswith("'") and len(s) > 1:
        s = s[1:]
    return s.strip()

def normalize_op_number(v: str) -> str:
    """
    Macht die Arbeitsvorgangsnummer wieder vierstellig (nur wenn es rein numerisch ist).
    Beispiele:
    - "50" -> "0050"
    - "0050" -> "0050"
    - "A010" -> "A010" (unverändert, da nicht rein numerisch)
    """
    v = extract_text_value(v)
    return v.zfill(4) if v.isdigit() else v

def safe_get(row, idx):
    """Sichere Index-Abfrage auf Zeilenliste."""
    return row[idx] if idx < len(row) else ""

def sanitize_filename(name: str, max_len: int = 150) -> str:
    """
    Entfernt unzulässige Zeichen für Dateinamen unter Windows und kürzt ggf.
    """
    name = extract_text_value(name)
    # Ersetze verbotene Zeichen
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    # Entferne führende/trailing Punkte/Spaces (Windows-Sonderfall)
    name = name.strip(' .')
    if not name:
        name = "unmerged"
    # Länge begrenzen
    if len(name) > max_len:
        name = name[:max_len]
    return name

# Alle CSV-Dateien im Eingangsordner verarbeiten
csv_files = [f for f in os.listdir(input_dir) if f.lower().endswith(".csv")]
if not csv_files:
    print("⚠️ Keine CSV-Dateien im Eingangsordner gefunden.")
    raise SystemExit

# Liste für wiederhergestellte Dateinamen (für unmergeDone.txt)
reconstructed_files = []

for file_name in csv_files:
    input_path = os.path.join(input_dir, file_name)

    try:
        with open(input_path, encoding=encoding, newline="") as f:
            reader = list(csv.reader(f, delimiter=";"))
    except Exception as e:
        print(f"❌ Datei {file_name} konnte nicht gelesen werden: {e}")
        continue

    if len(reader) < 6:
        print(f"⚠️ Datei {file_name} ist zu kurz – übersprungen.")
        # Ursprungsdatei optional löschen:
        try:
            os.remove(input_path)
            print(f"🗑️ Datei gelöscht (zu kurz): {input_path}")
        except Exception as e:
            print(f"❌ Fehler beim Löschen der Datei {input_path}: {e}")
        continue

    # Struktur: 4 Metazeilen, 1 Headerzeile, ab dann Datenzeilen
    meta_lines = reader[:4]
    header_line = reader[4]
    data_rows = reader[5:]

    # Zusatzspalten finden
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
            print(f"❌ Fehler beim Löschen der Datei {input_path}: {e}")
        continue

    # Datencontainer: bis zu 3 rekonstruierte Dateien
    output_data = {0: [], 1: [], 2: []}
    auftrag_names = ["", "", ""]  # Dateinamen-Basis aus FA-Spalten

    # Datenzeilen aufteilen (erste gefüllte FA-Spalte gewinnt)
    fa_indices = [f1_idx, f2_idx, f3_idx]

    for row in data_rows:
        # Robustheit: padde Zeile bis mindestens f1_idx Länge
        if len(row) <= f1_idx:
            row = row + [""] * (f1_idx + 1 - len(row))

        target_bucket = None
        bucket_name = ""

        for i, f_idx in enumerate(fa_indices):
            val = extract_text_value(safe_get(row, f_idx))
            if val:  # erste gefüllte FA-Spalte
                target_bucket = i
                bucket_name = val
                break

        if target_bucket is None:
            # Zeile ohne FA-Markierung – kann vorkommen; dann ignorieren oder einer Default-Gruppe zuordnen
            # Hier: ignorieren (alternativ: output_data[0].append(...))
            continue

        # Originaldaten links der ersten FA-Spalte
        orig_row = row[:f1_idx]

        # 1. Spalte (SAP-OPS-Nummer) vierstellig normalisieren
        if orig_row:
            orig_row[0] = normalize_op_number(orig_row[0])

        output_data[target_bucket].append(orig_row)

        # Dateiname merken/säubern
        if not auftrag_names[target_bucket]:
            auftrag_names[target_bucket] = sanitize_filename(bucket_name)

    # Schreiben der wiederhergestellten Einzeldateien
    any_output = False
    for i in range(3):
        rows = output_data[i]
        name = auftrag_names[i]
        if name and rows:
            output_file = os.path.join(output_dir, f"{name}.csv")

            # Header ohne Zusatzspalten (alles links von Fertigungsauftrag_1)
            base_header = header_line[:f1_idx]

            try:
                with open(output_file, "w", encoding=encoding, newline="") as f_out:
                    writer = csv.writer(f_out, delimiter=";")
                    # Metazeilen unverändert
                    writer.writerows(meta_lines)
                    # Ursprünglichen Header ohne Merge-Zusatzspalten
                    writer.writerow(base_header)
                    # Datenzeilen (erste Spalte bereits normalisiert)
                    writer.writerows(rows)

                reconstructed_files.append(name)
                any_output = True
                print(f"✅ Datei wiederhergestellt: {output_file}")
            except Exception as e:
                print(f"❌ Fehler beim Schreiben {output_file}: {e}")

    # Ursprungsdatei löschen (auch wenn keine Outputs entstanden sind, analog zu eurer bisherigen Logik)
    try:
        os.remove(input_path)
        print(f"🗑️ Datei gelöscht: {input_path}")
    except Exception as e:
        print(f"❌ Fehler beim Löschen der Datei {input_path}: {e}")

# done_unmerge.txt schreiben
try:
    with open(done_unmerge_path, "w", encoding="utf-8", newline="") as f_done:
        f_done.write("\n".join(reconstructed_files))
    print(f"📄 unmergeDone.txt erstellt: {done_unmerge_path}")
except Exception as e:
    print(f"❌ Fehler beim Schreiben von {done_unmerge_path}: {e}")
