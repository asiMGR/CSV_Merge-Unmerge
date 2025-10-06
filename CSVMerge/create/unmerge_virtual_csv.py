import os
import csv

# Ordnerpfade
input_dir = r"D:\CSVMerge\30_unmergeVirtualCSV"
output_dir = r"D:\CSVMerge\40_unmergedCSVs"
temp_dir = r"D:\CSVMerge\temp"
done_unmerge_path = os.path.join(temp_dir, "unmergeDone.txt")
encoding = "utf-8"

# Zielordner sicherstellen
os.makedirs(output_dir, exist_ok=True)
os.makedirs(temp_dir, exist_ok=True)

# Alle CSV-Dateien im Eingangsordner verarbeiten
csv_files = [f for f in os.listdir(input_dir) if f.lower().endswith(".csv")]
if not csv_files:
    print("⚠️ Keine CSV-Dateien im Eingangsordner gefunden.")
    exit()

# Liste für wiederhergestellte Dateinamen
reconstructed_files = []

for file_name in csv_files:
    input_path = os.path.join(input_dir, file_name)

    with open(input_path, encoding=encoding, newline="") as f:
        reader = list(csv.reader(f, delimiter=";"))

    if len(reader) < 6:
        print(f"⚠️ Datei {file_name} ist zu kurz – übersprungen.")
        continue

    # Header und Zusatzspalten erkennen
    meta_lines = reader[:4]
    header_line = reader[4]
    data_rows = reader[5:]

    # Zusatzspalten prüfen
    try:
        f1_idx = header_line.index("Fertigungsauftrag_1")
        f2_idx = header_line.index("Fertigungsauftrag_2")
        f3_idx = header_line.index("Fertigungsauftrag_3")
    except ValueError:
        print(f"❌ Datei {file_name} hat nicht das erwartete Format – übersprungen.")
        continue

    output_data = {0: [], 1: [], 2: []}
    auftrag_names = ["", "", ""]

    # Datenzeilen aufteilen
    for row in data_rows:
        for i, f_idx in enumerate([f1_idx, f2_idx, f3_idx]):
            if row[f_idx]:
                auftrag_names[i] = row[f_idx]
                orig_row = row[:f1_idx]
                output_data[i].append(orig_row)
                break

    # Schreiben der wiederhergestellten Einzeldateien
    for i in range(3):
        if auftrag_names[i] and output_data[i]:
            output_file = os.path.join(output_dir, f"{auftrag_names[i]}.csv")
            with open(output_file, "w", encoding=encoding, newline="") as f_out:
                writer = csv.writer(f_out, delimiter=";")
                writer.writerows(meta_lines)
                writer.writerow(header_line[:f1_idx])
                writer.writerows(output_data[i])
            reconstructed_files.append(auftrag_names[i])
            print(f"✅ Datei wiederhergestellt: {output_file}")

    # Ursprungsdatei löschen
    try:
        os.remove(input_path)
        print(f"🗑️ Datei gelöscht: {input_path}")
    except Exception as e:
        print(f"❌ Fehler beim Löschen der Datei {input_path}: {e}")

# done_unmerge.txt schreiben
with open(done_unmerge_path, "w", encoding="utf-8") as f_done:
    f_done.write("\n".join(reconstructed_files))
# Datei erstellen 
print(f"📄 unmergeDone.txt erstellt: {done_unmerge_path}")
