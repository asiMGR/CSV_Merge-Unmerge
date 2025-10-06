import os
import csv
from datetime import datetime

input_dir = r"D:\CSVMerge\10_createVirtualCSV"
output_dir = r"D:\CSVMerge\20_VirtualCSVToSQL"
done_file_path = r"D:\CSVMerge\temp\done.txt"
encoding = "latin1"  # falls utf-8 fehlschlägt

# Sammellisten
all_rows = []
done_fertigungsauftraege = []
meta_lines_first = []
sap_header = []
timestamp = datetime.now().strftime("%Y%m%d_%H%M")

# CSV-Dateien sammeln
csv_files = sorted([f for f in os.listdir(input_dir) if f.lower().endswith('.csv')])

if not csv_files:
    print("⚠️ Keine CSV-Dateien im Quellordner gefunden.")
    exit()

for index, csv_file in enumerate(csv_files):
    file_path = os.path.join(input_dir, csv_file)

    with open(file_path, encoding=encoding, newline='') as f:
        reader = csv.reader(f, delimiter=';')
        lines = list(reader)

    meta_lines = lines[:4]
    sap_data_start = 0

    for i, row in enumerate(lines):
        if row and row[0].startswith("SAP-Arbeitsvorgang"):
            sap_header = row
            sap_data_start = i + 1
            break

    sap_data = lines[sap_data_start:]

    # Metadaten extrahieren
    meta = {r[0]: r[1] for r in meta_lines if len(r) >= 2}
    material_id = meta.get("Material-Id", "")
    arbeitsplan = meta.get("Arbeitsplan", "")
    fertigungsauftrag = meta.get("Fertigungsauftrag", "")
    msn = meta.get("MSN", "")
    done_fertigungsauftraege.append(fertigungsauftrag)

    # zusammengesetzter FA-String
    fa_value = f"{fertigungsauftrag}_{material_id}_{msn}_{timestamp}"

    # Spalten vorbereiten
    fert_col = [""] * 3
    sap_col = [""] * 3
    if index < 3:
        fert_col[index] = fa_value
        for row in sap_data:
            row_pad = row + [''] * (len(sap_header) - len(row))
            op_number = row_pad[0] if row_pad else ""
            sap_col[index] = op_number
            new_row = row_pad + fert_col + sap_col
            all_rows.append(new_row)
    else:
        # Ignoriere mehr als 3 Dateien
        print(f"⚠️ Mehr als 3 Dateien – '{csv_file}' wird nicht berücksichtigt.")

    if index == 0:
        meta_lines_first = meta_lines  # nur von der ersten Datei verwenden

    # Datei löschen
    os.remove(file_path)

# Finaler Header
extra_cols = [
    "Fertigungsauftrag_1", "Fertigungsauftrag_2", "Fertigungsauftrag_3",
    "OriginalSAP_Fertigungsauftrag_1", "OriginalSAP_Fertigungsauftrag_2", "OriginalSAP_Fertigungsauftrag_3"
]
final_header = sap_header + extra_cols

# Ziel-Dateiname nach erster CSV
output_filename = csv_files[0]
output_path = os.path.join(output_dir, output_filename)

# Schreiben der Datei
with open(output_path, mode='w', encoding='utf-8', newline='') as f:
    writer = csv.writer(f, delimiter=';')

    for meta in meta_lines_first:
        writer.writerow(meta + [""] * (len(final_header) - len(meta)))

    writer.writerow(final_header)
    writer.writerows(all_rows)

# done.txt schreiben
with open(done_file_path, 'w') as f:
    f.write("\n".join(done_fertigungsauftraege))

print(f"✅ Zusammengeführt in: {output_path}")
print(f"🧹 Quell-Dateien gelöscht.")
print(f"📄 done.txt geschrieben mit {len(done_fertigungsauftraege)} Aufträgen.")
