import json
from collections import Counter

# --- Fungsi konversi aman ---
def safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None

def safe_int(val):
    try:
        return int(val)
    except (TypeError, ValueError):
        return None

# --- Path file JSON ---
json_file_path = r"D:\BMKG 2025\Project Sinta\response.json"

# --- Load file JSON ---
with open(json_file_path, 'r', encoding='utf-8') as f:
    json_data = json.load(f)

station_list = json_data.get("items", [])

print(f"🔍 Total data ditemukan: {len(station_list)}\n")

# --- Validasi ---
error_records = []
duplicate_check = []
for i, record in enumerate(station_list, 1):
    station_name = record.get("station_name")
    station_id = record.get("station_id")
    latitude = safe_float(record.get("current_latitude"))
    longitude = safe_float(record.get("current_longitude"))
    elevation = safe_float(record.get("station_elevation"))
    kabupaten_id = safe_int(record.get("kabupaten_id"))

    issue = []

    if not station_name:
        issue.append("Nama stasiun kosong")

    if not station_id:
        issue.append("station_id kosong")

    if latitude is None or longitude is None:
        issue.append("Koordinat tidak valid")

    if elevation is None:
        issue.append("Elevasi tidak valid")

    if kabupaten_id is None:
        issue.append("kabupaten_id tidak valid")

    duplicate_check.append(station_id)

    if issue:
        error_records.append({
            "record_ke": i,
            "station_name": station_name,
            "station_id": station_id,
            "masalah": issue
        })

# --- Cek duplikasi station_id ---
counter = Counter(duplicate_check)
duplicates = [sid for sid, count in counter.items() if count > 1 and sid is not None]

# --- Output hasil validasi ---
print(f"🚨 Data bermasalah: {len(error_records)}\n")
for rec in error_records:
    print(f"❗ Record ke-{rec['record_ke']} | {rec['station_name']} | Masalah: {rec['masalah']}")

if duplicates:
    print("\n⚠️ Ditemukan station_id duplikat:")
    for d in duplicates:
        print(f" - {d}")

print("\n✅ Validasi selesai.")
