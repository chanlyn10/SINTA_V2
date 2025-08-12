import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# Konfigurasi koneksi database
db_config = {
    "host": "localhost",
    "database": "Data_Warehouse_DDK",
    "user": "postgres",
    "password": "root",
    "port": "5432"
}
engine = create_engine(f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")

# Path ke folder CSV
folder_path = r"D:\BMKG 2025\Project Sinta\Data fklim"
csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
if not csv_files:
    print("❌ Tidak ada file CSV ditemukan.")
    exit()

# Ambil file CSV terbaru berdasarkan nama file (seringkali mengandung tanggal/timestamp)
file_name = sorted(csv_files)[-1]
file_path = os.path.join(folder_path, file_name)
print(f"📥 Membaca file: {file_path}")

# Baca file CSV
# Tambahkan na_values untuk menangani representasi NULL/kosong di CSV
# Tambahkan nilai-nilai lain yang Anda temukan di CSV yang berarti data kosong
df = pd.read_csv(file_path, na_values=['-', 'N/A', 'NA', '', ' '])
df.columns = df.columns.str.strip().str.lower()

# ---- REVISI: Ganti nama kolom dari CSV agar cocok dengan nama kolom database ----
# PENTING: Mapping ini didasarkan pada NAMA KOLOM TERBARU yang Anda berikan dari output script.
# Jika nama kolom di CSV Anda berubah lagi, mapping ini perlu disesuaikan kembali.
column_rename_map = {
    'temperature_07lt_c': 'temp_07lt_c',
    'temperature_13lt_c': 'temp_13lt_c',
    'temperature_18lt_c': 'temp_18lt_c',
    'temperature_avg_c': 'temp_avg_c',
    'temp_24h_min_c': 'temp_min_c', # Asumsi ini adalah kolom min temperature
    'temp_24h_max_c': 'temp_max_c', # Asumsi ini adalah kolom max temperature
    'rainfall_24h_mm': 'rainfall_mm',
    'sunshine_24h_h': 'sunshine_h',
    'qff_24h_mean_mb': 'pressure_mb',
    # Kolom kelembaban ('rel_humidity_xx_pc') dan 'weather_specific'
    # sudah memiliki nama yang cocok di CSV baru Anda, jadi tidak perlu di-rename di sini.
    # Namun, kecepatan angin memerlukan perhatian khusus karena perbedaan unit.
    'wind_speed_24h_mean_ms': 'wind_speed_avg_km_h', # Perlu konversi unit: m/s ke km/h
    'wind_speed_24h_max_ms': 'wind_speed_max_knots', # Perlu konversi unit: m/s ke knots
    'wind_dir_24h_max_deg': 'wind_dir_max',
    'wind_dir_24h_cardinal': 'wind_dir_cardinal'
}
df.rename(columns=column_rename_map, inplace=True)
print("ℹ️ Kolom-kolom CSV telah diganti namanya agar cocok dengan skema database.")
print(f"Nama kolom setelah rename: {df.columns.tolist()}")

# Pastikan data_timestamp dalam format datetime dan buang baris yang tidak valid
df["data_timestamp"] = pd.to_datetime(df["data_timestamp"], errors="coerce")
df = df.dropna(subset=["data_timestamp"]) # Hapus baris dengan data_timestamp yang gagal di-parse

# Eksplisit konversi kolom numerik ke tipe numerik
# Gunakan daftar valid_columns untuk memastikan semua kolom yang diharapkan ada
# dan dikonversi jika ada di CSV.
numeric_cols_to_convert = [
    'temp_07lt_c', 'temp_13lt_c', 'temp_18lt_c', 'temp_avg_c',
    'temp_max_c', 'temp_min_c', 'rainfall_mm', 'sunshine_h',
    'pressure_mb', 'rel_humidity_07lt_pc', 'rel_humidity_13lt_pc',
    'rel_humidity_18lt_pc', 'rel_humidity_avg_pc', 'wind_speed_avg_km_h',
    'wind_speed_max_knots'
]

for col in numeric_cols_to_convert:
    if col in df.columns: # Hanya coba konversi jika kolom ada setelah rename
        df[col] = pd.to_numeric(df[col], errors='coerce')
    # Jika kolom tidak ada di CSV (dan tidak ada di column_rename_map), ia akan ditambahkan sebagai None nanti.

# ---- CATATAN PENTING: Potensi Konversi Unit ----
# Jika kolom wind_speed_avg_km_h dan wind_speed_max_knots
# berasal dari kolom CSV yang dalam satuan m/s, Anda perlu melakukan konversi:
# 1 m/s = 3.6 km/h
# 1 m/s = 1.94384 knots
# Contoh:
# if 'wind_speed_avg_km_h' in df.columns:
#    df['wind_speed_avg_km_h'] = df['wind_speed_avg_km_h'] * 3.6
# if 'wind_speed_max_knots' in df.columns:
#    df['wind_speed_max_knots'] = df['wind_speed_max_knots'] * 1.94384


# Hapus baris duplikat dalam CSV berdasarkan kombinasi wmo_id + data_timestamp
before_csv_dedup = len(df)
df = df.drop_duplicates(subset=["wmo_id", "data_timestamp"])
after_csv_dedup = len(df)

print(f"ℹ️ {before_csv_dedup - after_csv_dedup} baris duplikat di CSV dihapus sebelum insert.")

# Drop baris tanpa wmo_id dan konversi ke int
df = df.dropna(subset=["wmo_id"])
df["wmo_id"] = df["wmo_id"].astype(int)

# Ambil dim_stations dan ubah wmo_id ke integer
dim_stations = pd.read_sql("SELECT station_sk_id, wmo_id, name_stations FROM dim_stations", engine)
dim_stations = dim_stations.dropna(subset=["wmo_id"])
dim_stations["wmo_id"] = dim_stations["wmo_id"].astype(int)

# ---- REVISI: Merge dan penentuan name_station ----
# Lakukan merge untuk mendapatkan station_sk_id dan name_stations dari dim_stations
df = df.merge(dim_stations[['wmo_id', 'station_sk_id', 'name_stations']], on='wmo_id', how='left')

# PENTING: name_station harus diambil dari name_stations hasil merge
# karena kolom 'station_name' (atau sejenisnya) tidak lagi ada di CSV Anda yang baru
df['name_station'] = df['name_stations']


# Cek hasil merge sebelum insert (opsional, bisa dihapus di produksi)
print("--- Contoh hasil merge wmo_id dan station_sk_id ---")
# Baris ini seharusnya sekarang bisa dieksekusi karena name_station sudah dibuat
print(df[['wmo_id', 'station_sk_id', 'name_station']].drop_duplicates().head())
print("----------------------------------------------------")

# Hapus kolom bantu jika tidak diperlukan
# 'name_stations' sekarang adalah nama kolom yang berasal dari dim_stations, jadi harus dihapus setelah penyalinan
# Hapus juga kolom lain dari CSV yang tidak diperlukan di fact table (misal: latitude, longitude, station_id jika ada)
# Sesuaikan daftar ini dengan kolom yang tidak Anda inginkan di fact table
df.drop(columns=['name_stations'], inplace=True, errors='ignore') # Hapus name_stations setelah dipakai
# Contoh jika ada kolom lain yang tidak diinginkan dari CSV:
# df.drop(columns=['latitude', 'longitude', 'station_id'], inplace=True, errors='ignore')


# Isi updated_at
df["updated_at"] = datetime.now()

# --- BAGIAN PENTING: Penyesuaian Unique Key untuk Pencegahan Duplikasi ---
# Format datetime ke string yang spesifik dan konsisten dengan PostgreSQL
df["data_timestamp_str"] = df["data_timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

# Buat unique key di DataFrame
df["unique_key"] = df["wmo_id"].astype(str) + "_" + df["data_timestamp_str"]

# Ambil unique key dari tabel fact_data_fklim dengan format yang sama persis
existing_keys = pd.read_sql("""
    SELECT wmo_id || '_' || TO_CHAR(data_timestamp, 'YYYY-MM-DD HH24:MI:SS') AS unique_key
    FROM fact_data_fklim
""", engine)

# Filter hanya data baru yang belum ada di database
before_db_dedup = len(df)
df = df[~df["unique_key"].isin(existing_keys["unique_key"])]
after_db_dedup = len(df)
print(f"✅ Data baru untuk dimasukkan: {after_db_dedup}")
if before_db_dedup > after_db_dedup:
    print(f"⚠️ {before_db_dedup - after_db_dedup} baris duplikat tidak dimasukkan karena sudah ada di database.")

# Hapus kolom bantu `data_timestamp_str` dan `unique_key` sebelum insert
df.drop(columns=['data_timestamp_str', 'unique_key'], inplace=True, errors='ignore')


# Kolom yang valid sesuai skema tabel 'fact_data_fklim'
valid_columns = [
    'station_sk_id', 'wmo_id', 'name_station', 'data_timestamp',
    'temp_07lt_c', 'temp_13lt_c', 'temp_18lt_c', 'temp_avg_c',
    'temp_max_c', 'temp_min_c', 'rainfall_mm', 'sunshine_h',
    'weather_specific', 'pressure_mb', 'rel_humidity_07lt_pc',
    'rel_humidity_13lt_pc', 'rel_humidity_18lt_pc', 'rel_humidity_avg_pc',
    'wind_speed_avg_km_h', 'wind_dir_max', 'wind_speed_max_knots',
    'wind_dir_cardinal', 'source_data', 'updated_at'
]

# Tambahkan kolom kosong (None) jika ada kolom valid yang belum ada di DataFrame
# Ini tetap diperlukan untuk kolom yang memang tidak ada di CSV Anda (seperti wind_dir_cardinal jika tidak muncul dari mapping)
for col in valid_columns:
    if col not in df.columns:
        df[col] = None

# Filter DataFrame hanya untuk kolom yang valid sesuai urutan tabel
df = df[valid_columns]

# Insert jika ada data baru
if not df.empty:
    try:
        df.to_sql("fact_data_fklim", engine, if_exists="append", index=False)
        print("✅ Insert berhasil.")
    except Exception as e:
        print(f"❌ Terjadi kesalahan saat insert data: {e}")
else:
    print("ℹ️ Tidak ada data baru untuk disimpan.")