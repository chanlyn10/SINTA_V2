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

# Ambil dim_stations hanya sekali di luar loop untuk efisiensi
dim_stations = pd.read_sql("SELECT station_sk_id, wmo_id, name_stations FROM dim_stations", engine)
dim_stations = dim_stations.dropna(subset=["wmo_id"])
dim_stations["wmo_id"] = dim_stations["wmo_id"].astype(int)

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

# Ambil unique key yang sudah ada di database hanya sekali di luar loop
existing_keys = pd.read_sql("""
    SELECT wmo_id || '_' || TO_CHAR(data_timestamp, 'YYYY-MM-DD HH24:MI:SS') AS unique_key
    FROM fact_data_fklim
""", engine)
existing_keys_set = set(existing_keys["unique_key"]) # Konversi ke set untuk pencarian lebih cepat


print(f"--- Memproses {len(csv_files)} file CSV ---")

for file_name in sorted(csv_files): # Urutkan agar pemrosesan lebih konsisten
    file_path = os.path.join(folder_path, file_name)
    print(f"\n📥 Membaca file: {file_path}")

    try:
        # Baca file CSV
        df = pd.read_csv(file_path, na_values=['-', 'N/A', 'NA', '', ' '])
        df.columns = df.columns.str.strip().str.lower()

        # ---- REVISI: Ganti nama kolom dari CSV agar cocok dengan nama kolom database ----
        column_rename_map = {
            'temperature_07lt_c': 'temp_07lt_c',
            'temperature_13lt_c': 'temp_13lt_c',
            'temperature_18lt_c': 'temp_18lt_c',
            'temperature_avg_c': 'temp_avg_c',
            'temp_24h_min_c': 'temp_min_c',
            'temp_24h_max_c': 'temp_max_c',
            'rainfall_24h_mm': 'rainfall_mm',
            'sunshine_24h_h': 'sunshine_h',
            'qff_24h_mean_mb': 'pressure_mb',
            'wind_speed_24h_mean_ms': 'wind_speed_avg_km_h',
            'wind_speed_24h_max_ms': 'wind_speed_max_knots',
            'wind_dir_24h_max_deg': 'wind_dir_max',
            'wind_dir_24h_cardinal': 'wind_dir_cardinal'
        }
        df.rename(columns=column_rename_map, inplace=True)
        print("ℹ️ Kolom-kolom CSV telah diganti namanya agar cocok dengan skema database.")
        # print(f"Nama kolom setelah rename: {df.columns.tolist()}") # Uncomment for detailed debug

        # Pastikan data_timestamp dalam format datetime dan buang baris yang tidak valid
        df["data_timestamp"] = pd.to_datetime(df["data_timestamp"], errors="coerce")
        df = df.dropna(subset=["data_timestamp"])

        # Eksplisit konversi kolom numerik ke tipe numerik
        numeric_cols_to_convert = [
            'temp_07lt_c', 'temp_13lt_c', 'temp_18lt_c', 'temp_avg_c',
            'temp_max_c', 'temp_min_c', 'rainfall_mm', 'sunshine_h',
            'pressure_mb', 'rel_humidity_07lt_pc', 'rel_humidity_13lt_pc',
            'rel_humidity_18lt_pc', 'rel_humidity_avg_pc', 'wind_speed_avg_km_h',
            'wind_speed_max_knots'
        ]

        for col in numeric_cols_to_convert:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # --- CATATAN PENTING: Potensi Konversi Unit ---
        # Jika kolom wind_speed_avg_km_h dan wind_speed_max_knots
        # berasal dari kolom CSV yang dalam satuan m/s, Anda perlu melakukan konversi:
        # 1 m/s = 3.6 km/h
        # 1 m/s = 1.94384 knots
        if 'wind_speed_avg_km_h' in df.columns:
            df['wind_speed_avg_km_h'] = df['wind_speed_avg_km_h'] * 3.6
        if 'wind_speed_max_knots' in df.columns:
            df['wind_speed_max_knots'] = df['wind_speed_max_knots'] * 1.94384

        # Hapus baris duplikat dalam CSV berdasarkan kombinasi wmo_id + data_timestamp
        before_csv_dedup = len(df)
        df = df.drop_duplicates(subset=["wmo_id", "data_timestamp"])
        after_csv_dedup = len(df)
        print(f"ℹ️ {before_csv_dedup - after_csv_dedup} baris duplikat di CSV dihapus.")

        # Drop baris tanpa wmo_id dan konversi ke int
        df = df.dropna(subset=["wmo_id"])
        df["wmo_id"] = df["wmo_id"].astype(int)

        # Merge untuk mendapatkan station_sk_id dan name_stations dari dim_stations
        df = df.merge(dim_stations[['wmo_id', 'station_sk_id', 'name_stations']], on='wmo_id', how='left')
        df['name_station'] = df['name_stations']

        # Hapus kolom bantu jika tidak diperlukan
        df.drop(columns=['name_stations'], inplace=True, errors='ignore')

        # Isi updated_at
        df["updated_at"] = datetime.now()

        # Format datetime ke string yang spesifik dan konsisten dengan PostgreSQL
        df["data_timestamp_str"] = df["data_timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

        # Buat unique key di DataFrame
        df["unique_key"] = df["wmo_id"].astype(str) + "_" + df["data_timestamp_str"]

        # Filter hanya data baru yang belum ada di database menggunakan set existing_keys_set
        before_db_dedup = len(df)
        df = df[~df["unique_key"].isin(existing_keys_set)]
        after_db_dedup = len(df)
        print(f"✅ Data baru dari file ini untuk dimasukkan: {after_db_dedup}")
        if before_db_dedup > after_db_dedup:
            print(f"⚠️ {before_db_dedup - after_db_dedup} baris duplikat dari file ini tidak dimasukkan karena sudah ada di database.")

        # Hapus kolom bantu `data_timestamp_str` dan `unique_key` sebelum insert
        df.drop(columns=['data_timestamp_str', 'unique_key'], inplace=True, errors='ignore')

        # Tambahkan kolom kosong (None) jika ada kolom valid yang belum ada di DataFrame
        for col in valid_columns:
            if col not in df.columns:
                df[col] = None
        
        # Tambahkan kolom 'source_data'
        df['source_data'] = file_name # Menyimpan nama file sebagai sumber

        # Filter DataFrame hanya untuk kolom yang valid sesuai urutan tabel
        df = df[valid_columns]

        # Insert jika ada data baru
        if not df.empty:
            try:
                df.to_sql("fact_data_fklim", engine, if_exists="append", index=False)
                print(f"✅ Insert berhasil untuk file {file_name}.")
            except Exception as e:
                print(f"❌ Terjadi kesalahan saat insert data dari file {file_name}: {e}")
        else:
            print(f"ℹ️ Tidak ada data baru dari file {file_name} untuk disimpan.")

    except Exception as e:
        print(f"❌ Gagal memproses file {file_name}: {e}")

print("\n--- Selesai memproses semua file CSV ---")