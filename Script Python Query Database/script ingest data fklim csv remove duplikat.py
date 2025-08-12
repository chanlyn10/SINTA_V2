import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text # Import 'text' untuk SQL literal
import re
import os
from datetime import datetime

# === 1. Konfigurasi Database ===
db_config = {
    "host": "localhost",
    "database": "Data_Warehouse_DDK",
    "user": "postgres",
    "password": "root",
    "port": "5432"
}

# === 2. Koneksi Database ===
try:
    # Menggunakan psycopg2 sebagai driver
    engine = create_engine(f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")
    # Coba koneksi untuk memastikan
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
    print("✅ Koneksi database berhasil.")
except Exception as e:
    print(f"❌ Error saat koneksi database: {e}")
    exit()

# === 3. Path File CSV ===
# PASTIKAN PATH INI AKURAT KE LOKASI FILE CSV ANDA
file_path = r"D:\BMKG 2025\Project Sinta\Data fklim\Data FKLIM 2024.csv"
if not os.path.exists(file_path):
    raise FileNotFoundError(f"❌ File CSV tidak ditemukan di: {file_path}")

# === 4. Baca CSV dan Rename Kolom ===
try:
    # Baca CSV sebagai string untuk penanganan tipe data lebih fleksibel
    df = pd.read_csv(file_path, dtype=str)
    print("✅ File CSV berhasil dibaca.")
except Exception as e:
    print(f"❌ Error membaca CSV: {e}")
    exit()

# Pemetaan nama kolom dari CSV yang teridentifikasi ke nama kolom target database
column_mapping_csv_to_db = {
    'WMO_ID': 'wmo_id',
    'DATA_TIMESTAMP': 'data_timestamp', # Ini akan kita perbaiki formatnya
    'TEMPERATURE_07LT_C': 'temp_07lt_c',
    'TEMPERATURE_13LT_C': 'temp_13lt_c',
    'TEMPERATURE_18LT_C': 'temp_18lt_c',
    'TEMPERATURE_AVG_C': 'temp_avg_c',
    'TEMP_24H_MAX_C': 'temp_max_c',
    'TEMP_24H_MIN_C': 'temp_min_c',
    'RAINFALL_24H_MM': 'rainfall_mm',
    'SUNSHINE_24H_H': 'sunshine_h',
    'WEATHER_SPECIFIC': 'weather_specific',
    'QFF_24H_MEAN_MB': 'pressure_mb',
    'REL_HUMIDITY_07LT_PC': 'rel_humidity_07lt_pc',
    'REL_HUMIDITY_13LT_PC': 'rel_humidity_13lt_pc',
    'REL_HUMIDITY_18LT_PC': 'rel_humidity_18lt_pc',
    'REL_HUMIDITY_AVG_PC': 'rel_humidity_avg_pc',
    'WIND_SPEED_24H_MEAN_MS': 'wind_speed_avg_km_h',
    'WIND_DIR_24H_MAX_DEG': 'wind_dir_max',
    'WIND_SPEED_24H_MAX_MS': 'wind_speed_max_knots',
    'WIND_DIR_24H_CARDINAL': 'wind_dir_cardinal'
}

# Lakukan rename kolom
df = df.rename(columns=column_mapping_csv_to_db)

# Hanya pertahankan kolom yang ada dalam mapping
df = df[list(column_mapping_csv_to_db.values())].copy()
print("✅ Kolom DataFrame setelah rename dan filter sesuai target.")
# print(df.columns.tolist()) # Untuk debug: uncomment untuk melihat daftar kolom

# === 5. Pembersihan dan Konversi 'wmo_id' ===
df = df.dropna(subset=["wmo_id"]).copy() # Drop baris jika wmo_id null
try:
    df["wmo_id"] = pd.to_numeric(df["wmo_id"], errors='coerce').astype('Int64') # Konversi ke Int64 (nullable integer)
    df = df.dropna(subset=["wmo_id"]).copy() # Drop lagi jika ada wmo_id yang jadi NaN setelah konversi
    print("✅ Konversi 'wmo_id' berhasil.")
except Exception as e:
    print(f"❌ Error konversi 'wmo_id': {e}")
    exit()

# === 6. Join dengan dim_stations untuk 'station_sk_id' dan 'name_station' ===
try:
    dim_stations = pd.read_sql("SELECT station_sk_id, wmo_id, name_stations FROM dim_stations", engine)
    dim_stations = dim_stations.dropna(subset=["wmo_id"]).copy()
    dim_stations["wmo_id"] = pd.to_numeric(dim_stations["wmo_id"], errors='coerce').astype('Int64')
    dim_stations = dim_stations.dropna(subset=["wmo_id"]).copy()

    # Lakukan join
    df = df.merge(dim_stations[['wmo_id', 'station_sk_id', 'name_stations']], on='wmo_id', how='left')
    df['name_station'] = df['name_stations'] # Buat kolom 'name_station'
    df.drop(columns=['name_stations'], inplace=True, errors='ignore') # Hapus kolom bantu 'name_stations'

    print("✅ Join dengan 'dim_stations' berhasil.")
except Exception as e:
    print(f"❌ Error join dengan 'dim_stations': {e}")
    exit()

# Drop baris yang tidak memiliki station_sk_id setelah join (jika wmo_id tidak ditemukan di dim_stations)
df = df.dropna(subset=['station_sk_id']).copy()
df['station_sk_id'] = df['station_sk_id'].astype('Int64') # Pastikan tipe Int64 untuk kolom FK

# === 7. Tambah kolom 'updated_at' ===
df["updated_at"] = datetime.now()

# === 8. Pembersihan dan Konversi Tipe Data Parameter ===
# Daftar kolom yang seharusnya numerik di database final
numeric_cols = [
    'temp_07lt_c', 'temp_13lt_c', 'temp_18lt_c', 'temp_avg_c',
    'temp_max_c', 'temp_min_c', 'rainfall_mm', 'sunshine_h',
    'pressure_mb', 'rel_humidity_07lt_pc', 'rel_humidity_13lt_pc',
    'rel_humidity_18lt_pc', 'rel_humidity_avg_pc',
    'wind_speed_avg_km_h', 'wind_dir_max', 'wind_speed_max_knots',
]

for col in numeric_cols:
    if col in df.columns:
        # Hapus spasi dari string, tangani format ribuan (misal '1.234' jadi '1234')
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].apply(lambda x: re.sub(r'\.', '', x) if re.match(r'^\d{1,3}(\.\d{3})+$', x) else x)
        
        # Ganti nilai '9999' (sebagai string) dengan NaN
        df[col] = df[col].replace('9999', np.nan)
        
        # Konversi ke numerik, memaksa nilai non-angka menjadi NaN
        df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Pastikan tipe data float
        df[col] = df[col].astype(float)
    else:
        print(f"⚠️ Kolom numerik '{col}' tidak ditemukan di DataFrame. Menambahkan sebagai NaN.")
        df[col] = np.nan

# Tangani kolom tekstual/kategorikal
categorical_text_cols = ['weather_specific', 'wind_dir_cardinal']
for col in categorical_text_cols:
    if col in df.columns:
        # Ganti string 'nan' atau nilai kosong menjadi None (NULL di DB)
        df[col] = df[col].astype(str).replace('nan', None).replace('', None)
    else:
        print(f"⚠️ Kolom teks '{col}' tidak ditemukan di DataFrame. Menambahkan sebagai NULL.")
        df[col] = None

print("✅ Data setelah pembersihan dan konversi tipe data parameter.")
print(df.info()) # Output info DataFrame setelah konversi
print("\n")

# === 9. Filter tanggal ===
# PERBAIKAN DI SINI: MENENTUKAN FORMAT TANGGAL SECARA EKSPLISIT
df["data_timestamp"] = pd.to_datetime(df["data_timestamp"], format='%d-%m-%Y', errors="coerce")
df = df.dropna(subset=['data_timestamp']).copy() # Hapus baris dengan tanggal tidak valid

# Filter hanya data yang ingin di-insert/update (misal: 2024-01-01 hingga 2024-07-08)
df = df[(df["data_timestamp"] >= '2024-01-01') & (df["data_timestamp"] <= '2024-07-08')].copy()

print(f"✅ Data setelah filter tanggal (2024-01-01 hingga 2024-07-08): {len(df)} baris.")

# === 10. Pastikan Kolom Sesuai Tabel Database dan Urutan ===
# Daftar kolom target di tabel fact_data_fklim
final_target_columns = [
    'station_sk_id', 'wmo_id', 'name_station', 'data_timestamp',
    'temp_07lt_c', 'temp_13lt_c', 'temp_18lt_c', 'temp_avg_c',
    'temp_max_c', 'temp_min_c', 'rainfall_mm', 'sunshine_h',
    'weather_specific', 'pressure_mb', 'rel_humidity_07lt_pc',
    'rel_humidity_13lt_pc', 'rel_humidity_18lt_pc', 'rel_humidity_avg_pc',
    'wind_speed_avg_km_h', 'wind_dir_max', 'wind_speed_max_knots',
    'wind_dir_cardinal', 'updated_at'
]

# Tambahkan kolom yang mungkin hilang dengan nilai default (NaN/None)
for col in final_target_columns:
    if col not in df.columns:
        df[col] = np.nan # Default numerik
        if col in ['name_station', 'weather_specific', 'wind_dir_cardinal']:
            df[col] = None # Default objek/string
print(f"✅ Memastikan kolom sesuai dengan tabel database.")

# Urutkan kolom DataFrame sesuai urutan kolom target
df = df[final_target_columns]

# === 11. Final Konversi Tipe Data untuk to_sql ===
# Ini penting agar Pandas tahu bagaimana memetakan ke tipe data PostgreSQL
df['station_sk_id'] = df['station_sk_id'].astype('Int64')
df['wmo_id'] = df['wmo_id'].astype('Int64')
df['name_station'] = df['name_station'].astype(str).replace('<NA>', None) # Handle pandas.NA dari Int64 jika ada
df['data_timestamp'] = pd.to_datetime(df['data_timestamp']) # Pastikan datetime
df['updated_at'] = pd.to_datetime(df['updated_at'])

# Konversi kolom numerik yang mungkin perlu diubah ke tipe spesifik (misal dari Int64 ke float jika ada NaN)
for col in numeric_cols:
    if col in df.columns and df[col].dtype == 'float64':
        pass
    elif col in df.columns and df[col].dtype == 'Int64':
        pass

# Untuk kolom object (string), pastikan np.nan diubah ke None atau string kosong
for col in categorical_text_cols:
    if col in df.columns:
        df[col] = df[col].apply(lambda x: None if pd.isna(x) else str(x))


# === 12. Debug Sebelum Insert ===
print("\n--- Debug Sebelum Insert ---")
print("Kolom DataFrame sebelum insert:", df.columns.tolist())
print("Tipe data DataFrame sebelum insert:\n", df.dtypes)
print("5 Baris pertama df[['wmo_id','station_sk_id','name_station','data_timestamp','updated_at']]:\n", df[['wmo_id','station_sk_id','name_station','data_timestamp','updated_at']].head())
print("5 Baris pertama kolom numerik:\n", df[numeric_cols].head())
print("Jumlah NaN per kolom numerik:\n", df[numeric_cols].isnull().sum())
print("Jumlah total baris yang akan di-UPSERT:", len(df))
print("---------------------------\n")

# === 13. UPSERT ke PostgreSQL ===
if not df.empty:
    try:
        temp_table_name = "temp_fact_data_fklim_upsert" # Nama tabel temporer

        # Drop tabel temporer jika sudah ada (untuk memastikan bersih)
        with engine.connect() as connection:
            connection.execute(text(f"DROP TABLE IF EXISTS {temp_table_name};"))
            connection.commit()

        # Load data ke tabel temporer
        df.to_sql(temp_table_name, engine, if_exists='replace', index=False)
        print(f"✅ Data berhasil diload ke tabel temporer '{temp_table_name}'.")

        # Bangun daftar kolom untuk klausa UPDATE SET (kecualikan PK dan kolom lookup)
        cols_to_update = [
            'temp_07lt_c', 'temp_13lt_c', 'temp_18lt_c', 'temp_avg_c',
            'temp_max_c', 'temp_min_c', 'rainfall_mm', 'sunshine_h',
            'weather_specific', 'pressure_mb', 'rel_humidity_07lt_pc',
            'rel_humidity_13lt_pc', 'rel_humidity_18lt_pc', 'rel_humidity_avg_pc',
            'wind_speed_avg_km_h', 'wind_dir_max', 'wind_speed_max_knots',
            'wind_dir_cardinal', 'updated_at'
        ]
        
        # Pastikan hanya kolom yang ada di df yang masuk ke update_set_clauses
        update_set_clauses = ', '.join([f"{col} = EXCLUDED.{col}" for col in cols_to_update if col in df.columns])

        if not update_set_clauses:
            raise ValueError("Tidak ada kolom yang valid untuk di-update. Cek 'cols_to_update' list.")

        columns_for_insert_select = ', '.join(df.columns)

        # Query UPSERT menggunakan ON CONFLICT
        upsert_query = f"""
        INSERT INTO fact_data_fklim ({columns_for_insert_select})
        SELECT {columns_for_insert_select} FROM {temp_table_name}
        ON CONFLICT (wmo_id, data_timestamp) DO UPDATE SET
            {update_set_clauses};
        """
        
        # Eksekusi UPSERT
        with engine.connect() as connection:
            with connection.begin(): # Menggunakan begin() sebagai context manager untuk transaksi
                result = connection.execute(text(upsert_query))
                print(f"✅ {result.rowcount} baris di-UPSERT (inserted/updated) ke fact_data_fklim.")

        # Drop tabel temporer setelah UPSERT
        with engine.connect() as connection:
            connection.execute(text(f"DROP TABLE IF EXISTS {temp_table_name};"))
            connection.commit()
            print(f"✅ Tabel temporer '{temp_table_name}' berhasil dihapus.")

    except Exception as e:
        print(f"❌ Error saat UPSERT data: {e}")
        print("\nContoh data yang mungkin bermasalah (10 baris pertama di DataFrame):")
        print(df.head(10))
else:
    print("ℹ️ Tidak ada data untuk diproses setelah pembersihan dan filtering.")

# Tutup koneksi database
engine.dispose()
print("✅ Koneksi database ditutup.")

df["data_timestamp"] = pd.to_datetime(df["data_timestamp"], format='%d-%m-%Y', errors="coerce")
df = df.dropna(subset=['data_timestamp']).copy() # Hapus baris dengan tanggal tidak valid

print("\n--- Debug Konversi Tanggal ---")
print("Info DataFrame setelah konversi dan drop NaT:\n", df.info())
print("\n5 baris pertama data_timestamp setelah konversi:\n", df['data_timestamp'].head())
print("\nTanggal paling awal di DataFrame setelah konversi:", df['data_timestamp'].min())
print("Tanggal paling akhir di DataFrame setelah konversi:", df['data_timestamp'].max())
print("Jumlah baris setelah konversi dan drop NaT:", len(df))
print("---------------------------\n")