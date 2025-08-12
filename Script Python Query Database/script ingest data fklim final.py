import requests
import pandas as pd
import psycopg2
from datetime import datetime
from requests.auth import HTTPBasicAuth
from psycopg2 import extras

# ====== Konfigurasi API dan Database ======
username_api = "integrasi"
password_api = "1ntegras1@2024"

db_config = {
    "host": "localhost",
    "database": "Data_Warehouse_DDK",
    "user": "postgres",
    "password": "root",
    "port": "5432"
}

url = "http://172.19.1.35:11091/db/bmkgsatu/@export_data"

params = {
    "type_name": "Fdih",
    "data_timestamp__gte": "2025-08-06T00:00",
    "data_timestamp__lte": "2025-08-10T23:59",
    "_size": "10000000",
    "fdih_type": "Fklim",
    "_metadata": "station_id,station_name,data_timestamp,alias_station_id,source_data,"
                 "m_0700ws[tbk_1c2m_0700],m_1300ws[tbk_1c2m_1300],m_1800ws[tbk_1c2m_1800],tbk_avg,"
                 "m_1800ws[t_max_1c2m],m_1300ws[t_min_1c2m],m_0700ws[rr_0700],m_0700ws[ss_8],m_0700ws[cu_khusus],"
                 "m_1800ws[pp_qfe_0000],m_0700ws[rh_1c2m_0700],m_1300ws[rh_1c2m_1300],m_1800ws[rh_1c2m_1800],rh_avg,"
                 "ff_avg_km_jm,wd_ff_max,ff_max,wd_cardinal"
}

# ====== Fungsi bantu ======
def safe_dict(x):
    """Mengembalikan dictionary jika input adalah dictionary, jika tidak mengembalikan dictionary kosong."""
    return x if isinstance(x, dict) else {}

def safe_float(x):
    """Mengonversi input ke float, menangani nilai 9999/8888, dan mengembalikan None jika gagal."""
    try:
        # Nilai 9999 dan 8888 sering digunakan sebagai placeholder untuk data kosong
        if x in [9999, '9999', 8888, '8888']:
            return None
        return float(x)
    except (ValueError, TypeError):
        return None

# ====== Ambil Data dari API ======
print(f"{datetime.now()} ⏳ Memulai pengambilan data dari API...")
try:
    response = requests.get(url, params=params, auth=HTTPBasicAuth(username_api, password_api), timeout=600)
    response.raise_for_status() # Akan memunculkan HTTPError jika status code 4xx/5xx
    data = response.json()
except requests.exceptions.RequestException as e:
    print(f"{datetime.now()} ❌ Gagal mengambil data dari API: {e}")
    exit()
except ValueError as e:
    print(f"{datetime.now()} ❌ Gagal mengonversi respons API ke JSON: {e}")
    exit()

# Cek struktur data
if isinstance(data, dict) and 'items' in data:
    df = pd.DataFrame(data['items'])
else:
    df = pd.DataFrame(data)

if df.empty:
    print(f"{datetime.now()} ⚠️ Data kosong, tidak ada yang diproses.")
    exit()

print(f"{datetime.now()} ✅ Jumlah data yang diambil dari API: {len(df)}")
print(f"{datetime.now()} ⏳ Memproses data dan memulai koneksi database...")

# Inisialisasi variabel untuk ringkasan proses
inserted = 0
skipped = 0
failed_insert = 0

try:
    # Koneksi Database
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cursor:
            # Pre-fetching WMO IDs dari dim_stations untuk efisiensi
            print(f"{datetime.now()} ⏳ Mengambil data WMO ID dari tabel dim_stations...")
            cursor.execute("SELECT wmo_id, station_sk_id FROM dim_stations")
            station_map = {str(wmo_id): station_sk_id for wmo_id, station_sk_id in cursor.fetchall()}
            print(f"{datetime.now()} ✅ Berhasil mengambil {len(station_map)} WMO ID.")

            # Mengkonversi alias_station_id yang mungkin berupa integer atau float dari API
            # Menjamin tipe string yang konsisten untuk pencarian di dictionary
            df['alias_station_id'] = df['alias_station_id'].astype(str).str.strip()

            # ====== Persiapan Batch Insert Data ======
            records_to_insert = []
            
            print(f"{datetime.now()} ⏳ Mempersiapkan data untuk batch insert...")
            for idx, row in df.iterrows():
                wmo_id = row['alias_station_id']
                station_sk_id = station_map.get(wmo_id)

                if station_sk_id is None:
                    # Log detail stasiun yang dilewatkan
                    print(f"{datetime.now()} ⚠️ Data untuk WMO ID '{wmo_id}' ({row.get('station_name', 'N/A')}) dilewatkan karena tidak ditemukan di tabel 'dim_stations'.")
                    skipped += 1
                    continue

                m_0700ws = safe_dict(row.get('m_0700ws'))
                m_1300ws = safe_dict(row.get('m_1300ws'))
                m_1800ws = safe_dict(row.get('m_1800ws'))

                values = (
                    station_sk_id,
                    wmo_id,
                    row.get('station_name', None),
                    row.get('data_timestamp', None),
                    safe_float(m_0700ws.get('tbk_1c2m_0700')),
                    safe_float(m_1300ws.get('tbk_1c2m_1300')),
                    safe_float(m_1800ws.get('tbk_1c2m_1800')),
                    safe_float(row.get('tbk_avg')),
                    safe_float(m_1800ws.get('t_max_1c2m')),
                    safe_float(m_1300ws.get('t_min_1c2m')),
                    safe_float(m_0700ws.get('rr_0700')),
                    safe_float(m_0700ws.get('ss_8')),
                    m_0700ws.get('cu_khusus', None),
                    safe_float(m_1800ws.get('pp_qfe_0000')),
                    safe_float(m_0700ws.get('rh_1c2m_0700')),
                    safe_float(m_1300ws.get('rh_1c2m_1300')),
                    safe_float(m_1800ws.get('rh_1c2m_1800')),
                    safe_float(row.get('rh_avg')),
                    safe_float(row.get('ff_avg_km_jm')),
                    row.get('wd_ff_max', None),
                    safe_float(row.get('ff_max')),
                    row.get('wd_cardinal', None),
                    row.get('source_data', None),
                    datetime.now()
                )
                records_to_insert.append(values)

            # ====== Proses Batch Insert ke Database ======
            insert_query = """
                INSERT INTO fact_data_fklim (
                    station_sk_id, wmo_id, name_station, data_timestamp,
                    temp_07lt_c, temp_13lt_c, temp_18lt_c, temp_avg_c,
                    temp_max_c, temp_min_c, rainfall_mm, sunshine_h,
                    weather_specific, pressure_mb, rel_humidity_07lt_pc,
                    rel_humidity_13lt_pc, rel_humidity_18lt_pc, rel_humidity_avg_pc,
                    wind_speed_avg_km_h, wind_dir_max, wind_speed_max_knots, wind_dir_cardinal,
                    source_data, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (wmo_id, data_timestamp) DO NOTHING;
            """
            
            if records_to_insert:
                print(f"{datetime.now()} ⏳ Memulai batch insert untuk {len(records_to_insert)} data...")
                extras.execute_batch(cursor, insert_query, records_to_insert, page_size=1000)
                conn.commit()
                inserted = len(records_to_insert)
            else:
                inserted = 0
            
            print(f"{datetime.now()} ✅ Batch insert selesai. {inserted} baris berhasil diproses.")

except psycopg2.Error as e:
    print(f"{datetime.now()} ❌ Error Database: {e}")
    failed_insert = len(records_to_insert)
    # Rollback otomatis karena menggunakan 'with' statement
finally:
    # ====== Summary Log ======
    print(f"{datetime.now()} ✅ Proses selesai.")
    print(f"{datetime.now()} ✅ Data berhasil di-insert : {inserted}")
    print(f"{datetime.now()} ⚠️ Data dilewatkan (WMO ID tidak ditemukan) : {skipped}")
    print(f"{datetime.now()} ❌ Data gagal insert : {failed_insert}")