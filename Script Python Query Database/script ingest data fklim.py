import requests
import pandas as pd
import psycopg2
from datetime import datetime
from requests.auth import HTTPBasicAuth

# ==== Konfigurasi ====
username_api = "integrasi"
password_api = "1ntegras1@2024"

db_config = {
    "host": "localhost",
    "database": "Data_Warehouse_DDK",
    "user": "postgres",
    "password": "root",
    "port": "5432"
}

# ==== API Request ====
url = "https://bmkgsatu.bmkg.go.id/db/bmkgsatu/@export_data"

params = {
    "type_name": "Fdih",
    "station_id": "168,10,15,7,12,14,13,9,8,11261",
    "data_timestamp__gte": "2025-07-01T00:00",
    "data_timestamp__lte": "2025-07-01T23:59",
    "_size": "1000000",
    "fdih_type": "Fklim",
    "_metadata": "station_id,station_name,data_timestamp,alias_station_id,source_data,"
                 "m_0700ws[tbk_1c2m_0700],m_1300ws[tbk_1c2m_1300],m_1800ws[tbk_1c2m_1800],tbk_avg,"
                 "m_1800ws[t_max_1c2m],m_1300ws[t_min_1c2m],m_0700ws[rr_0700],m_0700ws[ss_8],m_0700ws[cu_khusus],"
                 "m_1800ws[pp_qfe_0000],m_0700ws[rh_1c2m_0700],m_1300ws[rh_1c2m_1300],m_1800ws[rh_1c2m_1800],rh_avg,"
                 "ff_avg_km_jm,wd_ff_max,ff_max,wd_cardinal"
}

response = requests.get(url, params=params, auth=HTTPBasicAuth(username_api, password_api))

if response.status_code != 200:
    print(f"❌ Gagal mengambil data API: {response.status_code}")
    exit()

data = response.json()
df = pd.DataFrame(data)

print(f"✅ Jumlah data yang diambil: {len(df)}")

# ==== Koneksi Database ====
conn = psycopg2.connect(**db_config)
cursor = conn.cursor()

inserted = 0
skipped = 0

for idx, row in df.iterrows():
    # Ambil station_sk_id
    cursor.execute("""
        SELECT station_sk_id FROM dim_stations WHERE wmo_id = %s LIMIT 1
    """, (str(row['alias_station_id']),))
    result = cursor.fetchone()
    
    if result:
        station_sk_id = result[0]
    else:
        print(f"⚠️  WMO ID {row['alias_station_id']} tidak ditemukan di dim_stations. Data dilewatkan.")
        skipped += 1
        continue

    # Ambil data dari nested JSON
    m_0700ws = row.get('m_0700ws', {})
    m_1300ws = row.get('m_1300ws', {})
    m_1800ws = row.get('m_1800ws', {})

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
    """

    values = (
        station_sk_id,
        str(row['alias_station_id']),
        row['station_name'],
        row['data_timestamp'],
        m_0700ws.get('tbk_1c2m_0700', None),
        m_1300ws.get('tbk_1c2m_1300', None),
        m_1800ws.get('tbk_1c2m_1800', None),
        row.get('tbk_avg', None),
        m_1800ws.get('t_max_1c2m', None),
        m_1300ws.get('t_min_1c2m', None),
        float(m_0700ws.get('rr_0700', None)) if m_0700ws.get('rr_0700', None) not in [None, ""] else None,
        m_0700ws.get('ss_8', None),
        m_0700ws.get('cu_khusus', None),
        m_1800ws.get('pp_qfe_0000', None),
        m_0700ws.get('rh_1c2m_0700', None),
        m_1300ws.get('rh_1c2m_1300', None),
        m_1800ws.get('rh_1c2m_1800', None),
        row.get('rh_avg', None),
        row.get('ff_avg_km_jm', None),
        row.get('wd_ff_max', None),
        row.get('ff_max', None),
        row.get('wd_cardinal', None),
        row.get('source_data', None),
        datetime.now()
    )

    cursor.execute(insert_query, values)
    inserted += 1

# Commit & close
conn.commit()
cursor.close()
conn.close()

print(f"✅ Insert selesai. Data masuk: {inserted}, Data dilewatkan: {skipped}")
