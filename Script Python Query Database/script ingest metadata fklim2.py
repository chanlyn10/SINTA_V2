import json
import psycopg2
from datetime import datetime
import os
import logging

# --- Setup logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
try:
    with open(json_file_path, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
    logging.info(f"📂 Data JSON berhasil dibaca dari: {json_file_path}")
except FileNotFoundError:
    logging.error(f"❌ File tidak ditemukan: {json_file_path}")
    exit()
except json.JSONDecodeError:
    logging.error("❌ Format JSON tidak valid.")
    exit()
except Exception as e:
    logging.error(f"❌ Error saat membaca file JSON: {e}")
    exit()

# --- Konfigurasi koneksi PostgreSQL ---
db_config = {
    "host": "localhost",
    "database": "Data_Warehouse_DDK",
    "user": "postgres",
    "password": "root",
    "port": "5432"
}

# --- Fungsi insert data stasiun ---
def insert_dim_station(data, config):
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(**config)
        cur = conn.cursor()

        data_values = (
            data.get("station_name"),
            safe_float(data.get("current_latitude")),
            safe_float(data.get("current_longitude")),
            safe_float(data.get("station_elevation")),
            data.get("station_wmo_id"),
            data.get("station_id"),
            None,  # kodebmkg_id
            None,  # is_active
            data.get("station_operating_hours"),
            data.get("station_hour_start"),
            data.get("station_hour_end"),
            safe_int(data.get("kabupaten_id")),
            data.get("timezone"),
            None,  # status_reason
            None,  # source_system
            None,  # station_type
            None,  # station_code
            None,  # type_mkg
            datetime.now(),
            None,  # valid_to
            None,  # kecamatan
            None,  # kelurahan
            None   # station_address
        )

        insert_query = """
        INSERT INTO dim_stations (
            name,
            current_latitude,
            current_longitude,
            current_elevation_m,
            wmo_id,
            station_nk_id,
            kodebmkg_id,
            is_active,
            operating_hours,
            hour_start,
            hour_end,
            kabupaten_id,
            time_zone,
            status_reason,
            source_system,
            station_type,
            station_code,
            type_mkg,
            valid_from,
            valid_to,
            kecamatan,
            kelurahan,
            station_address
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        cur.execute(insert_query, data_values)
        conn.commit()
        logging.info(f"✅ Data stasiun '{data.get('station_name')}' berhasil dimasukkan.")

    except psycopg2.Error as e:
        logging.error(f"❌ Database error saat insert: {e}")
        if conn:
            conn.rollback()
    except Exception as e:
        logging.error(f"❌ Error tak terduga saat insert: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# --- Eksekusi utama ---
if __name__ == "__main__":
    # Ambil array dari field 'items'
    station_list = json_data.get("items", [])

    if isinstance(station_list, list):
        for i, record in enumerate(station_list, 1):
            logging.info(f"➡️ Memproses record ke-{i}")
            insert_dim_station(record, db_config)
    else:
        logging.warning("⚠️ Struktur JSON tidak dikenali. 'items' bukan list.")
    
    logging.info("✅ Seluruh proses insert selesai tanpa error fatal.")


