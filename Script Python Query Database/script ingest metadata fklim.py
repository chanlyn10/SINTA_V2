import json
import psycopg2
from datetime import datetime
import os

# --- 1. Path File JSON Anda ---
# Pastikan ini adalah path dan nama file yang benar
json_file_path = os.path.join("D:", "BMKG 2025", "Project Sinta", "response.json") # <--- PASTIKAN INI

# --- 2. Baca Data JSON dari File ---
try:
    with open(json_file_path, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
    print(f"Data JSON berhasil dibaca dari: {json_file_path}")
except FileNotFoundError:
    print(f"Error: File tidak ditemukan di {json_file_path}")
    exit()
except json.JSONDecodeError:
    print(f"Error: Gagal mengurai JSON dari file {json_file_path}. Pastikan formatnya benar.")
    exit()
except Exception as e:
    print(f"Terjadi error saat membaca file JSON: {e}")
    exit()

# --- 3. Konfigurasi Koneksi Database Anda ---
db_config = {
    "host": "localhost",       # Ganti dengan host database Anda
    "database": "Data_Warehouse_DDK",
    "user": "postgres",   # Ganti dengan username database Anda
    "password": "root", # Ganti dengan password database Anda
    "port": "5432"             # Ganti dengan port PostgreSQL Anda jika berbeda
}

# --- 4. Fungsi untuk Insert Data ---
def insert_dim_station(data, config):
    conn = None
    cur = None
    try:
        # Menghubungkan ke database
        conn = psycopg2.connect(**config)
        cur = conn.cursor()

        # Menyiapkan data untuk insert
        # Pastikan mapping key JSON ke nama kolom tabel sesuai
        # Perhatikan konversi tipe data jika diperlukan (misalnya string ke float/int/boolean)
        data_values = (
            data.get("station_name"),                           # name
            float(data.get("current_latitude")),               # current_latitude
            float(data.get("current_longitude")),              # current_longitude
            float(data.get("station_elevation")),              # current_elevation_m
            data.get("station_wmo_id"),                        # wmo_id
            data.get("station_id"),                            # station_nk_id
            # data.get("is_fklim") akan diabaikan untuk is_active
            data.get("station_operating_hours"),               # operating_hours
            data.get("station_hour_start"),                    # hour_start
            data.get("station_hour_end"),                      # hour_end
            int(data.get("kabupaten_id")),                     # kabupaten_id
            data.get("timezone"),                              # time_zone

            # Kolom tabel yang tidak ada di JSON, akan diisi NULL atau sesuai default DB
            None, # kodebmkg_id
            None, # is_active (sesuai permintaan, diabaikan dari is_fklim)
            None, # status_reason
            None, # source_system
            None, # station_type
            None, # station_code
            None, # type_mkg
            datetime.now(), # valid_from (contoh: tanggal/waktu saat ini)
            None, # valid_to (contoh: diisi NULL atau tanggal jauh di masa depan)
            None, # kecamatan
            None, # kelurahan
            None  # station_address
        )

        # Query INSERT
        # Pastikan urutan kolom sesuai dengan `data_values` di atas
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

        # Eksekusi query
        cur.execute(insert_query, data_values)

        # Commit perubahan ke database
        conn.commit()
        print("Data berhasil dimasukkan ke tabel dim_stations.")

    except psycopg2.Error as e:
        print(f"Error saat memasukkan data: {e}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"Terjadi kesalahan tak terduga: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# --- 5. Jalankan Fungsi ---
if __name__ == "__main__":
    if json_data:
        insert_dim_station(json_data, db_config)