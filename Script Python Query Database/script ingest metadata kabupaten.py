import psycopg2
import csv

# Konfigurasi koneksi database
db_config = {
    "host": "localhost",
    "database": "Data_Warehouse_DDK",
    "user": "postgres",
    "password": "root",
    "port": "5432"
}

# Path ke file CSV
csv_file_path = r'D:\BMKG 2025\Project Sinta\GEO_KABUPATEN.csv'# Ganti dengan path file Anda

try:
    # Koneksi ke database
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    print("✅ Terhubung ke database.")

    # Buka file CSV dan baca isinya
    with open(csv_file_path, mode='r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        count = 0
        for row in reader:
            # Ambil data dari setiap baris CSV
            kabupaten_id = int(row['KABUPATEN_ID'])
            kabupaten_code_kemendagri = int(row['KABUPATEN_CODE'])
            kabupaten_name = row['KABUPATEN_NAME']
            propinsi_id = int(row['PROPINSI_ID'])

            # Buat query INSERT
            insert_query = """
                INSERT INTO dim_geo_kabupaten (kabupaten_id, kabupaten_code_kemendagri, kabupaten_name, propinsi_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (kabupaten_id) DO NOTHING;
            """
            cur.execute(insert_query, (kabupaten_id, kabupaten_code_kemendagri, kabupaten_name, propinsi_id))
            count += 1

    # Commit transaksi
    conn.commit()
    print(f"✅ Selesai. {count} baris berhasil di-insert ke dim_geo_kabupaten.")

except Exception as e:
    print(f"❌ Terjadi kesalahan: {e}")

finally:
    # Tutup koneksi
    if conn:
        cur.close()
        conn.close()
        print("🔌 Koneksi ke database ditutup.")
