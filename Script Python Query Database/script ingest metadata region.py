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
csv_file_path = r'D:\BMKG 2025\Project Sinta\GEO_REGION.csv'# Ganti dengan path file Anda

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
            region_id = int(row['REGION_ID'])
            region_desc = row['REGION_DESC']
            

            # Buat query INSERT
            insert_query = """
                INSERT INTO dim_geo_region (region_id, region_desc)
                VALUES (%s, %s)
                ON CONFLICT (region_id) DO NOTHING;
            """
            cur.execute(insert_query, (region_id, region_desc))
            count += 1

    # Commit transaksi
    conn.commit()
    print(f"✅ Selesai. {count} baris berhasil di-insert ke dim_geo_region.")

except Exception as e:
    print(f"❌ Terjadi kesalahan: {e}")

finally:
    # Tutup koneksi
    if conn:
        cur.close()
        conn.close()
        print("🔌 Koneksi ke database ditutup.")
