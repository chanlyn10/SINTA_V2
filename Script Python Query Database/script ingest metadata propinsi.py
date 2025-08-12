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
csv_file_path = r'D:\BMKG 2025\Project Sinta\GEO_PROPINSI.csv'# Ganti dengan path file Anda

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
            propinsi_id = int(row['PROPINSI_ID'])
            propinsi_code_kemendagri = int(row['PROPINSI_CODE'])
            propinsi_name = row['PROPINSI_NAME']
            region_id = int(row['REGION_ID'])

            # Buat query INSERT
            insert_query = """
                INSERT INTO dim_geo_propinsi (propinsi_id, propinsi_code_kemendagri, propinsi_name, region_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (propinsi_id) DO NOTHING;
            """
            cur.execute(insert_query, (propinsi_id, propinsi_code_kemendagri, propinsi_name, region_id))
            count += 1

    # Commit transaksi
    conn.commit()
    print(f"✅ Selesai. {count} baris berhasil di-insert ke dim_geo_propinsi.")

except Exception as e:
    print(f"❌ Terjadi kesalahan: {e}")

finally:
    # Tutup koneksi
    if conn:
        cur.close()
        conn.close()
        print("🔌 Koneksi ke database ditutup.")
