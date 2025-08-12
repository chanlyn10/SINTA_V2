import oracledb
import csv
import os
from datetime import datetime

# --- Konfigurasi Database ---
DB_HOST = '172.19.2.67'
DB_PORT = 1521
DB_SERVICE_NAME = 'ORCL'
DB_USER = 'manajemen'
DB_PASSWORD = 'manajemen123'

# --- Parameter tanggal ---
START_DATE = '2005-01-01'
END_DATE   = '2005-12-31'

# --- Folder output ---
OUTPUT_DIR = r"D:\BMKG 2025\Project Sinta\Data fklim"
os.makedirs(OUTPUT_DIR, exist_ok=True)  # Pastikan folder ada

# --- Query dengan filter tanggal ---
SQL_QUERY = f"""
SELECT *
FROM BMKGDBA.VIEW_FKLIM
WHERE DATA_TIMESTAMP BETWEEN TO_DATE('{START_DATE}','YYYY-MM-DD')
                         AND TO_DATE('{END_DATE}','YYYY-MM-DD')
ORDER BY WMO_ID, DATA_TIMESTAMP
"""

try:
    dsn = f"{DB_HOST}:{DB_PORT}/{DB_SERVICE_NAME}"
    print(f"Mencoba koneksi ke Oracle di {dsn}...")

    connection = oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=dsn)
    print("✅ Koneksi berhasil!")

    cursor = connection.cursor()
    print(f"Menjalankan query untuk range {START_DATE} sampai {END_DATE}...")
    cursor.execute(SQL_QUERY)

    column_names = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    print(f"✅ Total {len(rows)} baris data ditemukan.")

    # --- Path file CSV ---
    filename = f"data_fklim_{START_DATE}_to_{END_DATE}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    file_path = os.path.join(OUTPUT_DIR, filename)

    # --- Simpan ke CSV ---
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(column_names)
        writer.writerows(rows)

    print(f"✅ Data berhasil disimpan ke file: {file_path}")

except oracledb.DatabaseError as e:
    error_obj, = e.args
    print(f"❌ Terjadi kesalahan Oracle: {error_obj.message}")
    print(f"Kode Error: {error_obj.code}")

finally:
    if 'cursor' in locals():
        cursor.close()
        print("Kursor ditutup.")
    if 'connection' in locals():
        connection.close()
        print("Koneksi database ditutup.")
