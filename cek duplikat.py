import pandas as pd
import os

def analyze_csv_duplicates(file_path):
    """
    Membaca file CSV, menormalisasi nama kolom, dan menghitung
    data duplikat berdasarkan kombinasi 'wmo_id' dan 'data_timestamp'.

    Args:
        file_path (str): Path lengkap ke file CSV yang akan dianalisis.
    """
    # Periksa apakah file ada
    if not os.path.exists(file_path):
        print(f"❌ Error: File tidak ditemukan di path yang diberikan: {file_path}")
        return

    print(f"📥 Membaca file: {file_path}")

    try:
        # Baca file CSV
        # Parameter na_values membantu Pandas menginterpretasikan nilai-nilai tertentu
        # sebagai 'Not a Number' (NaN) atau nilai kosong.
        df = pd.read_csv(file_path, na_values=['-', 'N/A', 'NA', '', ' '])

        # Normalisasi nama kolom: menghapus spasi di awal/akhir dan mengubah ke huruf kecil
        df.columns = df.columns.str.strip().str.lower()

        # Pastikan kolom 'data_timestamp' dalam format datetime
        # 'errors='coerce'' akan mengubah nilai yang tidak bisa di-parse menjadi NaT (Not a Time)
        df["data_timestamp"] = pd.to_datetime(df["data_timestamp"], errors="coerce")

        # Hapus baris di mana 'data_timestamp' tidak valid (NaT)
        initial_rows_count = len(df)
        df = df.dropna(subset=["data_timestamp"])
        if len(df) < initial_rows_count:
            print(f"ℹ️ {initial_rows_count - len(df)} baris dihapus karena 'data_timestamp' tidak valid.")

        # Pastikan 'wmo_id' ada dan konversi ke integer
        # Hapus baris di mana 'wmo_id' kosong
        initial_rows_wmo = len(df)
        df = df.dropna(subset=["wmo_id"])
        if len(df) < initial_rows_wmo:
            print(f"ℹ️ {initial_rows_wmo - len(df)} baris dihapus karena 'wmo_id' kosong.")

        # Konversi 'wmo_id' ke tipe integer
        df["wmo_id"] = df["wmo_id"].astype(int)

        # --- Perhitungan Duplikat ---
        # Jumlah baris sebelum deduplikasi
        rows_before_dedup = len(df)

        # Buat DataFrame yang hanya berisi baris unik berdasarkan subset kolom
        df_deduplicated = df.drop_duplicates(subset=["wmo_id", "data_timestamp"])

        # Jumlah baris setelah deduplikasi
        rows_after_dedup = len(df_deduplicated)

        # Hitung jumlah duplikat
        duplicate_count = rows_before_dedup - rows_after_dedup

        print(f"\n--- Hasil Analisis Duplikasi CSV ---")
        print(f"Total baris di CSV (setelah pembersihan awal data_timestamp & wmo_id): {rows_before_dedup}")
        print(f"Jumlah baris unik berdasarkan (wmo_id, data_timestamp): {rows_after_dedup}")
        print(f"Jumlah baris duplikat yang ditemukan: {duplicate_count}")

        if duplicate_count > 0:
            print("\n⚠️ Ada data duplikat dalam CSV ini berdasarkan kombinasi 'wmo_id' dan 'data_timestamp'.")
            print("Berikut adalah contoh baris-baris yang terlibat dalam duplikasi (10 baris pertama):")
            # Menampilkan baris yang duplikat (termasuk duplikatnya)
            # 'keep=False' menandai semua duplikat sebagai True
            duplicates_df = df[df.duplicated(subset=["wmo_id", "data_timestamp"], keep=False)]
            # Urutkan untuk melihat duplikat berdekatan
            duplicates_df = duplicates_df.sort_values(by=["wmo_id", "data_timestamp"])
            print(duplicates_df.head(10).to_string()) # to_string() agar tidak terpotong
        else:
            print("\n✅ Tidak ada duplikat yang ditemukan dalam CSV ini berdasarkan kombinasi 'wmo_id' dan 'data_timestamp'.")

    except Exception as e:
        print(f"❌ Terjadi kesalahan saat memproses file CSV: {e}")

# --- Cara Menggunakan Script Ini ---
if __name__ == "__main__":
    # Ganti path ini dengan path lengkap ke file CSV Anda
    # Contoh: r"C:\Users\YourUser\Documents\data_fklim_sample.csv"
    csv_file_to_check = r"D:\BMKG 2025\Project Sinta\Data fklim\data_fklim_2008-01-01_to_2008-12-31_20250731_134854.csv"
    analyze_csv_duplicates(csv_file_to_check)