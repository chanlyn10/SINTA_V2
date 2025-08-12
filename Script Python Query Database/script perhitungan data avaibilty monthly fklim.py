import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import calendar

# === 1. Koneksi Database ===
db_config = {
    "host": "localhost",
    "database": "Data_Warehouse_DDK",
    "user": "postgres",
    "password": "root",
    "port": "5432"
}

engine = create_engine(
    f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
)

# === 2. Baca Data ===
df = pd.read_sql("SELECT * FROM fact_data_fklim", engine)
df['data_timestamp'] = pd.to_datetime(df['data_timestamp'])
df['bulan'] = df['data_timestamp'].dt.to_period('M')

# === 3. List Parameter (sesuai script awal) ===
params = [
    'temp_07lt_c', 'temp_13lt_c', 'temp_18lt_c', 'temp_avg_c',
    'temp_max_c', 'temp_min_c', 'rainfall_mm', 'sunshine_h',
    'weather_specific', 'pressure_mb', 'rel_humidity_07lt_pc',
    'rel_humidity_13lt_pc', 'rel_humidity_18lt_pc', 'rel_humidity_avg_pc',
    'wind_speed_avg_km_h', 'wind_dir_max', 'wind_speed_max_knots', 'wind_dir_cardinal'
]

# === 4. Tambahkan jumlah hari dalam bulan ===
df['days_in_month'] = df['bulan'].apply(lambda p: calendar.monthrange(p.year, p.month)[1])

# === 5. Hitung jumlah data valid per parameter ===
valid_counts = df.groupby(['station_sk_id', 'bulan'])[params].apply(lambda g: g.notnull().sum()).reset_index()

# === 6. Ambil jumlah hari per group ===
days_per_month = df.groupby(['station_sk_id', 'bulan'])['days_in_month'].max().reset_index()

# === 7. Gabungkan dan hitung persentase ===
result = valid_counts.merge(days_per_month, on=['station_sk_id', 'bulan'])

persen_cols = []
for col in params:
    new_col = f'persen_{col}'
    result[new_col] = (result[col] / result['days_in_month'] * 100).round(2)
    persen_cols.append(new_col)
    result = result.drop(columns=[col])

# === 8. Format bulan ke YYYYMM sebagai integer ===
result = result.drop(columns=['days_in_month'])
result = result[['station_sk_id', 'bulan'] + persen_cols]
result['time_month_id'] = result['bulan'].dt.strftime('%Y%m').astype(int)
result = result.drop(columns=['bulan'])

# === 9. Mapping kolom ke nama tabel database ===
rename_dict = {
    'persen_temp_07lt_c': 'availability_temp_07lt_c',
    'persen_temp_13lt_c': 'availability_temp_13lt_c',
    'persen_temp_18lt_c': 'availability_temp_18lt_c',
    'persen_temp_avg_c': 'availability_temp_avg_c',
    'persen_temp_max_c': 'availability_temp_max_c',
    'persen_temp_min_c': 'availability_temp_min_c',
    'persen_rainfall_mm': 'availability_rainfall_mm',
    'persen_sunshine_h': 'availability_sunshine_h',
    'persen_weather_specific': 'availability_weather_specific',
    'persen_pressure_mb': 'availability_pressure_mb',
    'persen_rel_humidity_07lt_pc': 'availability_rel_humidity_07lt_pc',
    'persen_rel_humidity_13lt_pc': 'availability_rel_humidity_13lt_pc',
    'persen_rel_humidity_18lt_pc': 'availability_rel_humidity_18lt_pc',
    'persen_rel_humidity_avg_pc': 'availability_rel_humidity_avg_pc',
    'persen_wind_speed_avg_km_h': 'availability_wind_speed_avg_kmjam',
    'persen_wind_dir_max': 'availability_wind_dir_max',
    'persen_wind_speed_max_knots': 'availability_wind_speed_max_knots',
    'persen_wind_dir_cardinal': 'availability_wind_dir_cardinal'
}
result = result.rename(columns=rename_dict)

# === 10. Hitung percentage_available (rata-rata semua availability_*) ===
availability_cols = [c for c in result.columns if c.startswith('availability_')]
result['percentage_available'] = result[availability_cols].mean(axis=1).round(2)

# === 11. Atur urutan kolom sesuai tabel ===
ordered_cols = ['station_sk_id', 'time_month_id', 'percentage_available'] + availability_cols
result = result[ordered_cols]

# === 12. Preview sebelum insert ===
print("🔍 Preview Data (5 baris pertama):")
print(result.head())
#print("\n📌 Kolom & tipe data:")
#print(result.dtypes)

# === 13. Insert ke database (comment dulu untuk cek) ===
result.to_sql('fact_bmkgsoft_fklim_availability_monthly', engine, if_exists='append', index=False)
print("✅ Data berhasil di-insert ke fact_bmkgsoft_fklim_availability_monthly")
