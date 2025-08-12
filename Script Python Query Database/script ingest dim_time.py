import pandas as pd
import calendar
import numpy as np
from sqlalchemy import create_engine, MetaData
from sqlalchemy.dialects.postgresql import insert

# ====================
# Konfigurasi Database (Hardcode)
# ====================
DB_USER = "postgres"
DB_PASSWORD = "root"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "Data_Warehouse_DDK"

def convert_numpy_types(d):
    return {
        k: int(v) if isinstance(v, (np.integer, np.int64)) else
           str(v) if isinstance(v, (np.str_,)) else
           v for k, v in d.items()
    }

def get_pg_engine():
    return create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

def ingest_dim_time_tables(pg_engine):
    start_date = pd.to_datetime("1900-01-01")
    end_date = pd.to_datetime("2030-12-31")
    metadata = MetaData()
    metadata.reflect(pg_engine)

    with pg_engine.begin() as conn:  # Gunakan begin() agar auto-commit ✅
        # ==================== YEAR
        print("🗓️ Mengisi dim_time_year...")
        dim_time_year = metadata.tables['dim_time_year']
        years = pd.Series(range(start_date.year, end_date.year + 1))
        df_year = pd.DataFrame({
            'time_year_id': years,
            'year': years,
            'days_in_year': years.map(lambda y: 366 if calendar.isleap(y) else 365)
        })
        for _, row in df_year.iterrows():
            row_dict = convert_numpy_types(row.to_dict())
            stmt = insert(dim_time_year).values(row_dict).on_conflict_do_update(
                index_elements=['time_year_id'],
                set_={
                    'year': row_dict['year'],
                    'days_in_year': row_dict['days_in_year']
                }
            )
            conn.execute(stmt)
        print(f"✅ {len(df_year)} baris ke dim_time_year (upsert).")

        # ==================== MONTH
        print("📆 Mengisi dim_time_month...")
        dim_time_month = metadata.tables['dim_time_month']
        month_range = pd.date_range(start=start_date, end=end_date, freq='MS')
        month_name_map = {
            1: ("Januari", "January", "Jan", "Jan"),
            2: ("Februari", "February", "Feb", "Feb"),
            3: ("Maret", "March", "Mar", "Mar"),
            4: ("April", "April", "Apr", "Apr"),
            5: ("Mei", "May", "Mei", "May"),
            6: ("Juni", "June", "Jun", "Jun"),
            7: ("Juli", "July", "Jul", "Jul"),
            8: ("Agustus", "August", "Agu", "Aug"),
            9: ("September", "September", "Sep", "Sep"),
            10: ("Oktober", "October", "Okt", "Oct"),
            11: ("November", "November", "Nov", "Nov"),
            12: ("Desember", "December", "Des", "Dec"),
        }
        df_month = pd.DataFrame({
            'time_month_id': month_range.strftime('%Y%m').astype(int),
            'year': month_range.year,
            'month': month_range.month,
            'month_name_ina': month_range.month.map(lambda m: month_name_map[m][0]),
            'month_name_eng': month_range.month.map(lambda m: month_name_map[m][1]),
            'month_short_name_ina': month_range.month.map(lambda m: month_name_map[m][2]),
            'month_short_name_eng': month_range.month.map(lambda m: month_name_map[m][3]),
            'month_start_date': month_range.date,
            'month_end_date': month_range.map(lambda x: x + pd.offsets.MonthEnd(0)),
        })
        df_month['days_in_month'] = df_month['month_end_date'].dt.day
        for _, row in df_month.iterrows():
            row_dict = convert_numpy_types(row.to_dict())
            stmt = insert(dim_time_month).values(row_dict).on_conflict_do_update(
                index_elements=['time_month_id'],
                set_={k: row_dict[k] for k in row_dict if k != 'time_month_id'}
            )
            conn.execute(stmt)
        print(f"✅ {len(df_month)} baris ke dim_time_month (upsert).")

        # ==================== DAY
        print("📅 Mengisi dim_time_day...")
        dim_time_day = metadata.tables['dim_time_day']
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        df_day = pd.DataFrame({
            'time_day_id': dates.date,
            'time_month_id': dates.strftime('%Y%m').astype(int),
            'time_year_id': dates.year,
            'day_of_month': dates.day,
            'day_name_eng': dates.day_name()
        })
        day_map = {
            "Monday": "Senin", "Tuesday": "Selasa", "Wednesday": "Rabu",
            "Thursday": "Kamis", "Friday": "Jumat", "Saturday": "Sabtu", "Sunday": "Minggu"
        }
        df_day['day_name_ina'] = df_day['day_name_eng'].map(day_map)
        for _, row in df_day.iterrows():
            row_dict = convert_numpy_types(row.to_dict())
            stmt = insert(dim_time_day).values(row_dict).on_conflict_do_update(
                index_elements=['time_day_id'],
                set_={k: row_dict[k] for k in row_dict if k != 'time_day_id'}
            )
            conn.execute(stmt)
        print(f"✅ {len(df_day)} baris ke dim_time_day (upsert).")

    print("🎉 Semua tabel waktu berhasil di-upsert.")

# ================================
# MAIN
# ================================
if __name__ == "__main__":
    engine = get_pg_engine()
    ingest_dim_time_tables(engine)
