# app.py
from flask import Flask, render_template, jsonify, request
import psycopg2
from datetime import datetime, timedelta, date
from calendar import monthrange
import os

app = Flask(__name__)

# === Koneksi Database ===
db_config = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "database": os.environ.get("DB_NAME", "Data_Warehouse_DDK"),
    "user": os.environ.get("DB_USER", "postgres"),
    "password": os.environ.get("DB_PASSWORD", "root"),
    "port": os.environ.get("DB_PORT", "5432")
}

def get_db_connection():
    try:
        return psycopg2.connect(**db_config)
    except psycopg2.Error as e:
        print(f"Error connecting to DB: {e}")
        return None

# ======================== DASHBOARD (Dipertahankan) ========================

def get_time_month_id(date_obj):
    return int(date_obj.strftime('%Y%m'))

def get_start_month_id(period_months):
    today = datetime.now()
    start_date = today.replace(day=1) - timedelta(days=30 * (period_months - 1))
    return get_time_month_id(start_date)

def get_dynamic_data(period_months):
    if period_months == 0:
        return [], {}
    conn = get_db_connection()
    if conn is None:
        return None, None

    current_month_id = get_time_month_id(datetime.now())
    start_month_id = get_start_month_id(period_months)

    data_grafik = {}
    data_persentase_terkecil = []
    try:
        cur = conn.cursor()

        # Stasiun terkecil
        cur.execute("""
            SELECT s.name_stations, s.station_sk_id, AVG(f.percentage_available) as avg_percentage
            FROM fact_bmkgsoft_fklim_availability_monthly AS f
            JOIN dim_stations AS s ON f.station_sk_id = s.station_sk_id
            WHERE f.time_month_id BETWEEN %s AND %s
            GROUP BY s.name_stations, s.station_sk_id
            ORDER BY avg_percentage ASC
            LIMIT 3;
        """, (start_month_id, current_month_id))
        for row in cur.fetchall():
            data_persentase_terkecil.append({
                "persentase": f"{row[2]:.2f}%",
                "id": row[1],
                "nama": row[0]
            })

        # Grafik per region
        region_ids = [1, 2, 3, 4, 5]
        for region_id in region_ids:
            cur.execute("""
                SELECT s.name_stations, AVG(f.percentage_available)
                FROM fact_bmkgsoft_fklim_availability_monthly AS f
                JOIN dim_stations AS s ON f.station_sk_id = s.station_sk_id
                JOIN dim_geo_kabupaten AS k ON s.kabupaten_id = k.kabupaten_id
                JOIN dim_geo_propinsi AS p ON k.propinsi_id = p.propinsi_id
                WHERE p.region_id = %s AND f.time_month_id BETWEEN %s AND %s
                GROUP BY s.name_stations
                ORDER BY AVG(f.percentage_available) DESC;
            """, (region_id, start_month_id, current_month_id))
            rows = cur.fetchall()
            data_grafik[region_id] = {
                'labels': [row[0] for row in rows],
                'data': [row[1] for row in rows]
            }
        cur.close()
    finally:
        conn.close()
    return data_persentase_terkecil, data_grafik

def get_fklim_data(period_days):
    conn = get_db_connection()
    if conn is None:
        return None, None
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=period_days)
    data_grafik = {}
    data_persentase_terkecil = []

    param_columns = [
        'temp_07lt_c', 'temp_13lt_c', 'temp_18lt_c', 'temp_avg_c', 'temp_max_c', 'temp_min_c',
        'rainfall_mm', 'sunshine_h', 'weather_specific', 'pressure_mb', 'rel_humidity_07lt_pc',
        'rel_humidity_13lt_pc', 'rel_humidity_18lt_pc', 'rel_humidity_avg_pc', 'wind_speed_avg_km_h',
        'wind_dir_max', 'wind_speed_max_knots', 'wind_dir_cardinal'
    ]
    availability_sum = " + ".join([f"CASE WHEN {col} IS NOT NULL AND {col} = {col} THEN 1 ELSE 0 END" for col in param_columns])

    try:
        cur = conn.cursor()

        # Terkecil
        cur.execute(f"""
            SELECT s.name_stations, s.station_sk_id, AVG(({availability_sum}) / 18.0) * 100 as avg_percentage
            FROM fact_data_fklim AS f
            JOIN dim_stations AS s ON f.station_sk_id = s.station_sk_id
            WHERE f.data_timestamp BETWEEN %s AND %s
            GROUP BY s.name_stations, s.station_sk_id
            ORDER BY avg_percentage ASC
            LIMIT 3;
        """, (start_date, end_date))
        for row in cur.fetchall():
            data_persentase_terkecil.append({
                "persentase": f"{row[2]:.2f}%",
                "id": row[1],
                "nama": row[0]
            })

        # Grafik per region
        region_ids = [1, 2, 3, 4, 5]
        for region_id in region_ids:
            cur.execute(f"""
                SELECT s.name_stations, AVG(({availability_sum}) / 18.0) * 100
                FROM fact_data_fklim AS f
                JOIN dim_stations AS s ON f.station_sk_id = s.station_sk_id
                JOIN dim_geo_kabupaten AS k ON s.kabupaten_id = k.kabupaten_id
                JOIN dim_geo_propinsi AS p ON k.propinsi_id = p.propinsi_id
                WHERE p.region_id = %s AND f.data_timestamp BETWEEN %s AND %s
                GROUP BY s.name_stations
                ORDER BY AVG(({availability_sum}) / 18.0) * 100 DESC;
            """, (region_id, start_date, end_date))
            rows = cur.fetchall()
            data_grafik[region_id] = {
                'labels': [row[0] for row in rows],
                'data': [row[1] for row in rows]
            }
        cur.close()
    finally:
        conn.close()
    return data_persentase_terkecil, data_grafik

def get_available_years():
    conn = get_db_connection()
    years = []
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT DISTINCT SUBSTRING(time_month_id::text, 1, 4)
                FROM fact_bmkgsoft_fklim_availability_monthly
                ORDER BY 1 DESC;
            """)
            years = [row[0] for row in cur.fetchall()]
            cur.close()
        finally:
            conn.close()
    return years

@app.route('/')
def index():
    data_persentase_terkecil, data_grafik = get_fklim_data(7)
    region_mapping = {1: 'Balai I', 2: 'Balai II', 3: 'Balai III', 4: 'Balai IV', 5: 'Balai V'}
    return render_template('Dashboard.html',
                           data_persentase_terkecil=data_persentase_terkecil,
                           data_grafik=data_grafik,
                           region_mapping=region_mapping)

# ======================== KETERSEDIAAN DATA v2 ========================

@app.route('/ketersediaan_data_v2')
def ketersediaan_data_v2():
    conn = get_db_connection()
    region_list = []
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT region_id FROM dim_geo_propinsi ORDER BY region_id ASC;")
            region_list = [row[0] for row in cur.fetchall()]
            cur.close()
        finally:
            conn.close()
    initial_stations = []
    if region_list:
        initial_stations = get_stations_by_region(region_list[0]).json
    years_list = get_available_years()
    return render_template('ketersediaan_data_v2.html',
                           chart_data=None, table_data=None,
                           region_list=region_list,
                           initial_stations=initial_stations,
                           years_list=years_list)

@app.route('/api/get_stations/<int:region_id>')
def get_stations_by_region(region_id):
    conn = get_db_connection()
    if conn is None:
        return jsonify([])
    stations = []
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT s.station_sk_id, s.name_stations
            FROM dim_stations AS s
            JOIN dim_geo_kabupaten AS k ON s.kabupaten_id = k.kabupaten_id
            JOIN dim_geo_propinsi AS p ON k.propinsi_id = p.propinsi_id
            WHERE p.region_id = %s
            ORDER BY s.name_stations ASC;
        """, (region_id,))
        for row in cur.fetchall():
            stations.append({'id': row[0], 'name': row[1]})
        cur.close()
    finally:
        conn.close()
    return jsonify(stations)

@app.route('/api/ketersediaan_data_v2/search', methods=['POST'])
def search_data_availability():
    data = request.json
    station_id = data.get('station')
    parameter = data.get('parameter')
    time_option = data.get('time_option')
    tahun = data.get('tahun')
    bulan = data.get('bulan')
    start_date_str = data.get('start_date')
    end_date_str = data.get('end_date')

    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Database connection failed"}), 500

    try:
        cur = conn.cursor()
        if parameter == 'average':
            if time_option in ['seluruhData', 'pilihTahun']:
                where = ["s.station_sk_id = %s"]
                params = [station_id]
                if time_option == 'pilihTahun':
                    where.append("SUBSTRING(f.time_month_id::text, 1, 4) = %s")
                    params.append(str(tahun))
                where_str = " AND ".join(where)
                if time_option == 'pilihTahun':
                    cur.execute(f"""
                        SELECT t.month_name, f.percentage_available
                        FROM fact_bmkgsoft_fklim_availability_monthly f
                        JOIN dim_stations s ON f.station_sk_id = s.station_sk_id
                        JOIN dim_time_month t ON f.time_month_id = t.time_month_id
                        WHERE {where_str}
                        ORDER BY f.time_month_id ASC;
                    """, tuple(params))
                    rows = cur.fetchall()
                    chart_data = {'labels': [r[0] for r in rows], 'data': [r[1] for r in rows]}
                    table_data = [{'Bulan': r[0], 'Persentase': f"{r[1]:.2f}%"} for r in rows]
                else:
                    cur.execute(f"""
                        SELECT t.year, AVG(f.percentage_available)
                        FROM fact_bmkgsoft_fklim_availability_monthly f
                        JOIN dim_stations s ON f.station_sk_id = s.station_sk_id
                        JOIN dim_time_month t ON f.time_month_id = t.time_month_id
                        WHERE {where_str}
                        GROUP BY t.year
                        ORDER BY t.year ASC;
                    """, tuple(params))
                    rows = cur.fetchall()
                    chart_data = {'labels': [str(r[0]) for r in rows], 'data': [r[1] for r in rows]}
                    table_data = [{'Tahun': str(r[0]), 'Persentase Rata-rata': f"{r[1]:.2f}%"} for r in rows]
                return jsonify({"chart_data": chart_data, "table_data": table_data})
        else:
            # Parameter harian
            where = ["s.station_sk_id = %s"]
            params = [station_id]
            if time_option == 'pilihTahun':
                start = date(int(tahun), 1, 1)
                end = date(int(tahun), 12, 31)
                where.append("f.data_timestamp BETWEEN %s AND %s")
                params.extend([start, end])
            elif time_option == 'pilihBulan':
                start = date(int(tahun), int(bulan), 1)
                end = date(int(tahun), int(bulan), monthrange(int(tahun), int(bulan))[1])
                where.append("f.data_timestamp BETWEEN %s AND %s")
                params.extend([start, end])
            elif time_option == 'rentangWaktu':
                start = datetime.strptime(start_date_str, '%Y-%m-%d').date()
                end = datetime.strptime(end_date_str, '%Y-%m-%d').date()
                where.append("f.data_timestamp BETWEEN %s AND %s")
                params.extend([start, end])
            where_str = " AND ".join(where)
            cur.execute(f"""
                SELECT f.data_timestamp,
                       CASE WHEN f.{parameter} IS NOT NULL AND f.{parameter} = f.{parameter} THEN 100 ELSE 0 END
                FROM fact_data_fklim f
                JOIN dim_stations s ON f.station_sk_id = s.station_sk_id
                WHERE {where_str}
                ORDER BY f.data_timestamp ASC;
            """, tuple(params))
            rows = cur.fetchall()
            chart_data = {'labels': [str(r[0]) for r in rows], 'data': [r[1] for r in rows]}
            table_data = [{'Tanggal': str(r[0]), 'Persentase': f"{r[1]:.2f}%"} for r in rows]
            return jsonify({"chart_data": chart_data, "table_data": table_data})
    finally:
        conn.close()
    return jsonify({"chart_data": None, "table_data": None})

@app.route('/api/dashboard_data/<int:period>')
def api_dashboard_data(period):
    mapping = {1: {'days': 7, 'table': 'fklim'}, 2: {'days': 30, 'table': 'fklim'}, 3: {'months': 12, 'table': 'monthly'}}
    info = mapping.get(period, {'days': 0, 'table': 'none'})
    if info['table'] == 'monthly':
        data_persentase_terkecil, data_grafik = get_dynamic_data(info['months'])
    elif info['table'] == 'fklim':
        data_persentase_terkecil, data_grafik = get_fklim_data(info['days'])
    else:
        data_persentase_terkecil, data_grafik = [], {}
    return jsonify({"data_persentase_terkecil": data_persentase_terkecil, "data_grafik": data_grafik})

if __name__ == '__main__':
    app.run(debug=True)
