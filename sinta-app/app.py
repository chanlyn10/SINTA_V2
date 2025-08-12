# flask_app.py
from flask import Flask, render_template, jsonify, request
import psycopg2
from datetime import datetime, timedelta, date
from calendar import monthrange
import os

app = Flask(__name__)

# === 1. Koneksi Database ===
# It's best practice to use environment variables for sensitive data.
# Replace these placeholder values with your actual database credentials.
# You can also use a .env file and a library like python-dotenv.
db_config = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "database": os.environ.get("DB_NAME", "Data_Warehouse_DDK"),
    "user": os.environ.get("DB_USER", "postgres"),
    "password": os.environ.get("DB_PASSWORD", "root"),
    "port": os.environ.get("DB_PORT", "5432")
}

def get_db_connection():
    """Membuat dan mengembalikan koneksi database."""
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
        return None

def get_time_month_id(date_obj):
    """Mengubah objek datetime menjadi integer time_month_id (YYYYMM)."""
    return int(date_obj.strftime('%Y%m'))

def get_start_month_id(period_months):
    """Mendapatkan time_month_id awal berdasarkan periode dalam bulan."""
    today = datetime.now()
    # Calculate the start date by subtracting the number of months.
    start_date = today.replace(day=1) - timedelta(days=30 * (period_months - 1))
    return get_time_month_id(start_date)

def get_dynamic_data(period_months):
    """
    Mengambil data grafik dan stasiun terkecil dari tabel fact_bmkgsoft_fklim_availability_monthly
    berdasarkan periode dalam bulan.
    """
    if period_months == 0:
        print("Periode 0 bulan dipilih, mengembalikan data kosong.")
        return [], {}
        
    conn = get_db_connection()
    if conn is None:
        return None, None
    
    current_month_id = get_time_month_id(datetime.now())
    start_month_id = get_start_month_id(period_months)
    
    data_grafik = {}
    data_persentase_terkecil = []

    print(f"\n--- DEBUG: Mengambil data dari fact_bmkgsoft_fklim_availability_monthly untuk periode {period_months} bulan ---")
    print(f"Rentang time_month_id: {start_month_id} hingga {current_month_id}")
    
    try:
        cur = conn.cursor()
        
        # Query to find the 3 stations with the lowest average availability percentage
        query_terkecil = """
            SELECT 
                s.name_stations, 
                s.station_sk_id, 
                AVG(f.percentage_available) as avg_percentage
            FROM fact_bmkgsoft_fklim_availability_monthly AS f
            JOIN dim_stations AS s ON f.station_sk_id = s.station_sk_id
            WHERE f.time_month_id BETWEEN %s AND %s
            GROUP BY s.name_stations, s.station_sk_id
            ORDER BY avg_percentage ASC
            LIMIT 3;
        """
        cur.execute(query_terkecil, (start_month_id, current_month_id))
        rows_terkecil = cur.fetchall()
        for row in rows_terkecil:
            data_persentase_terkecil.append({
                "persentase": f"{row[2]:.2f}%",
                "id": row[1],
                "nama": row[0]
            })
            
        print(f"Hasil query stasiun terkecil: {len(rows_terkecil)} baris")
            
        # Query for chart data, grouped by region
        region_ids = [1, 2, 3, 4, 5]
        for region_id in region_ids:
            query_grafik = """
                SELECT s.name_stations, AVG(f.percentage_available)
                FROM fact_bmkgsoft_fklim_availability_monthly AS f
                JOIN dim_stations AS s ON f.station_sk_id = s.station_sk_id
                JOIN dim_geo_kabupaten AS k ON s.kabupaten_id = k.kabupaten_id
                JOIN dim_geo_propinsi AS p ON k.propinsi_id = p.propinsi_id
                WHERE p.region_id = %s AND f.time_month_id BETWEEN %s AND %s
                GROUP BY s.name_stations
                ORDER BY AVG(f.percentage_available) DESC;
            """
            cur.execute(query_grafik, (region_id, start_month_id, current_month_id))
            rows_grafik = cur.fetchall()
            
            data_grafik[region_id] = {
                'labels': [row[0] for row in rows_grafik],
                'data': [row[1] for row in rows_grafik]
            }
            print(f"Region {region_id}: {len(rows_grafik)} stasiun")
            
        cur.close()
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()
            
    return data_persentase_terkecil, data_grafik

def get_fklim_data(period_days):
    """
    Mengambil data dari fact_data_fklim dengan menghitung persentase ketersediaan.
    """
    conn = get_db_connection()
    if conn is None:
        return None, None
        
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=period_days)
    
    data_grafik = {}
    data_persentase_terkecil = []

    print(f"\n--- DEBUG: Mengambil data dari fact_data_fklim untuk periode {period_days} hari ---")
    print(f"Rentang tanggal: {start_date} hingga {end_date}")
    
    try:
        cur = conn.cursor()
        
        # Columns used to calculate availability
        param_columns = [
            'temp_07lt_c', 'temp_13lt_c', 'temp_18lt_c', 'temp_avg_c', 'temp_max_c', 'temp_min_c',
            'rainfall_mm', 'sunshine_h', 'weather_specific', 'pressure_mb', 'rel_humidity_07lt_pc',
            'rel_humidity_13lt_pc', 'rel_humidity_18lt_pc', 'rel_humidity_avg_pc', 'wind_speed_avg_km_h',
            'wind_dir_max', 'wind_speed_max_knots', 'wind_dir_cardinal'
        ]
        
        # Dynamic SQL to count non-null values
        availability_sum = " + ".join([f"CASE WHEN {col} IS NOT NULL AND {col} = {col} THEN 1 ELSE 0 END" for col in param_columns])
        
        # Query for stations with the lowest average availability percentage
        query_terkecil = f"""
            SELECT
                s.name_stations,
                s.station_sk_id,
                AVG(({availability_sum}) / 18.0) * 100 as avg_percentage
            FROM fact_data_fklim AS f
            JOIN dim_stations AS s ON f.station_sk_id = s.station_sk_id
            WHERE f.data_timestamp BETWEEN %s AND %s
            GROUP BY s.name_stations, s.station_sk_id
            ORDER BY avg_percentage ASC
            LIMIT 3;
        """
        cur.execute(query_terkecil, (start_date, end_date))
        rows_terkecil = cur.fetchall()
        for row in rows_terkecil:
            data_persentase_terkecil.append({
                "persentase": f"{row[2]:.2f}%",
                "id": row[1],
                "nama": row[0]
            })
            
        print(f"Hasil query stasiun terkecil (fact_data_fklim): {len(rows_terkecil)} baris")

        # Query for chart data, grouped by region
        region_ids = [1, 2, 3, 4, 5]
        for region_id in region_ids:
            query_grafik = f"""
                SELECT s.name_stations, AVG(({availability_sum}) / 18.0) * 100
                FROM fact_data_fklim AS f
                JOIN dim_stations AS s ON f.station_sk_id = s.station_sk_id
                JOIN dim_geo_kabupaten AS k ON s.kabupaten_id = k.kabupaten_id
                JOIN dim_geo_propinsi AS p ON k.propinsi_id = p.propinsi_id
                WHERE p.region_id = %s AND f.data_timestamp BETWEEN %s AND %s
                GROUP BY s.name_stations
                ORDER BY AVG(({availability_sum}) / 18.0) * 100 DESC;
            """
            cur.execute(query_grafik, (region_id, start_date, end_date))
            rows_grafik = cur.fetchall()
            
            data_grafik[region_id] = {
                'labels': [row[0] for row in rows_grafik],
                'data': [row[1] for row in rows_grafik]
            }
            print(f"Region {region_id}: {len(rows_grafik)} stasiun")
            
        cur.close()
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()
            
    return data_persentase_terkecil, data_grafik

def get_available_years():
    """Mengambil daftar tahun yang tersedia dari database."""
    conn = get_db_connection()
    years = []
    if conn:
        try:
            cur = conn.cursor()
            query = """
                SELECT DISTINCT t.year 
                FROM fact_bmkgsoft_fklim_availability_monthly AS f
                JOIN dim_time_month AS t ON f.time_month_id = t.time_month_id
                ORDER BY t.year DESC;
            """
            cur.execute(query)
            years = [str(row[0]) for row in cur.fetchall()]
            cur.close()
        except psycopg2.Error as e:
            print(f"Database error: {e}")
        finally:
            if conn:
                conn.close()
    return years

@app.route('/')
def index():
    # Initial data load for the dashboard, defaults to a 7-day period.
    data_persentase_terkecil, data_grafik = get_fklim_data(7)
    if data_grafik is None:
        data_grafik = {}
    if data_persentase_terkecil is None:
        data_persentase_terkecil = []
    
    region_mapping = {
        1: 'Balai I',
        2: 'Balai II',
        3: 'Balai III',
        4: 'Balai IV',
        5: 'Balai V'
    }
        
    return render_template(
        'Dashboard.html',
        data_persentase_terkecil=data_persentase_terkecil,
        data_grafik=data_grafik,
        region_mapping=region_mapping
    )

@app.route('/ketersediaan_data_v2')
def ketersediaan_data_v2():
    # Page for detailed data availability search.
    conn = get_db_connection()
    region_list = []
    initial_stations = []
    if conn:
        try:
            cur = conn.cursor()
            query = "SELECT DISTINCT region_id FROM dim_geo_propinsi ORDER BY region_id ASC;"
            cur.execute(query)
            rows = cur.fetchall()
            region_list = [row[0] for row in rows]
            cur.close()
        except psycopg2.Error as e:
            print(f"Database error: {e}")
        finally:
            if conn:
                conn.close()
    
    if region_list:
        initial_stations = get_stations_by_region(region_list[0]).json
    
    years_list = get_available_years()

    return render_template('ketersediaan_data_v2.html', 
                            chart_data=None, 
                            table_data=None,
                            region_list=region_list,
                            initial_stations=initial_stations,
                            years_list=years_list)

@app.route('/api/get_stations/<int:region_id>')
def get_stations_by_region(region_id):
    """API endpoint to get stations based on region_id."""
    conn = get_db_connection()
    if conn is None:
        return jsonify([])
    
    stations = []
    try:
        cur = conn.cursor()
        query = """
            SELECT s.station_sk_id, s.name_stations
            FROM dim_stations AS s
            JOIN dim_geo_kabupaten AS k ON s.kabupaten_id = k.kabupaten_id
            JOIN dim_geo_propinsi AS p ON k.propinsi_id = p.propinsi_id
            WHERE p.region_id = %s
            ORDER BY s.name_stations ASC;
        """
        cur.execute(query, (region_id,))
        rows = cur.fetchall()
        for row in rows:
            stations.append({
                'id': row[0],
                'name': row[1]
            })
        cur.close()
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()
            
    return jsonify(stations)

@app.route('/api/ketersediaan_data_v2/search', methods=['POST'])
def search_data_availability():
    """API endpoint to handle detailed data availability search."""
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

        if parameter != 'average':
            query_params = [station_id]
            where_clauses = ["s.station_sk_id = %s"]

            if time_option == 'pilihTahun':
                where_clauses.append("t.year = %s")
                query_params.append(str(tahun))
                
                availability_column = f"availability_{parameter}"
                
                query_data = f"""
                    SELECT 
                        t.month_name_ina, 
                        AVG(f.{availability_column}) as available_percentage
                    FROM fact_bmkgsoft_fklim_availability_monthly AS f
                    JOIN dim_stations AS s ON f.station_sk_id = s.station_sk_id
                    JOIN dim_time_month AS t ON f.time_month_id = t.time_month_id
                    WHERE {' AND '.join(where_clauses)}
                    GROUP BY f.time_month_id, t.month_name_ina
                    ORDER BY f.time_month_id ASC;
                """
                cur.execute(query_data, tuple(query_params))
                rows = cur.fetchall()
                
                chart_labels = [row[0] for row in rows]
                chart_data_values = [row[1] for row in rows]
                chart_data = {'labels': chart_labels, 'data': chart_data_values}
                table_data_formatted = [{'Bulan': chart_labels[i], 'Persentase': f"{chart_data_values[i]:.2f}%"} for i in range(len(chart_labels))]
            
            elif time_option == 'pilihBulan':
                if not tahun or not bulan:
                    return jsonify({"error": "Tahun dan Bulan harus diisi"}), 400
                start_date_obj = date(int(tahun), int(bulan), 1)
                last_day = monthrange(int(tahun), int(bulan))[1]
                end_date_obj = date(int(tahun), int(bulan), last_day)
                where_clauses.append("f.data_timestamp BETWEEN %s AND %s")
                query_params.extend([start_date_obj, end_date_obj])
                
                query_data_daily = f"""
                    SELECT
                        f.data_timestamp,
                        CASE WHEN f.{parameter} IS NOT NULL AND f.{parameter} = f.{parameter} THEN 100 ELSE 0 END AS available_percentage
                    FROM fact_data_fklim AS f
                    JOIN dim_stations AS s ON f.station_sk_id = s.station_sk_id
                    WHERE {' AND '.join(where_clauses)}
                    ORDER BY f.data_timestamp ASC;
                """
                cur.execute(query_data_daily, tuple(query_params))
                rows = cur.fetchall()
                chart_data = {'labels': [str(row[0].date()) for row in rows], 'data': [row[1] for row in rows]}
                table_data_formatted = [{'Tanggal': str(row[0].date()), 'Persentase': f"{row[1]:.2f}%"} for row in rows]
                
            elif time_option == 'rentangWaktu':
                start_date_obj = datetime.strptime(start_date_str, '%Y-%m-%d').date()
                end_date_obj = datetime.strptime(end_date_str, '%Y-%m-%d').date()
                where_clauses.append("f.data_timestamp BETWEEN %s AND %s")
                query_params.extend([start_date_obj, end_date_obj])
                
                query_data_daily = f"""
                    SELECT
                        f.data_timestamp,
                        CASE WHEN f.{parameter} IS NOT NULL AND f.{parameter} = f.{parameter} THEN 100 ELSE 0 END AS available_percentage
                    FROM fact_data_fklim AS f
                    JOIN dim_stations AS s ON f.station_sk_id = s.station_sk_id
                    WHERE {' AND '.join(where_clauses)}
                    ORDER BY f.data_timestamp ASC;
                """
                cur.execute(query_data_daily, tuple(query_params))
                rows = cur.fetchall()
                chart_data = {'labels': [str(row[0].date()) for row in rows], 'data': [row[1] for row in rows]}
                table_data_formatted = [{'Tanggal': str(row[0].date()), 'Persentase': f"{row[1]:.2f}%"} for row in rows]

            elif time_option == 'seluruhData':
                return jsonify({"error": "Pilihan 'Seluruh Data' hanya tersedia untuk parameter 'Rata-rata'."}), 400
            
            return jsonify({"chart_data": chart_data, "table_data": table_data_formatted, "time_option": time_option, "parameter": parameter, "tahun": tahun})
        
        else: # parameter == 'average'
            query_params = [station_id]
            where_clauses = ["s.station_sk_id = %s"]
            
            if time_option == 'pilihTahun':
                where_clauses.append("t.year = %s")
                query_params.append(str(tahun))
                
                query_chart_final = f"""
                    SELECT t.month_name_ina, AVG(f.percentage_available)
                    FROM fact_bmkgsoft_fklim_availability_monthly AS f
                    JOIN dim_stations AS s ON f.station_sk_id = s.station_sk_id
                    JOIN dim_time_month AS t ON f.time_month_id = t.time_month_id
                    WHERE {' AND '.join(where_clauses)}
                    GROUP BY f.time_month_id, t.month_name_ina
                    ORDER BY f.time_month_id ASC;
                """
                cur.execute(query_chart_final, tuple(query_params))
                chart_rows = cur.fetchall()

                labels = [row[0] for row in chart_rows]
                data_values = [row[1] for row in chart_rows]
                chart_data = {'labels': labels, 'data': data_values}
                table_data_formatted = [{'Bulan': labels[i], 'Persentase Rata-rata': f"{data_values[i]:.2f}%"} for i in range(len(labels))]

            elif time_option == 'seluruhData':
                query_chart_final = f"""
                    SELECT t.year, AVG(f.percentage_available)
                    FROM fact_bmkgsoft_fklim_availability_monthly AS f
                    JOIN dim_stations AS s ON f.station_sk_id = s.station_sk_id
                    JOIN dim_time_month AS t ON f.time_month_id = t.time_month_id
                    WHERE {' AND '.join(where_clauses)}
                    GROUP BY t.year ORDER BY t.year ASC;
                """
                cur.execute(query_chart_final, tuple(query_params))
                chart_rows = cur.fetchall()
                chart_data = {'labels': [str(row[0]) for row in chart_rows], 'data': [row[1] for row in chart_rows]}
                table_data_formatted = [{'Tahun': str(row[0]), 'Persentase Rata-rata': f"{row[1]:.2f}%"} for row in chart_rows]
            
            else: # pilihBulan and rentangWaktu
                 query_params = [station_id]
                 where_clauses = ["s.station_sk_id = %s"]

                 if time_option == 'pilihBulan':
                    if not tahun or not bulan:
                        return jsonify({"error": "Tahun dan Bulan harus diisi"}), 400
                    start_date_obj = date(int(tahun), int(bulan), 1)
                    last_day = monthrange(int(tahun), int(bulan))[1]
                    end_date_obj = date(int(tahun), int(bulan), last_day)
                    where_clauses.append("f.data_timestamp BETWEEN %s AND %s")
                    query_params.extend([start_date_obj, end_date_obj])
                 elif time_option == 'rentangWaktu':
                    start_date_obj = datetime.strptime(start_date_str, '%Y-%m-%d').date()
                    end_date_obj = datetime.strptime(end_date_str, '%Y-%m-%d').date()
                    where_clauses.append("f.data_timestamp BETWEEN %s AND %s")
                    query_params.extend([start_date_obj, end_date_obj])
                 
                 where_clause_str = " AND ".join(where_clauses)
                 
                 param_columns = [
                    'temp_07lt_c', 'temp_13lt_c', 'temp_18lt_c', 'temp_avg_c', 'temp_max_c', 'temp_min_c',
                    'rainfall_mm', 'sunshine_h', 'weather_specific', 'pressure_mb', 'rel_humidity_07lt_pc',
                    'rel_humidity_13lt_pc', 'rel_humidity_18lt_pc', 'rel_humidity_avg_pc', 'wind_speed_avg_km_h',
                    'wind_dir_max', 'wind_speed_max_knots', 'wind_dir_cardinal'
                 ]
                 availability_sum = " + ".join([f"CASE WHEN {col} IS NOT NULL AND {col} = {col} THEN 1 ELSE 0 END" for col in param_columns])
                 
                 query_data_daily_avg = f"""
                     SELECT
                         f.data_timestamp,
                         AVG(({availability_sum}) / 18.0) * 100 as avg_percentage
                     FROM fact_data_fklim AS f
                     JOIN dim_stations AS s ON f.station_sk_id = s.station_sk_id
                     WHERE {where_clause_str}
                     GROUP BY f.data_timestamp
                     ORDER BY f.data_timestamp ASC;
                 """
                 cur.execute(query_data_daily_avg, tuple(query_params))
                 rows = cur.fetchall()
                 chart_data = {'labels': [str(row[0].date()) for row in rows], 'data': [row[1] for row in rows]}
                 table_data_formatted = [{'Tanggal': str(row[0].date()), 'Persentase Rata-rata': f"{row[1]:.2f}%"} for row in rows]
            
            return jsonify({"chart_data": chart_data, "table_data": table_data_formatted, "time_option": time_option, "parameter": parameter, "tahun": tahun})
                
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        return jsonify({"error": "Failed to fetch data from database"}), 500
    finally:
        if conn:
            conn.close()

    return jsonify({"chart_data": None, "table_data": None})


@app.route('/api/dashboard_data/<int:period>')
def get_dashboard_data(period):
    """API endpoint to get dashboard data based on a pre-defined period."""
    period_mapping = {
        1: {'days': 7, 'table': 'fklim'},
        2: {'days': 30, 'table': 'fklim'},
        3: {'months': 12, 'table': 'monthly'},
    }
    period_info = period_mapping.get(period, {'days': 0, 'table': 'none'})
    if period_info['table'] == 'monthly':
        data_persentase_terkecil, data_grafik = get_dynamic_data(period_info['months'])
    elif period_info['table'] == 'fklim':
        data_persentase_terkecil, data_grafik = get_fklim_data(period_info['days'])
    else:
        data_persentase_terkecil, data_grafik = [], {}
    if data_grafik is None:
        return jsonify({"error": "Failed to fetch data from database"}), 500
    return jsonify({
        "data_persentase_terkecil": data_persentase_terkecil,
        "data_grafik": data_grafik
    })

@app.route('/api/get_daily_data', methods=['POST'])
def get_daily_data():
    """Endpoint API untuk mengambil data harian dari tabel fact_data_fklim."""
    data = request.json
    station_id = data.get('station')
    tahun = data.get('tahun')
    bulan_label = data.get('bulan')
    parameter = data.get('parameter')

    if not all([station_id, tahun, bulan_label, parameter]):
        return jsonify({"error": "Missing required parameters"}), 400

    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Database connection failed"}), 500

    bulan_map = {
        'Januari': 1, 'Februari': 2, 'Maret': 3, 'April': 4, 'Mei': 5, 'Juni': 6,
        'Juli': 7, 'Agustus': 8, 'September': 9, 'Oktober': 10, 'November': 11, 'Desember': 12
    }
    bulan_int = bulan_map.get(bulan_label)
    if not bulan_int:
        return jsonify({"error": "Invalid month"}), 400
    
    start_date = date(int(tahun), bulan_int, 1)
    end_date = date(int(tahun), bulan_int, monthrange(int(tahun), bulan_int)[1])

    try:
        cur = conn.cursor()

        param_columns = [
            'temp_07lt_c', 'temp_13lt_c', 'temp_18lt_c', 'temp_avg_c', 'temp_max_c', 'temp_min_c',
            'rainfall_mm', 'sunshine_h', 'weather_specific', 'pressure_mb', 'rel_humidity_07lt_pc',
            'rel_humidity_13lt_pc', 'rel_humidity_18lt_pc', 'rel_humidity_avg_pc', 'wind_speed_avg_km_h',
            'wind_dir_max', 'wind_speed_max_knots', 'wind_dir_cardinal'
        ]
        
        select_columns = ", ".join([f"f.{col}" for col in param_columns])
        
        query = f"""
            SELECT
                f.data_timestamp,
                {select_columns}
            FROM fact_data_fklim AS f
            JOIN dim_stations AS s ON f.station_sk_id = s.station_sk_id
            WHERE s.station_sk_id = %s AND f.data_timestamp BETWEEN %s AND %s
            ORDER BY f.data_timestamp ASC;
        """
        cur.execute(query, (station_id, start_date, end_date))
        rows = cur.fetchall()
        cur.close()

        # Process data for the daily table
        processed_data = {}
        for row in rows:
            day = row[0].day
            day_data = {'date': str(row[0].date()), 'available': {}}
            for i, col in enumerate(param_columns):
                is_available = row[i + 1] is not None
                day_data['available'][col] = is_available
            processed_data[day] = day_data
            
        days_in_month = monthrange(int(tahun), bulan_int)[1]
        daily_table_data = []

        headers = ['Bulan/Tanggal']
        for i in range(1, days_in_month + 1):
             headers.append(f"{i}")
        headers.append('Persentase') # Tambahkan kolom persentase

        param_to_display = param_columns
        if parameter != 'average':
            param_to_display = [parameter]

        for param in param_to_display:
            row_data = {'Bulan/Tanggal': param}
            available_days_count = 0
            for day in range(1, days_in_month + 1):
                is_available = processed_data.get(day, {}).get('available', {}).get(param, False)
                if is_available:
                    available_days_count += 1
                row_data[f"{day}"] = '<i class="fas fa-check-circle text-success"></i>' if is_available else ''
            
            # Hitung dan tambahkan persentase
            percentage = (available_days_count / days_in_month) * 100 if days_in_month > 0 else 0
            row_data['Persentase'] = f"{percentage:.2f}%"

            daily_table_data.append(row_data)

        return jsonify({
            "headers": headers, 
            "table_data": daily_table_data
        })

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        return jsonify({"error": "Failed to fetch daily data from database"}), 500
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    app.run(debug=True)
