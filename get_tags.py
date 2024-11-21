import googleapiclient.discovery
import googleapiclient.errors
import psycopg2
from psycopg2 import sql

# Konfigurasi API Key
API_KEY = "AIzaSyCndbg6Hu8UsyiNAcj9l11fT_JZIPjiWbU"
API_SERVICE_NAME = "youtube"
API_VERSION = "v3"

# Konfigurasi koneksi PostgreSQL
DB_HOST = "localhost"
DB_NAME = "monitoring_youtube"
DB_USER = "postgres"
DB_PASSWORD = "dika1708"

# Inisialisasi YouTube API client
youtube = googleapiclient.discovery.build(API_SERVICE_NAME, API_VERSION, developerKey=API_KEY)

# Fungsi untuk membuat database jika belum ada
def create_database():
    try:
        # Membuat koneksi ke PostgreSQL (tanpa menyebutkan nama database)
        connection = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD
        )
        connection.autocommit = True  # Set autocommit untuk memungkinkan perintah CREATE DATABASE
        cursor = connection.cursor()

        # Membuat database jika belum ada
        cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{DB_NAME}';")
        exists = cursor.fetchone()
        if not exists:
            cursor.execute(f"CREATE DATABASE {DB_NAME};")
            print(f"Database {DB_NAME} berhasil dibuat.")
        else:
            print(f"Database {DB_NAME} sudah ada.")

        cursor.close()
        connection.close()

    except Exception as e:
        print(f"Terjadi kesalahan saat membuat database: {e}")

# Fungsi untuk membuat tabel 'hastags' di dalam database yang sudah dibuat
def create_hastags_table():
    try:
        # Membuat koneksi ke database yang sudah ada
        connection = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = connection.cursor()

        # Membuat tabel 'hastags' jika belum ada
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hastags (
                video_id VARCHAR NOT NULL,
                tag VARCHAR NOT NULL,
                category_name VARCHAR NOT NULL,
                published_at TIMESTAMP
            );
        """)
        connection.commit()  # Commit the table creation
        print("Tabel 'hastags' berhasil dibuat.")

        cursor.close()
        connection.close()

    except Exception as e:
        print(f"Terjadi kesalahan saat membuat tabel: {e}")

# Fungsi untuk mendapatkan daftar kategori video (ID ke nama kategori)
def get_video_categories(region_code="ID"):
    category_dict = {}
    try:
        request = youtube.videoCategories().list(
            part="snippet",
            regionCode=region_code
        )
        response = request.execute()
        for item in response.get("items", []):
            category_id = item["id"]
            category_name = item["snippet"]["title"]
            category_dict[category_id] = category_name
    except googleapiclient.errors.HttpError as e:
        print(f"Terjadi kesalahan saat mengambil kategori video: {e}")
    except Exception as e:
        print(f"Kesalahan tidak terduga: {e}")
    return category_dict

# Fungsi untuk menyimpan data ke PostgreSQL
def save_to_postgresql(data):
    connection = None  # Initialize connection to handle the finally block
    try:
        # Membuat koneksi ke database PostgreSQL
        connection = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = connection.cursor()

        # Menyusun query SQL untuk memasukkan data
        insert_query = sql.SQL("""
            INSERT INTO hastags (video_id, tag, category_name, published_at)
            VALUES (%s, %s, %s, %s)
        """)

        # Menyisipkan setiap baris data ke dalam tabel
        for item in data:
            cursor.execute(insert_query, (item['video_id'], item['tag'], item['category_name'], item['publishedAt']))

        # Commit transaksi
        connection.commit()

        print("Data berhasil disimpan ke PostgreSQL.")

    except Exception as error:
        print(f"Terjadi kesalahan saat menyimpan data: {error}")
    finally:
        # Menutup koneksi jika berhasil dibuat
        if connection:
            cursor.close()
            connection.close()

# Fungsi untuk mengambil data dari video trending dan menyimpan langsung ke PostgreSQL
def fetch_and_save_trending_video_tags(region_code="ID"):
    next_page_token = None
    category_dict = get_video_categories(region_code)  # Mendapatkan kategori video

    # List untuk menyimpan data video trending
    trending_data = []

    # Loop untuk mengambil video trending dan menyimpan data
    while True:
        request = youtube.videos().list(
            part="snippet",
            chart="mostPopular",
            regionCode=region_code,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        # Proses setiap video dalam respons dan simpan ke list
        for item in response.get("items", []):
            video_id = item['id']
            video_tags = item['snippet'].get('tags', [])
            published_at = item['snippet'].get('publishedAt')
            category_id = item['snippet'].get('categoryId')
            category_name = category_dict.get(category_id, "Unknown")  # Ambil nama kategori

            # Menyimpan setiap tag sebagai baris terpisah
            for tag in video_tags:
                trending_data.append({
                    "video_id": video_id,
                    "tag": tag,
                    "category_name": category_name,
                    "publishedAt": published_at
                })

        # Dapatkan token halaman berikutnya jika ada
        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break  # Hentikan jika tidak ada halaman berikutnya

    # Menyimpan data ke PostgreSQL
    save_to_postgresql(trending_data)

# Menjalankan proses untuk membuat database, tabel, dan kemudian mengambil serta menyimpan data tags
create_database()  # Pastikan database sudah ada
create_hastags_table()  # Pastikan tabel 'hastags' sudah ada
fetch_and_save_trending_video_tags()  # Menjalankan scraping dan penyimpanan data
