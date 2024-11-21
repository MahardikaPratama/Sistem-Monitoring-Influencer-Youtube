import googleapiclient.discovery
import googleapiclient.errors
import psycopg2
from psycopg2 import sql
import csv


# Konfigurasi API key
API_KEY = "AIzaSyAznRNuRXpprsNHylKDrMtwYbKtB7Qzx60"  
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

def create_comments_table():
    try:
        # Membuat koneksi ke database PostgreSQL
        connection = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = connection.cursor()

        # Membuat tabel 'comments' jika belum ada
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS comments (
                video_id VARCHAR NOT NULL,
                comment_id VARCHAR NOT NULL PRIMARY KEY,
                comment_text TEXT NOT NULL
            );
        """)
        connection.commit()
        print("Tabel 'comments' berhasil dibuat.")

        cursor.close()
        connection.close()

    except Exception as e:
        print(f"Terjadi kesalahan saat membuat tabel komentar: {e}")


# Fungsi untuk mendapatkan semua komentar dari video (maksimal 25 komentar)
def get_video_comments(video_id):
    comments = []
    next_page_token = None

    while len(comments) < 25:  # Batasi jumlah komentar hingga 25
        try:
            request = youtube.commentThreads().list(
                part="id,snippet",
                videoId=video_id,
                textFormat="plainText",
                pageToken=next_page_token  # Menggunakan token halaman berikutnya
            )
            response = request.execute()

            for item in response['items']:
                if len(comments) >= 25:
                    break  # Hentikan jika sudah mencapai 25 komentar
                comment_data = {
                    "video_id": video_id,  # Menambahkan videoId
                    "comment_id": item['id'],
                    "comment_text": item['snippet']['topLevelComment']['snippet']['textDisplay'],
                }
                comments.append(comment_data)

            next_page_token = response.get('nextPageToken')
            if not next_page_token or len(comments) >= 25:
                break  # Hentikan jika tidak ada halaman berikutnya atau sudah cukup 25 komentar

        except googleapiclient.errors.HttpError as e:
            # Tangani error jika komentar dinonaktifkan
            if "commentsDisabled" in str(e):
                print(f"Komentar dinonaktifkan untuk video ID: {video_id}")
            else:
                print(f"Terjadi error lain untuk video ID {video_id}: {e}")
            break  # Hentikan loop jika terjadi error

    return comments

def save_comments_to_postgresql(comments):
    connection = None
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
            INSERT INTO comments (video_id, comment_id, comment_text)
            VALUES (%s, %s, %s)
            ON CONFLICT (comment_id) DO NOTHING;
        """)

        # Menyisipkan setiap komentar ke dalam tabel
        for comment in comments:
            cursor.execute(insert_query, (comment['video_id'], comment['comment_id'], comment['comment_text']))

        # Commit transaksi
        connection.commit()
        print("Komentar berhasil disimpan ke PostgreSQL.")

    except Exception as error:
        print(f"Terjadi kesalahan saat menyimpan komentar: {error}")
    finally:
        if connection:
            cursor.close()
            connection.close()


def process_and_save_video_comments(input_filename="videos.csv"):
    all_comments = []
    
    # Membaca video ID dari file CSV
    with open(input_filename, mode="r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            video_id = row['video_id']
            print(f"Memproses komentar untuk video ID: {video_id}")
            comments = get_video_comments(video_id)
            all_comments.extend(comments)
    
    # Simpan semua komentar ke PostgreSQL
    save_comments_to_postgresql(all_comments)


# Membuat database terlebih dahulu
create_database()

# Membuat tabel untuk komentar
create_comments_table()

# Memproses video dan menyimpan komentar ke PostgreSQL
process_and_save_video_comments()
