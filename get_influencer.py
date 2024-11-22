import googleapiclient.discovery
import csv
from datetime import datetime

# Konfigurasi API key
API_KEY = "AIzaSyB13bZ-Ydw__76mOUYF9iap8ZPxz3yDssg"
API_SERVICE_NAME = "youtube"
API_VERSION = "v3"

# Inisialisasi YouTube API client
youtube = googleapiclient.discovery.build(API_SERVICE_NAME, API_VERSION, developerKey=API_KEY)

# Data statis untuk negara dengan country_id berupa kode negara
countries = [
    {"country_id": "ID", "country_name": "Indonesia"},
    {"country_id": "EN", "country_name": "Inggris"},
    {"country_id": "US", "country_name": "USA"},
]

# Fungsi untuk mendapatkan data channel berdasarkan nama
def get_channel_data(query):
    request = youtube.search().list(
        part="id,snippet",
        type="channel",
        q=query,
        maxResults=1
    )
    response = request.execute()
    
    if not response['items']:
        print(f"Channel '{query}' tidak ditemukan.")
        return None
    
    channel = response['items'][0]
    return {
        "influencer_id": channel['id']['channelId'],
        "username": channel['snippet']['title'],
        "created_at": channel['snippet']['publishedAt']
    }


# Simpan data ke dalam CSV
def save_to_csv(data, filename, fieldnames):
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

# Proses utama
def main():
    influencers = []

    for country in countries:
        for i in range(30):  # 30 influencer per negara
            query = f"Influencer {country['country_name']} {i+1}"
            channel_data = get_channel_data(query)
            if not channel_data:
                continue

            channel_data["view_count"] = 0  # Placeholder untuk view_count
            channel_data["subcriber_count"] = 0  # Placeholder untuk subscriber_count
            channel_data["video_count"] = 0  # Placeholder untuk video_count
            channel_data["country_id"] = country['country_id']
            influencers.append(channel_data)

    # Simpan ke CSV
    save_to_csv(countries, "countries.csv", ["country_id", "country_name"])
    save_to_csv(influencers, "influencers.csv", ["influencer_id", "username", "view_count", "subcriber_count", "video_count", "created_at", "country_id"])
    print("Data berhasil disimpan.")

if __name__ == "__main__":
    main()
