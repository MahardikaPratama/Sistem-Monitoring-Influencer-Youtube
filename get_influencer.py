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
def get_channel_data(channel_name):
    request = youtube.search().list(
        part="id,snippet",
        type="channel",
        q=channel_name,
        maxResults=1
    )
    response = request.execute()
    
    if not response['items']:
        print(f"Channel '{channel_name}' tidak ditemukan.")
        return None
    
    channel = response['items'][0]
    return {
        "influencer_id": channel['id']['channelId'],
        "username": channel['snippet']['title'],
        "created_at": channel['snippet']['publishedAt']
    }

# Fungsi untuk mendapatkan 20 video terbaru dari channel
def get_channel_videos(channel_id):
    video_data = []
    next_page_token = None
    
    while len(video_data) < 20:
        request = youtube.search().list(
            part="id,snippet",
            channelId=channel_id,
            maxResults=50,
            order="date",
            pageToken=next_page_token
        )
        response = request.execute()

        video_ids = [item['id']['videoId'] for item in response['items'] if 'videoId' in item['id']]
        
        if video_ids:
            videos_request = youtube.videos().list(
                part="snippet,statistics",
                id=",".join(video_ids)
            )
            videos_response = videos_request.execute()

            for video in videos_response['items']:
                if len(video_data) >= 20:  # Batasi hanya 20 video
                    break
                video_info = {
                    "video_id": video['id'],
                    "video_title": video['snippet']['title'],
                    "video_view_count": video['statistics'].get('viewCount', '0'),
                    "video_like_count": video['statistics'].get('likeCount', '0'),
                    "comment_count": video['statistics'].get('commentCount', '0'),
                    "tags": ', '.join(video['snippet'].get('tags', [])),
                    "published_at": video['snippet']['publishedAt']
                }
                video_data.append(video_info)

        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break
    
    return video_data

# Simpan data ke dalam CSV
def save_to_csv(data, filename, fieldnames):
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

# Proses utama
def main():
    influencers = []
    videos = []

    for country in countries:
        for i in range(30):  # 30 influencer per negara
            channel_name = f"Influencer {country['country_name']} {i+1}"
            channel_data = get_channel_data(channel_name)
            if not channel_data:
                continue

            channel_data["view_count"] = 0  # Placeholder untuk view_count
            channel_data["subcriber_count"] = 0  # Placeholder untuk subscriber_count
            channel_data["video_count"] = 0  # Placeholder untuk video_count
            channel_data["country_id"] = country['country_id']
            influencers.append(channel_data)

            video_data = get_channel_videos(channel_data["influencer_id"])
            for video in video_data:
                video["influencer_id"] = channel_data["influencer_id"]
                video["category_id"] = 1  # Placeholder category_id
                videos.append(video)

    # Simpan ke CSV
    save_to_csv(countries, "countries.csv", ["country_id", "country_name"])
    save_to_csv(influencers, "influencers.csv", ["influencer_id", "username", "view_count", "subcriber_count", "video_count", "created_at", "country_id"])
    save_to_csv(videos, "videos.csv", ["video_id", "video_title", "video_view_count", "video_like_count", "comment_count", "tags", "published_at", "influencer_id", "category_id"])
    print("Data berhasil disimpan.")

if __name__ == "__main__":
    main()
