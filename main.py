import os
import requests
import subprocess
import time
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from pytz import timezone
from datetime import datetime

# Twitch API Config
TWITCH_CLIENT_ID = "your_twitch_client_id"
TWITCH_ACCESS_TOKEN = "your_twitch_access_token"
TWITCH_USER_ID = "your_twitch_user_id"  # Find this using Twitch API or from your account.

# YouTube API Config
YOUTUBE_API_SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]
YOUTUBE_CLIENT_SECRET_FILE = "client_secret.json"

# Directory for video files
DOWNLOAD_DIR = "./vods"
SEGMENTS_DIR = "./segments"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(SEGMENTS_DIR, exist_ok=True)

# Check for new VODs on Twitch
def fetch_latest_vod():
    url = f"https://api.twitch.tv/helix/videos?user_id={TWITCH_USER_ID}&first=1"
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {TWITCH_ACCESS_TOKEN}",
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    videos = response.json()["data"]
    if videos:
        latest_vod = videos[0]
        return latest_vod["id"], latest_vod["url"], latest_vod["title"]
    return None, None, None

# Download VOD using Streamlink
def download_vod(vod_url, vod_id):
    output_path = os.path.join(DOWNLOAD_DIR, f"{vod_id}.mp4")
    subprocess.run(["streamlink", vod_url, "best", "-o", output_path])
    return output_path

# Split VOD into 30-minute segments using ffmpeg
def split_vod(vod_path):
    segments = []
    output_template = os.path.join(SEGMENTS_DIR, "segment_%03d.mp4")
    subprocess.run([
        "ffmpeg", "-i", vod_path, "-c", "copy", "-map", "0",
        "-segment_time", "1800", "-f", "segment", output_template
    ])
    for file in os.listdir(SEGMENTS_DIR):
        if file.startswith("segment_") and file.endswith(".mp4"):
            segments.append(os.path.join(SEGMENTS_DIR, file))
    return segments

# Upload video to YouTube
def upload_to_youtube(video_file, title, description):
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.errors import HttpError

    # Authenticate YouTube API
    flow = InstalledAppFlow.from_client_secrets_file(YOUTUBE_CLIENT_SECRET_FILE, YOUTUBE_API_SCOPES)
    credentials = flow.run_local_server(port=8080, prompt="consent")
    youtube = build("youtube", "v3", credentials=credentials)

    try:
        body = {
            "snippet": {
                "title": title,
                "description": description,
                "tags": ["Twitch", "Gaming", "VOD"],
                "categoryId": "20",  # Gaming category
            },
            "status": {"privacyStatus": "public"},
        }
        media = MediaFileUpload(video_file, chunksize=-1, resumable=True)
        request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
        response = request.execute()
        print(f"Uploaded: {video_file}, YouTube Video ID: {response['id']}")
    except HttpError as e:
        print(f"An error occurred: {e}")

# Main automation logic
def main():
    print("Checking for new Twitch VODs...")
    vod_id, vod_url, vod_title = fetch_latest_vod()
    if not vod_id:
        print("No new VODs found.")
        return

    # Skip if VOD already processed
    if os.path.exists(os.path.join(DOWNLOAD_DIR, f"{vod_id}.mp4")):
        print(f"VOD {vod_id} already processed.")
        return

    print(f"Downloading VOD: {vod_title}")
    vod_path = download_vod(vod_url, vod_id)

    print("Splitting VOD into 30-minute segments...")
    segments = split_vod(vod_path)

    print("Uploading segments to YouTube...")
    for i, segment in enumerate(segments):
        segment_title = f"{vod_title} - Part {i+1}"
        description = f"Segment {i+1} of Twitch VOD: {vod_title}. Exported automatically."
        upload_to_youtube(segment, segment_title, description)

    print("All segments uploaded successfully!")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")

    # while True:
    #     try:
    #         main()
    #     except Exception as e:
    #         print(f"Error: {e}")
    #     time.sleep(3600)  # Check for new VODs every hour
