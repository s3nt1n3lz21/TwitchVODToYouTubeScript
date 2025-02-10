import os
import requests
import subprocess
import time
import csv
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from pytz import timezone
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Twitch API Config
TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_ACCESS_TOKEN = os.getenv("TWITCH_ACCESS_TOKEN")
TWITCH_USER_ID = os.getenv("TWITCH_USER_ID")

# YouTube API Config
YOUTUBE_API_SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]
YOUTUBE_CLIENT_SECRET_FILE = os.getenv("YOUTUBE_CLIENT_SECRET_FILE")

# Directory for video files
DOWNLOAD_DIR = "./vods"
SEGMENTS_DIR = "./segments"
PROCESSED_FILE = "processed_vods.csv"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(SEGMENTS_DIR, exist_ok=True)

def load_processed_vods():
    processed = []
    if os.path.exists(PROCESSED_FILE):
        with open(PROCESSED_FILE, "r") as file:
            reader = csv.reader(file)
            processed = [row for row in reader]
    return processed

def save_processed_vod(vod_id, game_name, part_number):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(PROCESSED_FILE, "a") as file:
        writer = csv.writer(file)
        writer.writerow([vod_id, game_name, part_number, timestamp])

def get_last_part_number_for_game(game_name, processed_vods):
    max_part_number = 0
    for row in processed_vods:
        if row[1].lower() == game_name.lower():
            max_part_number = max(max_part_number, int(row[2]))
    return max_part_number

def sort_processed_vods():
    if os.path.exists(PROCESSED_FILE):
        with open(PROCESSED_FILE, "r") as file:
            reader = csv.reader(file)
            sorted_vods = sorted(reader, key=lambda row: row[1], reverse=True)
        with open(PROCESSED_FILE, "w") as file:
            writer = csv.writer(file)
            writer.writerows(sorted_vods)

def fetch_vod_details():
    url = f"https://api.twitch.tv/helix/videos?user_id={TWITCH_USER_ID}&first=100"
    headers = {"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {TWITCH_ACCESS_TOKEN}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    videos = response.json()["data"]
    return [(video["id"], video["url"], video["title"], video["game_name"]) for video in videos]

def download_vod(vod_url, vod_id):
    output_path = os.path.join(DOWNLOAD_DIR, f"{vod_id}.mp4")
    subprocess.run(["streamlink", vod_url, "best", "-o", output_path])
    return output_path


def split_vod(vod_path):
    segments = []
    total_duration = get_video_duration(vod_path)
    
    # Determine the number of 30-minute segments
    target_segment_duration = 1800  # 30 minutes in seconds
    num_segments = round(total_duration / target_segment_duration)
    
    # Ensure a reasonable segment length based on the total duration
    segment_duration = total_duration / max(1, num_segments)

    output_template = os.path.join(SEGMENTS_DIR, "segment_%03d.mp4")
    subprocess.run([
        "ffmpeg", "-i", vod_path, "-c", "copy", "-map", "0",
        "-segment_time", str(segment_duration), "-f", "segment", output_template
    ])
    for file in os.listdir(SEGMENTS_DIR):
        if file.startswith("segment_") and file.endswith(".mp4"):
            segments.append(os.path.join(SEGMENTS_DIR, file))
    
    return segments

def get_video_duration(video_path):
    result = subprocess.run(["ffprobe", "-i", video_path, "-show_entries", "format=duration", "-v", "quiet", "-of", "csv=p=0"], capture_output=True, text=True)
    return float(result.stdout.strip())

def upload_to_youtube(video_file, title, description):
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.errors import HttpError

    flow = InstalledAppFlow.from_client_secrets_file(YOUTUBE_CLIENT_SECRET_FILE, YOUTUBE_API_SCOPES)
    credentials = flow.run_local_server(port=8080, prompt="consent")
    youtube = build("youtube", "v3", credentials=credentials)

    try:
        body = {
            "snippet": {
                "title": title,
                "description": description,
                "tags": ["Twitch", "Gaming", "VOD"],
                "categoryId": "20",
            },
            "status": {"privacyStatus": "public"},
        }
        media = MediaFileUpload(video_file, chunksize=-1, resumable=True)
        request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
        response = request.execute()
        print(f"Uploaded: {video_file}, YouTube Video ID: {response['id']}")
    except HttpError as e:
        print(f"An error occurred: {e}")

def main():
    print("Checking for new Twitch VODs...")
    latest_vods = fetch_vod_details()
    processed_vods = load_processed_vods()

    for vod_id, vod_url, vod_title, vod_category in latest_vods:
        # Check if VOD has been processed
        if any(vod_id == row[0] for row in processed_vods):
            print(f"VOD {vod_id} already processed.")
            continue

        print(f"Downloading VOD: {vod_title} (Category: {vod_category})")
        vod_path = download_vod(vod_url, vod_id)

        # Extract game name from the VOD title
        game_name = vod_title.split(" |")[0]  # Get everything before the first " |"

        last_part_number = get_last_part_number_for_game(game_name, processed_vods)

        # For cooking videos, treat them as "Just Chatting" and ensure correct part number
        if vod_title.lower().startswith("cooking |"):
            vod_category = "just chatting"

        # Handle Just Chatting and Cooking videos with incremented part numbers
        if vod_category.lower() == "just chatting" or vod_category.lower() == "cooking":
            # Upload full VOD and increment the part number
            part_number = last_part_number + 1
            print(f"Uploading VOD (Category: {vod_category}) with Part Number {part_number}")
            upload_to_youtube(vod_path, vod_title, f"Full Twitch VOD: {vod_title}")
            save_processed_vod(vod_id, game_name, part_number)
        else:
            # Split the VOD into segments and upload
            print("Splitting VOD into consistent-length segments...")
            segments = split_vod(vod_path)
            for i, segment in enumerate(segments):
                part_number = last_part_number + i + 1  # Increment part number for each segment
                segment_title = f"{vod_title} - Part {part_number}"
                description = f"Segment {part_number} of Twitch VOD: {vod_title}. Exported automatically."
                upload_to_youtube(segment, segment_title, description)

            # Save the last part number after uploading all segments
            save_processed_vod(vod_id, game_name, last_part_number + len(segments))

        print(f"VOD {vod_id} processed successfully!")

    sort_processed_vods()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")