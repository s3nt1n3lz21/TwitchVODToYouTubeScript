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

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

# Load environment variables
load_dotenv()

# Twitch API Config
TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_ACCESS_TOKEN = os.getenv("TWITCH_ACCESS_TOKEN")
TWITCH_REFRESH_TOKEN = os.getenv("TWITCH_REFRESH_TOKEN")
TWITCH_USER_ID = os.getenv("TWITCH_USER_ID")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")

# Token refresh URL
TOKEN_URL = "https://id.twitch.tv/oauth2/token"

# YouTube API Config
YOUTUBE_API_SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload", # For uploading videos
    "https://www.googleapis.com/auth/youtube.force-ssl" # For adding videos to playlists
]
YOUTUBE_CLIENT_SECRET_FILE = os.getenv("YOUTUBE_CLIENT_SECRET_FILE")
YOUTUBE_CHANNEL_ID = os.getenv("YOUTUBE_CHANNEL_ID")
YOUTUBE_TOKEN_FILE = os.getenv("YOUTUBE_TOKEN_FILE")

# Directory for video files
DOWNLOAD_DIR = "./vods"
SEGMENTS_DIR = "./segments"
PROCESSED_FILE = "processed_vods.csv"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(SEGMENTS_DIR, exist_ok=True)

def load_processed_vods():
    print("Loading list of processed vods from processed_vods.csv")
    processed = []
    if os.path.exists(PROCESSED_FILE):
        with open(PROCESSED_FILE, "r") as file:
            reader = csv.reader(file)
            processed = [row for row in reader]
    return processed

def save_processed_vod(vod_id, game_name, part_number):
    print(f"Logging {vod_id} {game_name} part {part_number} to csv as successfully processed")
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

def refresh_access_token():
    global TWITCH_ACCESS_TOKEN
    print("Refreshing Twitch access token...")
    response = requests.post(TOKEN_URL, data={
        "client_id": TWITCH_CLIENT_ID,
        "client_secret": TWITCH_CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": TWITCH_REFRESH_TOKEN
    })
    if response.status_code == 200:
        new_tokens = response.json()
        TWITCH_ACCESS_TOKEN = new_tokens["access_token"]
        with open(".env", "r") as file:
            lines = file.readlines()
        with open(".env", "w") as file:
            for line in lines:
                if line.startswith("TWITCH_ACCESS_TOKEN"):
                    file.write(f"TWITCH_ACCESS_TOKEN={TWITCH_ACCESS_TOKEN}\n")
                else:
                    file.write(line)
        print("Twitch access token refreshed.")
    else:
        print("Failed to refresh access token:", response.text)

def fetch_vod_details(start_date="2025-02-11"):
    url = f"https://api.twitch.tv/helix/videos?user_id={TWITCH_USER_ID}&first=10"
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {TWITCH_ACCESS_TOKEN}"
    }
    response = requests.get(url, headers=headers)
    
    if response.status_code == 401:  # Unauthorized, refresh token
        refresh_access_token()
        headers["Authorization"] = f"Bearer {TWITCH_ACCESS_TOKEN}"
        response = requests.get(url, headers=headers)

    response.raise_for_status()
    videos = response.json()["data"]

    # Filter VODs based on start_date
    if start_date:
        start_date = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone("UTC"))
        videos = [
            (video["id"], video["url"], video["title"], video["created_at"])
            for video in videos if datetime.strptime(video["created_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone("UTC")) >= start_date
        ]
    else:
        videos = [
            (video["id"], video["url"], video["title"], video["created_at"])
            for video in videos
        ]

    return videos

def download_vod(vod_url, vod_id):
    print(f"Downloading VOD: {vod_id}")
    output_path = os.path.join(DOWNLOAD_DIR, f"{vod_id}.mp4")
    subprocess.run(["streamlink", vod_url, "best", "-o", output_path])
    print(f"Downloaded VOD: {vod_id}")
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
    print(f"Splitting vod at {vod_path} into {num_segments} parts of duration {segment_duration}")
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

def authenticate_youtube():
    print("Authenticating YouTube Credentials")
    credentials = None

    # Load existing credentials if available
    if os.path.exists(YOUTUBE_TOKEN_FILE):
        credentials = Credentials.from_authorized_user_file(YOUTUBE_TOKEN_FILE)

    # If credentials are invalid or missing, perform authentication
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())  # Refresh token if expired
        else:
            flow = InstalledAppFlow.from_client_secrets_file(YOUTUBE_CLIENT_SECRET_FILE, YOUTUBE_API_SCOPES)
            credentials = flow.run_local_server(port=8080, prompt="consent")

            # Save credentials for future use
            with open(YOUTUBE_TOKEN_FILE, "w") as token:
                token.write(credentials.to_json())

    youtube = build("youtube", "v3", credentials=credentials)

    print(f"Authenticated with YouTube")
    return youtube

def upload_to_youtube(youtube, video_file, title, description):
    print(f"Starting upload: {title} to YouTube")
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
        media = MediaFileUpload(video_file, chunksize=1024 * 1024, resumable=True)
        request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
        
        print(f"Uploading {video_file} in chunks...")

        response = None
        progress = 0
        while response is None:
            status, response = request.next_chunk()
            if status:
                progress = int(status.progress() * 100)
                print(f"Upload progress: {progress}%")
        
        print(f"Upload complete! Video ID: {response['id']}")
        return response
    except HttpError as e:
        print(f"An error occurred uploading to YouTube: {e}")
        return None

def is_user_live():
    url = f"https://api.twitch.tv/helix/streams?user_id={TWITCH_USER_ID}"
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {TWITCH_ACCESS_TOKEN}"
    }
    response = requests.get(url, headers=headers)
    
    if response.status_code == 401:  # Unauthorized, refresh token
        refresh_access_token()
        headers["Authorization"] = f"Bearer {TWITCH_ACCESS_TOKEN}"
        response = requests.get(url, headers=headers)

    response.raise_for_status()
    stream_data = response.json()["data"]
    return len(stream_data) > 0  # If there's any stream data, the user is live

# Function to clear the vods and segments folder
def clear_folders():
    print("Clearing the vods and segments folders of downloaded and processed vods")
    # Clear the vods folder
    for file in os.listdir(DOWNLOAD_DIR):
        file_path = os.path.join(DOWNLOAD_DIR, file)
        if os.path.isfile(file_path):
            os.remove(file_path)
    
    # Clear the segments folder
    for file in os.listdir(SEGMENTS_DIR):
        file_path = os.path.join(SEGMENTS_DIR, file)
        if os.path.isfile(file_path):
            os.remove(file_path)

    print("Vods and segments folders cleared.")

def get_playlist_id_by_name(youtube, playlist_name):
    # List all playlists in the channel
    request = youtube.playlists().list(
        part="snippet",
        channelId=YOUTUBE_CHANNEL_ID,
        maxResults=50  # Adjust as needed
    )
    response = request.execute()

    for playlist in response["items"]:
        if playlist["snippet"]["title"].lower() == playlist_name.lower():
            return playlist["id"]

    # If no matching playlist found, create a new one
    return create_playlist(youtube, playlist_name)

def create_playlist(youtube, playlist_name):
    print(f"Creating a new playlist for {playlist_name}...")
    request = youtube.playlists().insert(
        part="snippet",
        body={
            "snippet": {
                "title": playlist_name,
                "description": f"Playlist for VODs of {playlist_name}",
            }
        }
    )
    response = request.execute()
    return response["id"]

def add_video_to_playlist(youtube, video_id, playlist_id):
    print(f"Adding video {video_id} to playlist {playlist_id}...")
    request = youtube.playlistItems().insert(
        part="snippet",
        body={
            "snippet": {
                "playlistId": playlist_id,
                "resourceId": {
                    "kind": "youtube#video",
                    "videoId": video_id
                }
            }
        }
    )
    request.execute()
    print(f"Video {video_id} added to playlist {playlist_id}.")

def main():
    print("Checking for new Twitch VODs...")
    
    # Check if the user is live
    if is_user_live():
        print("User is currently live. Skipping the first VOD.")
        skip_first_vod = True
    else:
        skip_first_vod = False

    latest_vods = fetch_vod_details()
    print(f"Number of latest vods: {len(latest_vods)}")
    processed_vods = load_processed_vods()
    print(f"Number of processed vods: {len(processed_vods)}")

    youtube = authenticate_youtube()

    for index, (vod_id, vod_url, vod_title, _) in enumerate(latest_vods):
        # Skip the first VOD if the user is live
        if skip_first_vod and index == 0:
            print("Skipping the first VOD while the user is live.")
            continue

        # Check if VOD has been processed
        if any(vod_id == row[0] for row in processed_vods):
            print(f"VOD {vod_id} already processed.")
            continue

        try:
            print(f"Downloading VOD: {vod_title}")
            vod_path = download_vod(vod_url, vod_id)

            # Extract game name from the VOD title
            print(f"Extracting game name")
            game_name = vod_title.split(" |")[0]  # Get everything before the first " |"

            last_part_number = get_last_part_number_for_game(game_name, processed_vods)

            # Handle Just Chatting and Cooking videos with incremented part numbers
            if game_name.lower() == "just chatting" or game_name.lower() == "cooking":
                # Upload full VOD and increment the part number
                part_number = last_part_number + 1
                print(f"Uploading VOD (game_name: {game_name}) with Part Number {part_number}")

                # Add the part number after the first pipe
                modified_title = f"{game_name} | Part {part_number} | {vod_title.split(' |', 1)[1]}"

                # The description for the full VOD
                description = f"Part {part_number} of Twitch VOD: {game_name}. Broadcasted live on Twitch -- Watch live at https://www.twitch.tv/watcherneil. Uploaded automatically"

                response = upload_to_youtube(youtube, vod_path, modified_title, description)
                playlist_id = get_playlist_id_by_name(youtube, game_name)
                add_video_to_playlist(youtube, response["id"], playlist_id)
                save_processed_vod(vod_id, game_name, part_number)
            else:
                # Split the VOD into segments and upload
                print("Splitting VOD into consistent-length segments...")
                segments = split_vod(vod_path)
                for i, segment in enumerate(segments):
                    part_number = last_part_number + i + 1  # Increment part number for each segment
                    
                    # Add the part number after the first pipe in the title
                    modified_title = f"{vod_title.split(' |', 1)[0]} | Part {part_number} | {vod_title.split(' |', 1)[1]}"

                    # Segment-specific description
                    description = f"Part {part_number} of Twitch VOD: {game_name}. Broadcasted live on Twitch -- Watch live at https://www.twitch.tv/watcherneil. Uploaded automatically"

                    response = upload_to_youtube(youtube, segment, modified_title, description)
                    playlist_id = get_playlist_id_by_name(youtube, game_name)
                    add_video_to_playlist(youtube, response["id"], playlist_id)

                # Save the last part number after uploading all segments
                save_processed_vod(vod_id, game_name, last_part_number + len(segments))

            print(f"VOD {vod_id} processed successfully!")

        except Exception as e:
            print(f"Error processing VOD {vod_id} ({vod_title}): {e}")
            continue  # Skip to the next VOD in the list
    
    # Clear the folders after processing all VODs
    clear_folders()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")