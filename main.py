import gspread
from googleapiclient.discovery import build
import time
import os

import json

service_account_info = json.loads(os.getenv("SERVICE_ACCOUNT"))
gc = gspread.service_account_from_dict(service_account_info)

# Function to fetch YouTube videos from a channel
def get_channel_name(youtube, channel_id):
    request = youtube.channels().list(
        part="snippet",
        id=channel_id
    )
    response = request.execute()
    return response['items'][0]['snippet']['title']

def get_youtube_videos(api_key, channel_id, max_retries=3, delay=1):
    youtube = build('youtube', 'v3', developerKey=api_key)
    video_data = []
    next_page_token = None
    retry_count = 0

    while True:
        try:
            # Fetch the list of videos from the channel
            request = youtube.search().list(
                part="id,snippet",
                channelId=channel_id,
                maxResults=50,
                order="date",
                type="video",
                pageToken=next_page_token
            )
            response = request.execute()

            video_ids = []
            for item in response.get('items', []):
                if 'videoId' in item.get('id', {}):
                    video_ids.append(item['id']['videoId'])

            if not video_ids:
                break

            # Split video IDs into chunks to avoid exceeding quota
            chunk_size = 25
            for i in range(0, len(video_ids), chunk_size):
                chunk = video_ids[i:i + chunk_size]

                # Retry logic for video details
                for _ in range(max_retries):
                    try:
                        video_details = youtube.videos().list(
                            part="snippet,statistics,contentDetails",
                            id=','.join(chunk)
                        ).execute()

                        for video in video_details.get("items", []):
                            title = video["snippet"]["title"]
                            video_url = f'https://www.youtube.com/watch?v={video["id"]}'
                            views = video["statistics"].get("viewCount", "0")
                            duration = video["contentDetails"].get("duration", "")

                            # Convert views to human readable format
                            view_count = int(views)
                            if view_count >= 1000000:
                                human_views = f"{view_count/1000000:.1f}M"
                            elif view_count >= 1000:
                                human_views = f"{view_count/1000:.1f}K"
                            else:
                                human_views = str(view_count)

                            video_data.append([title, video_url, views, human_views])
                        break
                    except Exception as e:
                        print(f"Retry {_ + 1}/{max_retries} for chunk {i//chunk_size + 1}: {str(e)}")
                        time.sleep(2 ** _)  # Exponential backoff

                time.sleep(delay)  # Rate limiting

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break

            time.sleep(delay)  # Rate limiting
            retry_count = 0  # Reset retry count on successful iteration

        except Exception as e:
            retry_count += 1
            if retry_count >= max_retries:
                print(f"Max retries reached. Error: {str(e)}")
                break
            print(f"Retry {retry_count}/{max_retries}: {str(e)}")
            time.sleep(2 ** retry_count)  # Exponential backoff

    # Sort videos by views (descending order)
    sorted_video_data = sorted(video_data, key=lambda x: int(x[2]), reverse=True)
    return sorted_video_data

# Function to upload data to Google Sheets in a specified sheet
def upload_to_sheets(sheet_id, sheet_name, data, delay=1): #Added delay parameter
    # Open the Google Sheets document by its ID
    sheet = gc.open_by_key(sheet_id)

    try:
        # Try to access the sheet by its name (sheet_name)
        sheet_to_upload = sheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Sheet '{sheet_name}' not found. Creating new sheet...")
        sheet_to_upload = sheet.add_worksheet(sheet_name, rows=1000, cols=20)

    # Clear existing data before uploading
    sheet_to_upload.clear()

    # Add headers once
    sheet_to_upload.append_row(["Title", "Video Link", "View Count", "Round Off View Count"])

    # Split data into batches (e.g., 50 videos per batch)
    batch_size = 50
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        sheet_to_upload.append_rows(batch)  # Append multiple rows in a single request
        time.sleep(delay)  # Add a slight delay to prevent hitting the rate limit

    print(f"Upload complete to '{sheet_name}'!")

# Main script execution
if __name__ == "__main__":
    API_KEY = os.getenv("API_KEY") # Replace with your YouTube API key
    CHANNEL_ID = "UCX6OQ3DkcsbYNE6H8uQQuVA"  # Replace with your YouTube Channel ID
    SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")


    youtube = build('youtube', 'v3', developerKey=API_KEY)
    channel_name = get_channel_name(youtube, CHANNEL_ID)
    print(f"Fetching videos for channel: {channel_name}...")
    videos = get_youtube_videos(API_KEY, CHANNEL_ID)

    if videos:
        print(f"Uploading {len(videos)} videos to Google Sheets...")
        upload_to_sheets(SPREADSHEET_ID, channel_name, videos)
        print("Upload complete!")
    else:
        print("No videos found or API limit reached.")
