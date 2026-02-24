from .constants import AUDIO_EXTENSION, DATASET_NAME, CHANNEL_IDS
import dagster as dg
from ds_storage import StorageS3
import os
import tempfile
import yt_dlp
from yt_dlp.utils import DateRange


# Define partitions
partition_per_channel = dg.StaticPartitionsDefinition(list(CHANNEL_IDS['audio'].keys()))
partition_per_week = dg.WeeklyPartitionsDefinition(start_date="2024-01-01")
partition_per_channel_per_week = dg.MultiPartitionsDefinition({
    "channel": partition_per_channel,
    "time": partition_per_week,
})


@dg.asset(
    name="raw_youtube_audio",
    group_name=DATASET_NAME,
    partitions_def=partition_per_channel_per_week,
)
def asset_raw_youtube_audio(
    context: dg.AssetExecutionContext,
    youtube_bucket: StorageS3
):
    """
    Downloads audio from YouTube and uploads to an S3 storage.
    Returns metadata about the processed videos.
    """
    # Extract Partition Keys
    # Multi-partitions require extracting the specific dimensions
    partition_keys: dg.MultiPartitionKey = context.partition_key.keys_by_dimension
    channel_name = partition_keys["channel"]
    channel_id = CHANNEL_IDS['audio'].get(channel_name)

    # Extract Time Window and format for yt-dlp (YYYYMMDD)
    time_window = context.partition_time_window
    start_date_str = time_window.start.strftime('%Y%m%d')
    end_date_str = time_window.end.strftime('%Y%m%d')

    url = f"https://www.youtube.com/channel/{channel_id}/videos"

    # Create a temporary directory for downloads
    with tempfile.TemporaryDirectory() as temp_dir:
        results = []

        # yt-dlp options for Audio Only
        ydl_opts = {
            'format': 'bestaudio/best',
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': AUDIO_EXTENSION,
                'preferredquality': '192',
            }],
            # Filename = VideoID.m4a
            'outtmpl': os.path.join(temp_dir, '%(id)s.%(ext)s'),
            'quiet': True,
            'no_warnings': True,
            # Date Range for yt-dlp
            'daterange': DateRange(start_date_str, end_date_str)
        }

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            context.log.info(f"Processing: {url} from {start_date_str} to {end_date_str}")

            # Extract Info (Metadata)
            info = ydl.extract_info(url, download=False)
            entries = info['entries']

            # Iterate through downloaded videos and Upload to bucket
            for entry in entries:
                if not entry:
                    # yt-dlp sometimes yields None for hidden/deleted videos
                    continue
                
                video_id = entry.get('id')
                title = entry.get('title')
                upload_date = entry.get('upload_date')

                # Define expected filename
                object_name = f"{video_id}.{AUDIO_EXTENSION}"
                local_path = os.path.join(temp_dir, object_name)

                # We pass the already-extracted dictionary to avoid re-fetching metadata
                ydl.process_ie_result(entry, download=True)

                # Sanity check in case the download failed but didn't throw an error
                if not os.path.exists(local_path):
                    context.log.warning(f"File not found for {title}, skipping: {local_path}")
                    continue

                # Upload to bucket
                context.log.info(f"Uploading {title} (upload_date={upload_date}) to S3...")
                youtube_bucket.upload_file(
                    local_path,
                    "audio",
                    object_name,
                    channel_id=channel_id
                )

                # Clean up local file to save space
                os.remove(local_path)
                context.log.info(f"Cleaned up local file for {title}")

                # Collect Result Data
                results.append({
                    "title": title,
                    "id": video_id,
                    "upload_date": upload_date,
                })

    # Return result with rich metadata for the Dagster UI
    return dg.MaterializeResult(
        metadata={
            "processed_count": len(results),
            "videos": dg.MetadataValue.json(results),
            "latest_video": results[-1]["title"] if results else "None"
        }
    )