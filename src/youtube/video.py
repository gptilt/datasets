# import os
# import shutil
# import yt_dlp
# from dagster import asset, Config, MaterializeResult, MetadataValue
# from .resources import R2Resource


# class YouTubeIngestConfig(Config):
#     video_urls: list[str]
#     keep_local: bool = False  # Set to True for debugging

# @asset
# def raw_audio_files(context, config: YouTubeIngestConfig, youtube_bucket: R2Resource):
#     """
#     Downloads audio from YouTube and uploads to Cloudflare R2.
#     Returns metadata about the processed videos.
#     """
    
#     # Create a temporary directory for downloads
#     temp_dir = "temp_downloads"
#     os.makedirs(temp_dir, exist_ok=True)

#     results = []

#     # yt-dlp options for Audio Only
#     ydl_opts = {
#         'format': 'bestaudio/best',
#         'postprocessors': [{
#             'key': 'FFmpegExtractAudio',
#             'preferredcodec': 'm4a', # m4a is excellent for Whisper
#             'preferredquality': '192',
#         }],
#         'outtmpl': f'{temp_dir}/%(id)s.%(ext)s', # Filename = VideoID.m4a
#         'quiet': True,
#         'no_warnings': True,
#     }

#     try:
#         with yt_dlp.YoutubeDL(ydl_opts) as ydl:
#             for url in config.video_urls:
#                 context.log.info(f"Processing: {url}")
                
#                 # A. Extract Info (Metadata)
#                 info = ydl.extract_info(url, download=False)
#                 video_id = info['id']
#                 title = info['title']
#                 uploader = info['uploader']
#                 duration = info['duration']
                
#                 # Define expected filename
#                 filename = f"{video_id}.m4a"
#                 local_path = os.path.join(temp_dir, filename)
                
#                 # B. Download
#                 context.log.info(f"Downloading {title}...")
#                 ydl.download([url])

#                 # C. Upload to R2
#                 context.log.info(f"Uploading to R2...")
#                 r2_path = f"raw_audio/{uploader}/{filename}" # Structured path in bucket
#                 r2.upload_file(local_path, r2_path)
                
#                 # D. Collect Result Data
#                 results.append({
#                     "title": title,
#                     "id": video_id,
#                     "r2_path": r2_path,
#                     "duration": duration
#                 })
                
#                 context.log.info(f"Successfully uploaded {r2_path}")

#     finally:
#         # E. Cleanup Local Files
#         if not config.keep_local:
#             if os.path.exists(temp_dir):
#                 shutil.rmtree(temp_dir)
#                 context.log.info("Cleaned up temporary files.")

#     # Return result with rich metadata for the Dagster UI
#     return MaterializeResult(
#         metadata={
#             "processed_count": len(results),
#             "videos": MetadataValue.json(results),
#             "latest_video": results[-1]["title"] if results else "None"
#         }
#     )