import requests
import json
import html
from telegram import Bot
from telegram.error import TelegramError, TimedOut
import logging
from dotenv import load_dotenv
import os
import re
import tempfile
import asyncio
from datetime import datetime, timedelta, timezone
from moviepy.editor import VideoFileClip
import gc
import warnings
from prometheus_client import start_http_server, Counter, Gauge
from aiohttp import web

# Suppress moviepy warnings
warnings.filterwarnings("ignore", category=UserWarning, module='moviepy')

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Get environment variables
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHANNEL_ID = os.environ.get('TELEGRAM_CHANNEL_ID')
TELEGRAM_CHANNEL_ID_2 = os.environ.get('TELEGRAM_CHANNEL_ID_2')
E621_USERNAME = os.environ.get('E621_USERNAME')

# Verify environment variables
if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL_ID, TELEGRAM_CHANNEL_ID_2, E621_USERNAME]):
    logger.error("One or more required environment variables are missing.")
    exit(1)

# Format chat_ids correctly
for channel_id in [TELEGRAM_CHANNEL_ID, TELEGRAM_CHANNEL_ID_2]:
    if channel_id.startswith('@'):
        channel_id = channel_id
    elif not channel_id.startswith('-100'):
        channel_id = f"@{channel_id}"

# Base URL for e621.net API
BASE_URL = "https://e621.net/posts.json"

# Set up headers
headers = {
    "User-Agent": f"TelegramBot/1.0 (by {E621_USERNAME} on e621)"
}

# Prometheus metrics
POSTS_PROCESSED = Counter('posts_processed', 'Number of posts processed')
POSTS_SENT = Counter('posts_sent', 'Number of posts sent to Telegram')
CONVERSION_ERRORS = Counter('conversion_errors', 'Number of WebM to MP4 conversion errors')
API_ERRORS = Counter('api_errors', 'Number of API errors')
TELEGRAM_ERRORS = Counter('telegram_errors', 'Number of Telegram sending errors')
LAST_RUN_TIMESTAMP = Gauge('last_run_timestamp', 'Timestamp of the last successful run')

POSTS_PROCESSED_2 = Counter('posts_processed_2', 'Number of posts processed for second channel')
POSTS_SENT_2 = Counter('posts_sent_2', 'Number of posts sent to second Telegram channel')
LAST_RUN_TIMESTAMP_2 = Gauge('last_run_timestamp_2', 'Timestamp of the last successful run for second channel')

def load_blacklist():
    """
    Load the blacklist from a JSON file.
    """
    try:
        with open('blacklist.json', 'r') as f:
            return set(json.load(f))
    except FileNotFoundError:
        logger.warning("Blacklist file not found. Using empty blacklist.")
        return set()

blacklist = load_blacklist()

def fetch_e621_posts():
    """
    Fetch posts from e621.net API.
    """
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    params = {
        "tags": f"animated score:>30 date:>={yesterday} rating:e",
        "limit": 320
    }
    try:
        response = requests.get(BASE_URL, params=params, headers=headers)
        response.raise_for_status()
        return response.json()['posts']
    except requests.RequestException as e:
        logger.error(f"Failed to fetch posts: {e}")
        return None

async def fetch_e621_posts_2():
    """
    Fetch posts from e621.net API with different search parameters.
    """
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    params = {
        "tags": f"big_penis score:>30 date:>={yesterday} rating:e",
        "limit": 320
    }
    try:
        response = requests.get(BASE_URL, params=params, headers=headers)
        response.raise_for_status()
        return response.json()['posts']
    except requests.RequestException as e:
        logger.error(f"Failed to fetch posts for second channel: {e}")
        return None

def is_blacklisted(post):
    """
    Check if a post contains any blacklisted tags.
    """
    post_tags = set()
    for tag_category in post['tags'].values():
        post_tags.update(tag_category)
    return bool(post_tags & blacklist)

def escape_markdown(text):
    """
    Escape Markdown special characters in a string.
    """
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

def convert_webm_to_mp4(webm_url):
    """
    Convert a WebM file to MP4 format.
    """
    try:
        with tempfile.NamedTemporaryFile(suffix='.webm', delete=False) as temp_webm:
            response = requests.get(webm_url, stream=True)
            response.raise_for_status()
            for chunk in response.iter_content(chunk_size=8192):
                temp_webm.write(chunk)
            temp_webm_path = temp_webm.name

        mp4_path = temp_webm_path.rsplit('.', 1)[0] + '.mp4'
        with VideoFileClip(temp_webm_path) as video:
            video.write_videofile(mp4_path, codec='libx264', audio_codec='aac')

        gc.collect()

        return mp4_path
    except Exception as e:
        logger.error(f"Error converting WebM to MP4: {str(e)}")
        return None
    finally:
        if 'temp_webm_path' in locals() and os.path.exists(temp_webm_path):
            os.remove(temp_webm_path)

async def send_telegram_message(bot, post, channel_id, is_second_channel=False):
    """
    Send a post to the specified Telegram channel.
    """
    try:
        artist_tags = ', '.join(post['tags']['artist']) if post['tags']['artist'] else 'Unknown Artist'
        character_tags = ', '.join(post['tags']['character']) if post['tags']['character'] else 'No specific character'
        
        artist_tags = escape_markdown(artist_tags)
        character_tags = escape_markdown(character_tags)
        
        message_text = f"*Artist:* {artist_tags}\n" \
                       f"*Characters:* {character_tags}\n" \
                       f"*Score:* {post['score']['total']}\n" \
                       f"*Favorites:* {post['fav_count']}\n" \
                       f"[Original Post](https://e621.net/posts/{post['id']})"
        
        logger.debug(f"Final message text: {message_text}")

        file_url = post['file']['url']
        file_size = post['file']['size']
        logger.debug(f"File URL: {file_url}")
        logger.debug(f"File size: {file_size} bytes")

        if file_url.lower().endswith('.webm'):
            logger.info(f"WebM file detected for post {post['id']}. Converting to MP4...")
            mp4_path = convert_webm_to_mp4(file_url)
            if mp4_path:
                with open(mp4_path, 'rb') as video_file:
                    await bot.send_video(
                        chat_id=channel_id,
                        video=video_file,
                        caption=message_text,
                        parse_mode='MarkdownV2',
                        supports_streaming=True
                    )
                os.remove(mp4_path)
                logger.info(f"Successfully sent converted MP4 for post {post['id']} to Telegram channel.")
                if is_second_channel:
                    POSTS_SENT_2.inc()
                else:
                    POSTS_SENT.inc()
            else:
                logger.error(f"Failed to convert WebM to MP4 for post {post['id']}. Skipping this post.")
                CONVERSION_ERRORS.inc()
                return
        else:
            try:
                await bot.send_video(
                    chat_id=channel_id,
                    video=file_url,
                    caption=message_text,
                    parse_mode='MarkdownV2',
                    supports_streaming=True
                )
                logger.info(f"Successfully sent video for post {post['id']} to Telegram channel.")
                if is_second_channel:
                    POSTS_SENT_2.inc()
                else:
                    POSTS_SENT.inc()
            except TelegramError as e:
                logger.warning(f"Failed to send as video for post {post['id']}, falling back to animation. Error: {e}")
                await bot.send_animation(
                    chat_id=channel_id,
                    animation=file_url,
                    caption=message_text,
                    parse_mode='MarkdownV2'
                )
                logger.info(f"Successfully sent animation for post {post['id']} to Telegram channel.")
                if is_second_channel:
                    POSTS_SENT_2.inc()
                else:
                    POSTS_SENT.inc()

    except TimedOut as e:
        logger.error(f"Timed out sending post {post['id']} to Telegram. File size: {file_size} bytes. Error: {e}")
        TELEGRAM_ERRORS.inc()
    except TelegramError as e:
        logger.error(f"Failed to send post {post['id']} to Telegram. File size: {file_size} bytes. Error: {e}")
        TELEGRAM_ERRORS.inc()
    except Exception as e:
        logger.error(f"Unexpected error sending post {post['id']} to Telegram: {e}")
        TELEGRAM_ERRORS.inc()

async def process_posts(is_second_channel=False):
    """
    Fetch posts from e621.net and send them to the appropriate Telegram channel.
    """
    logger.info(f"Running scheduled task for {'second' if is_second_channel else 'first'} channel at {datetime.now(timezone.utc).isoformat()}")
    posts = await fetch_e621_posts_2() if is_second_channel else fetch_e621_posts()
    if posts:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        filtered_posts = [post for post in posts if not is_blacklisted(post)]
        logger.info(f"Found {len(filtered_posts)} posts after blacklisting (out of {len(posts)} total) for {'second' if is_second_channel else 'first'} channel")
        for post in filtered_posts:
            try:
                await send_telegram_message(bot, post, TELEGRAM_CHANNEL_ID_2 if is_second_channel else TELEGRAM_CHANNEL_ID, is_second_channel)
                if is_second_channel:
                    POSTS_PROCESSED_2.inc()
                else:
                    POSTS_PROCESSED.inc()
            except Exception as e:
                logger.error(f"Failed to process post {post['id']} for {'second' if is_second_channel else 'first'} channel: {str(e)}")
                API_ERRORS.inc()
            await asyncio.sleep(5)  # Add a delay to avoid rate limiting
        if is_second_channel:
            LAST_RUN_TIMESTAMP_2.set_to_current_time()
        else:
            LAST_RUN_TIMESTAMP.set_to_current_time()
    else:
        logger.error(f"Failed to fetch posts for {'second' if is_second_channel else 'first'} channel, cannot proceed with scheduled task.")
        API_ERRORS.inc()

async def run_scheduler():
    """
    Run the scheduler to process posts at specified times for both channels.
    """
    utc_tz = timezone.utc
    nz_offset = timedelta(hours=12)  # New Zealand is typically UTC+12
    
    while True:
        now_utc = datetime.now(utc_tz)
        now_nz = now_utc + nz_offset
        
        # Check for UTC midnight (first channel)
        if now_utc.hour == 0 and now_utc.minute == 0:
            try:
                await process_posts(is_second_channel=False)
            except Exception as e:
                logger.error(f"Error occurred during scheduled task for first channel: {e}")
                API_ERRORS.inc()
        
        # Check for NZ midnight (second channel)
        if now_nz.hour == 0 and now_nz.minute == 0:
            try:
                await process_posts(is_second_channel=True)
            except Exception as e:
                logger.error(f"Error occurred during scheduled task for second channel: {e}")
                API_ERRORS.inc()
        
        await asyncio.sleep(60)  # Sleep for 60 seconds before checking again

async def start_metrics_server():
    """
    Start the Prometheus metrics server.
    """
    app = web.Application()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8000)
    await site.start()
    logger.info("Metrics server started on port 8000")

async def main():
    """
    Main function to run both the scheduler and metrics server concurrently.
    """
    await asyncio.gather(
        run_scheduler(),
        start_metrics_server()
    )

if __name__ == "__main__":
    logger.info("Starting application")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application stopped.")
    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}", exc_info=True)
        exit(1)