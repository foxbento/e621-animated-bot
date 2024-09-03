import requests
import json
from datetime import datetime, timedelta, timezone
import schedule
import time
from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TelegramError
import asyncio
import logging
import sys
import os
from moviepy.editor import VideoFileClip
import tempfile

# Set up logging
log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('bot.log')  # Add file logging
    ]
)
logger = logging.getLogger(__name__)

# Base URL for e621.net API
BASE_URL = "https://e621.net/posts.json"

# Set up headers
headers = {
    "User-Agent": "dahlia_bot/1.0 (by dahlia_ad on e621)"
}

# File path for the blacklist JSON
BLACKLIST_FILE = "blacklist.json"

# Telegram Bot Token and Channel ID
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHANNEL_ID = os.environ.get('TELEGRAM_CHANNEL_ID')

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL_ID:
    logger.error("Telegram bot token or channel ID not set in environment variables")
    sys.exit(1)

def load_blacklist():
    try:
        with open(BLACKLIST_FILE, 'r') as f:
            blacklist = json.load(f)
        if not isinstance(blacklist, list):
            raise ValueError("Blacklist must be a list of tags")
        return set(blacklist)
    except FileNotFoundError:
        logger.warning(f"Blacklist file '{BLACKLIST_FILE}' not found. Creating an empty blacklist.")
        return set()
    except json.JSONDecodeError:
        logger.error(f"Error decoding '{BLACKLIST_FILE}'. Please ensure it's valid JSON. Using an empty blacklist.")
        return set()
    except Exception as e:
        logger.error(f"An error occurred while loading the blacklist: {e}. Using an empty blacklist.")
        return set()

# Load the blacklist
blacklist_tags = load_blacklist()

def fetch_posts():
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    params = {
        "tags": f"animated score:>30 date:>={yesterday}",
        "limit": 320  # Maximum allowed limit
    }
    try:
        response = requests.get(BASE_URL, params=params, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"An error occurred while fetching posts: {e}")
        return None

def is_blacklisted(post, blacklist):
    post_tags = set()
    for tag_category in post['tags'].values():
        post_tags.update(tag_category)
    return bool(post_tags & blacklist)

def convert_webm_to_mp4(webm_url):
    try:
        with tempfile.NamedTemporaryFile(suffix='.webm', delete=False) as temp_webm:
            # Download the WebM file
            response = requests.get(webm_url)
            response.raise_for_status()
            temp_webm.write(response.content)
            temp_webm_path = temp_webm.name

        # Convert to MP4
        mp4_path = temp_webm_path.rsplit('.', 1)[0] + '.mp4'
        video = VideoFileClip(temp_webm_path)
        video.write_videofile(mp4_path, codec='libx264')
        video.close()

        return mp4_path
    except Exception as e:
        logger.error(f"Error converting WebM to MP4: {e}")
        return None
    finally:
        # Clean up the temporary WebM file
        if os.path.exists(temp_webm_path):
            os.remove(temp_webm_path)

async def send_telegram_message(bot, post):
    post_id = post['id']
    file_url = post['file']['url']
    file_ext = os.path.splitext(file_url)[1].lower()

    try:
        # Prepare the message text
        artist_tags = ', '.join(post['tags']['artist']) if post['tags']['artist'] else 'Unknown Artist'
        character_tags = ', '.join(post['tags']['character']) if post['tags']['character'] else 'No specific character'
        message_text = f"*Artist:* {artist_tags}\n" \
                       f"*Characters:* {character_tags}\n" \
                       f"*Score:* {post['score']['total']}\n" \
                       f"*Favorites:* {post['fav_count']}\n" \
                       f"[Original Post](https://e621.net/posts/{post_id})"

        if file_ext == '.webm':
            logger.info(f"WebM file detected for post {post_id}. Converting to MP4...")
            mp4_path = convert_webm_to_mp4(file_url)
            if mp4_path:
                with open(mp4_path, 'rb') as video_file:
                    await bot.send_video(
                        chat_id=TELEGRAM_CHANNEL_ID,
                        video=video_file,
                        caption=message_text,
                        parse_mode=ParseMode.MARKDOWN,
                        supports_streaming=True
                    )
                os.remove(mp4_path)  # Clean up the converted file
                logger.info(f"Successfully sent converted MP4 for post {post_id} to Telegram channel.")
            else:
                logger.error(f"Failed to convert WebM to MP4 for post {post_id}. Skipping this post.")
                return
        else:
            # Try to send as video first
            try:
                await bot.send_video(
                    chat_id=TELEGRAM_CHANNEL_ID,
                    video=file_url,
                    caption=message_text,
                    parse_mode=ParseMode.MARKDOWN,
                    supports_streaming=True
                )
                logger.info(f"Successfully sent video for post {post_id} to Telegram channel.")
            except TelegramError as e:
                logger.warning(f"Failed to send as video for post {post_id}, falling back to animation. Error: {e}")
                # Fall back to sending as animation
                await bot.send_animation(
                    chat_id=TELEGRAM_CHANNEL_ID,
                    animation=file_url,
                    caption=message_text,
                    parse_mode=ParseMode.MARKDOWN
                )
                logger.info(f"Successfully sent animation for post {post_id} to Telegram channel.")
    except TelegramError as e:
        logger.error(f"Failed to send post {post_id} to Telegram. Error: {e}")
        logger.error(f"Problematic post details: ID: {post_id}, URL: {file_url}, File type: {file_ext}")
    except Exception as e:
        logger.error(f"Unexpected error while processing post {post_id}: {e}")
        logger.error(f"Problematic post details: ID: {post_id}, URL: {file_url}, File type: {file_ext}")

async def process_posts():
    logger.info(f"Running scheduled task at {datetime.now(timezone.utc).isoformat()}")
    data = fetch_posts()
    if data:
        all_posts = data.get('posts', [])
        filtered_posts = [post for post in all_posts if not is_blacklisted(post, blacklist_tags)]
        
        logger.info(f"Found {len(filtered_posts)} posts after blacklisting (out of {len(all_posts)} total):")
        
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        for post in filtered_posts:
            await send_telegram_message(bot, post)
            # Add a delay to avoid hitting rate limits
            await asyncio.sleep(5)
    else:
        logger.warning("No data retrieved from e621.")

async def run_scheduler():
    while True:
        now = datetime.now(timezone.utc)
        if now.hour == 0 and now.minute == 0:
            try:
                await process_posts()
            except Exception as e:
                logger.error(f"Error occurred during scheduled task: {e}")
        await asyncio.sleep(60)  # Sleep for 60 seconds before checking again

if __name__ == "__main__":
    logger.info("Starting scheduler. Press Ctrl+C to exit.")
    try:
        asyncio.run(run_scheduler())
    except KeyboardInterrupt:
        logger.info("Scheduler stopped.")
    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)