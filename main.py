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
E621_USERNAME = os.environ.get('E621_USERNAME')

# Verify environment variables
if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL_ID or not E621_USERNAME:
    logger.error("One or more required environment variables are missing.")
    exit(1)

# Format chat_id correctly
if TELEGRAM_CHANNEL_ID.startswith('@'):
    TELEGRAM_CHANNEL_ID = TELEGRAM_CHANNEL_ID
elif not TELEGRAM_CHANNEL_ID.startswith('-100'):
    TELEGRAM_CHANNEL_ID = f"@{TELEGRAM_CHANNEL_ID}"

# Base URL for e621.net API
BASE_URL = "https://e621.net/posts.json"

# Set up headers
headers = {
    "User-Agent": f"TelegramBot/1.0 (by {E621_USERNAME} on e621)"
}

def load_blacklist():
    """
    Load the blacklist from a JSON file.
    
    Returns:
        set: A set of blacklisted tags.
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
    
    Returns:
        list: A list of post dictionaries, or None if the request fails.
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

def is_blacklisted(post):
    """
    Check if a post contains any blacklisted tags.
    
    Args:
        post (dict): A post dictionary from e621.net API.
    
    Returns:
        bool: True if the post contains a blacklisted tag, False otherwise.
    """
    post_tags = set()
    for tag_category in post['tags'].values():
        post_tags.update(tag_category)
    return bool(post_tags & blacklist)

def escape_markdown(text):
    """
    Escape Markdown special characters in a string.
    
    Args:
        text (str): The text to escape.
    
    Returns:
        str: The escaped text.
    """
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

def convert_webm_to_mp4(webm_url):
    """
    Convert a WebM file to MP4 format.
    
    Args:
        webm_url (str): The URL of the WebM file.
    
    Returns:
        str: The path to the converted MP4 file, or None if conversion fails.
    """
    try:
        with tempfile.NamedTemporaryFile(suffix='.webm', delete=False) as temp_webm:
            # Download the WebM file
            response = requests.get(webm_url, stream=True)
            response.raise_for_status()
            for chunk in response.iter_content(chunk_size=8192):
                temp_webm.write(chunk)
            temp_webm_path = temp_webm.name

        # Convert to MP4
        mp4_path = temp_webm_path.rsplit('.', 1)[0] + '.mp4'
        with VideoFileClip(temp_webm_path) as video:
            video.write_videofile(mp4_path, codec='libx264', audio_codec='aac')

        # Force garbage collection
        gc.collect()

        return mp4_path
    except Exception as e:
        logger.error(f"Error converting WebM to MP4: {str(e)}")
        return None
    finally:
        # Clean up the temporary WebM file
        if 'temp_webm_path' in locals() and os.path.exists(temp_webm_path):
            os.remove(temp_webm_path)

async def send_telegram_message(bot, post):
    """
    Send a post to the Telegram channel.
    
    Args:
        bot (telegram.Bot): The Telegram bot instance.
        post (dict): A post dictionary from e621.net API.
    """
    try:
        # Prepare the message text
        artist_tags = ', '.join(post['tags']['artist']) if post['tags']['artist'] else 'Unknown Artist'
        character_tags = ', '.join(post['tags']['character']) if post['tags']['character'] else 'No specific character'
        
        # Escape special characters for Markdown formatting
        artist_tags = escape_markdown(artist_tags)
        character_tags = escape_markdown(character_tags)
        
        message_text = f"*Artist:* {artist_tags}\n" \
                       f"*Characters:* {character_tags}\n" \
                       f"*Score:* {post['score']['total']}\n" \
                       f"*Favorites:* {post['fav_count']}\n" \
                       f"[Original Post](https://e621.net/posts/{post['id']})"
        
        logger.debug(f"Final message text: {message_text}")

        # Get the animation file URL and size
        file_url = post['file']['url']
        file_size = post['file']['size']
        logger.debug(f"File URL: {file_url}")
        logger.debug(f"File size: {file_size} bytes")

        # Check if the file is WebM and convert if necessary
        if file_url.lower().endswith('.webm'):
            logger.info(f"WebM file detected for post {post['id']}. Converting to MP4...")
            mp4_path = convert_webm_to_mp4(file_url)
            if mp4_path:
                with open(mp4_path, 'rb') as video_file:
                    await bot.send_video(
                        chat_id=TELEGRAM_CHANNEL_ID,
                        video=video_file,
                        caption=message_text,
                        parse_mode='MarkdownV2',
                        supports_streaming=True
                    )
                os.remove(mp4_path)  # Clean up the converted file
                logger.info(f"Successfully sent converted MP4 for post {post['id']} to Telegram channel.")
            else:
                logger.error(f"Failed to convert WebM to MP4 for post {post['id']}. Skipping this post.")
                return
        else:
            # Try to send as video first
            try:
                await bot.send_video(
                    chat_id=TELEGRAM_CHANNEL_ID,
                    video=file_url,
                    caption=message_text,
                    parse_mode='MarkdownV2',
                    supports_streaming=True
                )
                logger.info(f"Successfully sent video for post {post['id']} to Telegram channel.")
            except TelegramError as e:
                logger.warning(f"Failed to send as video for post {post['id']}, falling back to animation. Error: {e}")
                # Fall back to sending as animation
                await bot.send_animation(
                    chat_id=TELEGRAM_CHANNEL_ID,
                    animation=file_url,
                    caption=message_text,
                    parse_mode='MarkdownV2'
                )
                logger.info(f"Successfully sent animation for post {post['id']} to Telegram channel.")

    except TimedOut as e:
        logger.error(f"Timed out sending post {post['id']} to Telegram. File size: {file_size} bytes. Error: {e}")
    except TelegramError as e:
        logger.error(f"Failed to send post {post['id']} to Telegram. File size: {file_size} bytes. Error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error sending post {post['id']} to Telegram: {e}")

async def process_posts():
    """
    Fetch posts from e621.net and send them to the Telegram channel.
    """
    logger.info(f"Running scheduled task at {datetime.now(timezone.utc).isoformat()}")
    posts = fetch_e621_posts()
    if posts:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        filtered_posts = [post for post in posts if not is_blacklisted(post)]
        logger.info(f"Found {len(filtered_posts)} posts after blacklisting (out of {len(posts)} total)")
        for post in filtered_posts:
            try:
                await send_telegram_message(bot, post)
            except Exception as e:
                logger.error(f"Failed to process post {post['id']}: {str(e)}")
            await asyncio.sleep(5)  # Add a delay to avoid rate limiting
    else:
        logger.error("Failed to fetch posts, cannot proceed with scheduled task.")

async def run_scheduler():
    """
    Run the scheduler to process posts at midnight UTC daily.
    """
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
        exit(1)