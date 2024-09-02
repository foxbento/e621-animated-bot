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

# Set up logging
log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
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

async def send_telegram_message(bot, post):
    try:
        # Prepare the message text
        artist_tags = ', '.join(post['tags']['artist']) if post['tags']['artist'] else 'Unknown Artist'
        character_tags = ', '.join(post['tags']['character']) if post['tags']['character'] else 'No specific character'
        message_text = f"*Artist:* {artist_tags}\n" \
                       f"*Characters:* {character_tags}\n" \
                       f"*Score:* {post['score']['total']}\n" \
                       f"*Favorites:* {post['fav_count']}\n" \
                       f"[Original Post](https://e621.net/posts/{post['id']})"

        # Get the animation file URL
        file_url = post['file']['url']

        # Send the animation with the caption
        await bot.send_animation(
            chat_id=TELEGRAM_CHANNEL_ID,
            animation=file_url,
            caption=message_text,
            parse_mode=ParseMode.MARKDOWN
        )
        logger.info(f"Successfully sent post {post['id']} to Telegram channel.")
    except TelegramError as e:
        logger.error(f"Failed to send post {post['id']} to Telegram: {e}")

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
        logger.warning("No data retrieved.")

def run_scheduler():
    schedule.every().day.at("00:00").do(lambda: asyncio.run(process_posts()))
    
    while True:
        schedule.run_pending()
        time.sleep(60)  # Sleep for 60 seconds before checking again

if __name__ == "__main__":
    logger.info("Starting scheduler. Press Ctrl+C to exit.")
    try:
        run_scheduler()
    except KeyboardInterrupt:
        logger.info("Scheduler stopped.")
    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)