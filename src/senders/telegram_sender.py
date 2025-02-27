"""Telegram message formatting and sending service"""

import logging
import html
import telegram
from telegram import Bot
from telegram.constants import ParseMode
import asyncio
import os
from pathlib import Path
import json
from datetime import datetime
import sys
from utils.error_handler import RetryConfig, with_retry, TelegramError, log_error, DataProcessingError
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Reduce httpx logging
logging.getLogger('httpx').setLevel(logging.WARNING)

# Load environment variables before importing category_mapping
load_dotenv()

# Import after loading environment variables
from category_mapping import TELEGRAM_CHANNELS, EMOJI_MAP, CATEGORY

class TelegramSender:
    def __init__(self, bot_token):
        if not bot_token:
            raise ValueError("Bot token is required")
            
        self.bot = Bot(token=bot_token)
        self.used_emojis = set()  # Track used emojis per summary
        
        # Initialize data directories
        self.data_dir = Path('data')
        self.input_dir = self.data_dir / 'filtered' / 'news_filtered'  # Input from news_filter
        
        # Create directories if they don't exist
        self.input_dir.mkdir(parents=True, exist_ok=True)
        
        # Debug: Print environment variables and channel IDs
        logger.error(f"DEBUG - Environment Variables:")
        for name, channel_id in TELEGRAM_CHANNELS.items():
            logger.error(f"Channel {name}: ID = '{channel_id}' (type: {type(channel_id)})")

    def _reset_used_emojis(self):
        """Reset the used emojis tracking set"""
        self.used_emojis.clear()

    async def format_text(self, text):
        """Format text with HTML tags according to instructions"""
        if not text:
            logger.warning("Received empty text to format")
            return ""
            
        # Skip if text is already formatted (contains HTML tags)
        if any(tag in text for tag in ['<u>', '<b>', '<i>', '<a href']):
            return text
            
        try:
            lines = text.split('\n')
            formatted_lines = []
            
            i = 0
            while i < len(lines):
                line = lines[i].strip()
                if not line:
                    i += 1
                    continue
                    
                # Format header (Date - Category Rollup)
                if ' - ' in line and 'Rollup' in line:
                    try:
                        date_str, rest = line.split(' - ', 1)
                        date_obj = datetime.strptime(date_str.strip(), '%Y%m%d')
                        formatted_date = date_obj.strftime('%B %d')
                        formatted_header = f"{formatted_date} - {rest.replace('Rollup', 'News Drop')}"
                        formatted_lines.append(f"<u><b><i>{html.escape(formatted_header)}</i></b></u>")
                    except ValueError as e:
                        log_error(logger, e, f"Failed to parse date: {date_str}")
                        # Fallback to original format if date conversion fails
                        formatted_lines.append(f"<u><b><i>{html.escape(line)}</i></b></u>")
                    i += 1
                    continue
                    
                # Format subcategory with emoji
                if not ':' in line and not line.startswith('http'):
                    if formatted_lines:
                        formatted_lines.append('')
                    formatted_lines.append(f"<u><b>{html.escape(line)}</b></u>")
                    i += 1
                    continue
                    
                # Format tweet lines with URL on next line
                if not line.startswith('http'):
                    try:
                        # Add line as is since formatting is handled in format_category_summary
                        formatted_lines.append(html.escape(line))
                        i += 1
                    except Exception as e:
                        log_error(logger, e, f"Failed to format tweet line: {line}")
                        formatted_lines.append(line)
                        i += 1
                    continue
                    
                # Keep URLs as is but prevent auto-embedding
                if line.startswith('http'):
                    formatted_lines.append(line.replace('https:', 'https:\u200B'))
                    i += 1
                    continue
                    
                # Default case - keep line as is
                formatted_lines.append(html.escape(line))
                i += 1
                
            return '\n'.join(formatted_lines)
            
        except Exception as e:
            log_error(logger, e, "Failed to format text")
            raise TelegramError(f"Text formatting failed: {str(e)}")

    @with_retry(RetryConfig(max_retries=3, base_delay=1.0))
    async def send_message(self, channel_id: str, text: str) -> bool:
        """Send a message to a Telegram channel with retry logic"""
        if not text:
            return False
            
        if not channel_id:
            logger.error("No channel ID provided")
            return False
            
        try:
            formatted_text = await self.format_text(text)
            if not formatted_text:
                logger.error("Empty formatted text")
                return False
                
            await self.bot.send_message(
                chat_id=channel_id,
                text=formatted_text,
                parse_mode='HTML',
                disable_web_page_preview=True
            )
            return True
            
        except telegram.error.TimedOut:
            logger.warning(f"Timeout sending message to channel {channel_id[:8]}... (Retrying)")
            raise TelegramError("Connection timed out")
        except telegram.error.RetryAfter as e:
            logger.warning(f"Rate limit hit, retry after {e.retry_after}s")
            await asyncio.sleep(e.retry_after)
            raise TelegramError(f"Rate limited, retry after {e.retry_after}s")
        except telegram.error.TelegramError as e:
            logger.error(f"Telegram API error: {str(e)}")
            raise TelegramError(f"Telegram API error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error sending message: {str(e)}")
            raise TelegramError(f"Failed to send message: {str(e)}")

    async def process_category(self, category: str, content: dict):
        """Process and send a category summary to Telegram"""
        try:
            channel_id = TELEGRAM_CHANNELS[category]
            if not await self._validate_channel(channel_id):
                logger.error(f"Skipping invalid channel: {channel_id}")
                return False
            
            formatted_text = await self.format_text(content['content'])
            if not formatted_text:
                logger.error(f"Empty content for {category}")
                return False
            
            return await self.send_message(channel_id, formatted_text)
        
        except KeyError as e:
            logger.error(f"Category {category} not found in channel map")
            return False
        except Exception as e:
            log_error(logger, e, f"Error processing {category}")
            return False

    async def _validate_channel(self, channel_id: str) -> bool:
        try:
            chat = await self.bot.get_chat(chat_id=channel_id)
            return chat.type in ["channel", "supergroup"]
        except telegram.error.BadRequest:
            logger.error(f"Invalid channel ID: {channel_id}")
            return False

    def _get_emoji_for_subcategory(self, subcategory: str) -> str:
        """Get unique emoji for subcategory by matching individual words"""
        try:
            # Split subcategory into words and clean them
            words = set(word.strip() for word in subcategory.split())
            
            # Find all matching emojis that haven't been used yet
            available_emojis = []
            for word in words:
                if emoji := EMOJI_MAP.get(word):
                    if emoji not in self.used_emojis:
                        available_emojis.append(emoji)
            
            # If we found an unused emoji, use it
            if available_emojis:
                chosen_emoji = available_emojis[0]
                self.used_emojis.add(chosen_emoji)
                return chosen_emoji
            
            # If all matching emojis are used, find first unused emoji from default set
            default_emojis = ['📌', '📍', '🔖', '🏷️', '💠', '🔸', '🔹', '🔰']
            for emoji in default_emojis:
                if emoji not in self.used_emojis:
                    self.used_emojis.add(emoji)
                    return emoji
                    
            # If somehow all emojis are used, return the last default emoji
            return default_emojis[-1]
            
        except Exception as e:
            logger.error(f"Error getting emoji for {subcategory}: {str(e)}")
            return '📌'

    async def format_category_summary(self, category: str, summary: dict, channel_username: str = None) -> str:
        """Format category summary into a Telegram message"""
        try:
            # Reset used emojis for new summary
            self._reset_used_emojis()
            
            # Find the actual category key that matches case-insensitively
            category_key = None
            for key in summary.keys():
                if key.lower() == category.lower():
                    category_key = key
                    break
                    
            logger.error(f"DEBUG - category_key: {category_key}, summary keys: {list(summary.keys())}")  # Temporary debug
            
            if not summary or not category_key:
                logger.error(f"Invalid summary format for {category}")
                return ""
            
            category_data = summary[category_key]
            logger.error(f"DEBUG - subcategories: {list(category_data.keys())}")  # Temporary debug
            
            if not isinstance(category_data, dict):
                logger.error(f"Invalid category data format for {category}")
                return ""
            
            # Build message with header using the original category key to preserve case
            lines = [f"<u><b><i>{category_key} News Drop</i></b></u> ~\n"]
            
            # Add each subcategory and its tweets
            for subcategory, tweets in category_data.items():
                try:
                    # Add newline before each subcategory (except the first one)
                    if len(lines) > 1:  # If not first subcategory
                        lines.append("")  # Add newline before subcategory
                    
                    # Get unique emoji for subcategory
                    emoji = self._get_emoji_for_subcategory(subcategory)
                    logger.error(f"DEBUG - Processing {subcategory} with {len(tweets)} tweets")  # Temporary debug
                    
                    # Add subcategory header with emoji
                    subcategory_text = f"<u><b>{subcategory}</b></u>"
                    if emoji:
                        subcategory_text += f" {emoji}"
                    lines.append(subcategory_text)
                    
                    # Group tweets by attribution
                    attribution_tweets = {}
                    for tweet in tweets:
                        attribution = tweet.get('attribution', '')
                        content = tweet.get('content', '')
                        url = tweet.get('url', '')
                        
                        if attribution and content and url:
                            if attribution not in attribution_tweets:
                                attribution_tweets[attribution] = []
                            attribution_tweets[attribution].append((content, url))
                    
                    # Add tweets for each attribution
                    for attribution, tweet_list in attribution_tweets.items():
                        for content, url in tweet_list:
                            # Format as: "- <b>Attribution</b> <a href='url'>content</a>"
                            formatted_line = f"- <b>{html.escape(attribution)}</b> <a href='{url}'>{html.escape(content)}</a>"
                            lines.append(formatted_line)
                    
                except Exception as e:
                    logger.error(f"DEBUG - Error in subcategory {subcategory}: {str(e)}")  # Temporary debug
                    continue
            
            # Add footer with channel reference
            if channel_username:
                lines.append(f"\nFollow @{channel_username} for more updates")
            else:
                lines.append("\nStay tuned for more updates!")
            
            final_text = "\n".join(lines)
            logger.error(f"DEBUG - Final text length: {len(final_text)}")  # Temporary debug
            return final_text
            
        except Exception as e:
            logger.error(f"DEBUG - Top level error: {str(e)}")  # Temporary debug
            log_error(logger, e, f"Failed to format {category} summary")
            return ""

    def _get_latest_summary_file(self) -> Path:
        """Get the most recent summary file"""
        try:
            pattern = f'{CATEGORY.lower()}_summary_*.json'
            files = list(self.input_dir.glob(pattern))
            
            if not files:
                logger.error("No summary files found")
                return None
                
            # Find the latest file by date in filename
            latest_file = max(files, key=lambda f: f.stem.split('_')[-1])
            logger.info(f"Latest summary file: {latest_file.name}")
            return latest_file
            
        except Exception as e:
            logger.error(f"Error finding latest summary file: {str(e)}")
            return None

    def _get_input_file(self, date_str=None):
        """Get input file path for specific date"""
        if not date_str:
            # Find latest summary file
            return self._get_latest_summary_file()
            
        # If date provided, look for the category's summary for that date
        file_path = self.input_dir / f'{CATEGORY.lower()}_summary_{date_str}.json'
        return file_path if file_path.exists() else None

    async def process_news_summary(self, date_str=None):
        """Process news summary file and send to all configured channels"""
        try:
            # Get and validate input file
            input_file = self._get_input_file(date_str)
            if not input_file or not await self._validate_summary_file(input_file):
                logger.error(f"Summary file validation failed: {input_file}")
                return False
            
            # Load summary data
            data = await load_json_file(input_file)
            if not data:
                logger.error("Failed to load summary data")
                return False
            
            # Get the category from the data
            if not data or len(data.keys()) != 1:
                logger.error(f"Invalid data structure. Expected one category, got: {list(data.keys())}")
                return False
            
            category = list(data.keys())[0]  # Get the single category
            logger.info(f"Processing summary for category: {category}")
            
            success = True
            # Send to each configured channel that has a channel ID
            for channel_name, channel_id in TELEGRAM_CHANNELS.items():
                if not channel_id:
                    logger.warning(f"No channel ID configured for {channel_name}, skipping...")
                    continue
                
                logger.info(f"Sending to channel {channel_name} with ID: {channel_id}")
                
                # Validate channel ID format - allow negative IDs for channels
                if not str(channel_id).replace('-', '').isdigit():
                    logger.warning(f"Invalid channel ID format for {channel_name}: {channel_id}, skipping...")
                    continue
                
                # Get channel info to get username
                try:
                    chat = await self.bot.get_chat(chat_id=channel_id)
                    channel_username = chat.username  # Only use actual Telegram username
                except Exception as e:
                    logger.warning(f"Could not get channel info: {str(e)}")
                    channel_username = None  # Fall back to None instead of channel_name
                
                # Format message with channel-specific footer
                formatted_text = await self.format_category_summary(category, data, channel_username)
                if not formatted_text:
                    logger.error(f"Failed to format summary for {category}")
                    continue
                
                # Send the formatted message
                if await self.send_message(channel_id, formatted_text):
                    logger.info(f"✅ Successfully sent summary to {channel_name} channel")
                else:
                    logger.error(f"Failed to send summary to {channel_name} channel")
                    success = False
                
                # Brief pause between sends
                await asyncio.sleep(2)
            
            return success
            
        except Exception as e:
            log_error(logger, e, "Failed to process news summary")
            return False

    async def _validate_summary_file(self, file_path: Path) -> bool:
        """Validate summary file exists and has correct format"""
        try:
            if not file_path.exists():
                logger.error(f"Summary file not found: {file_path}")
                return False
                
            with open(file_path, 'r') as f:
                data = json.load(f)
                
            # Validate basic structure
            if not isinstance(data, dict):
                logger.error("Summary file is not a valid JSON object")
                return False
                
            # Validate that the category exists
            if CATEGORY not in data:
                logger.error(f"Category {CATEGORY} not found in summary")
                return False
                
            # Validate structure for the category
            category_data = data[CATEGORY]
            if not isinstance(category_data, dict):
                logger.error(f"Invalid category data structure for {CATEGORY}")
                return False
                
            # Validate subcategories
            for subcategory, tweets in category_data.items():
                if not isinstance(tweets, list):
                    logger.error(f"Invalid tweets format in {subcategory}")
                    return False
                    
                # Validate tweet structure
                for tweet in tweets:
                    required_fields = ['attribution', 'content', 'url']
                    if not all(field in tweet for field in required_fields):
                        logger.error(f"Missing required fields in tweet under {subcategory}")
                        return False
            
            logger.info(f"✅ Summary file validation successful: {file_path.name}")
            return True
            
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in summary file: {file_path}")
            return False
        except Exception as e:
            logger.error(f"Error validating summary file: {str(e)}")
            return False

@with_retry(RetryConfig(max_retries=3, base_delay=1.0))
async def load_json_file(file_path):
    """Load and parse JSON file with retry"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        log_error(logger, e, f"Failed to load JSON file: {file_path}")
        raise DataProcessingError(f"Failed to load JSON file: {str(e)}")

@with_retry(RetryConfig(max_retries=3, base_delay=1.0))
async def process_category(sender, category, content, channel_id):
    """Process and send a category summary with retry"""
    try:
        if not isinstance(content, dict) or 'content' not in content:
            logger.error(f"Invalid content structure for {category}")
            return False
            
        raw_content = content['content']
        if not raw_content:
            logger.error(f"Empty content for {category}")
            return False
            
        formatted_content = await sender.format_text(raw_content)
        if not formatted_content:
            logger.error(f"Empty formatted content for {category}")
            return False
            
        return await sender.send_message(channel_id=channel_id, text=formatted_content)
        
    except Exception as e:
        log_error(logger, e, f"Failed to process category: {category}")
        raise DataProcessingError(f"Failed to process category: {str(e)}")

if __name__ == "__main__":
    load_dotenv()
    
    try:
        # Initialize sender with bot token
        bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        if not bot_token:
            raise ValueError("TELEGRAM_BOT_TOKEN not found in environment")
            
        sender = TelegramSender(bot_token)
        
        # Get date from command line argument if provided
        date_str = sys.argv[1] if len(sys.argv) > 1 else None
        
        async def process_summary():
            if date_str:
                logger.info(f"Processing summary for specific date: {date_str}")
                await sender.process_news_summary(date_str)
            else:
                logger.info("Processing latest summary file")
                await sender.process_news_summary()
        
        # Run everything in a single event loop
        asyncio.run(process_summary())
                
    except Exception as e:
        logger.error(f"Script error: {str(e)}")
        sys.exit(1) 