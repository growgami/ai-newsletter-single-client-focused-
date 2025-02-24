"""Discord message formatting and sending service"""

import logging
import aiohttp
import asyncio
import os
from pathlib import Path
import json
from datetime import datetime
import sys
from utils.error_handler import RetryConfig, with_retry, log_error, DataProcessingError
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables before importing category_mapping
load_dotenv()

# Import after loading environment variables
from category_mapping import CATEGORY, EMOJI_MAP, DISCORD_WEBHOOKS

# Discord webhook mapping (similar to TELEGRAM_CHANNELS)
DISCORD_WEBHOOKS = {
    'GROWGAMI': os.getenv('DISCORD_GROWGAMI_WEBHOOK'),
    'STABLECOINS': os.getenv('DISCORD_STABLECOINS_WEBHOOK')
}

class DiscordSender:
    def __init__(self):
        # Initialize data directories
        self.data_dir = Path('data')
        self.input_dir = self.data_dir / 'filtered' / 'news_filtered'  # Input from news_filter
        
        # Create directories if they don't exist
        self.input_dir.mkdir(parents=True, exist_ok=True)
        
        # Track used emojis per summary
        self.used_emojis = set()
        
        # Debug: Print webhook URLs
        logger.error(f"DEBUG - Environment Variables:")
        for name, webhook in DISCORD_WEBHOOKS.items():
            logger.error(f"Webhook {name}: URL exists = {bool(webhook)}")

    def _reset_used_emojis(self):
        """Reset the used emojis tracking set"""
        self.used_emojis.clear()

    async def format_text(self, text):
        """Format text with Discord markdown"""
        if not text:
            logger.warning("Received empty text to format")
            return ""
            
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
                        formatted_lines.append(f"__**{formatted_header}**__")
                    except ValueError as e:
                        log_error(logger, e, f"Failed to parse date: {date_str}")
                        formatted_lines.append(f"__**{line}**__")
                    i += 1
                    continue
                    
                # Format subcategory with emoji
                if not ':' in line and not line.startswith('http'):
                    if formatted_lines:
                        formatted_lines.append('')
                    formatted_lines.append(f"__**{line}**__")
                    i += 1
                    continue
                    
                # Format tweet lines with URL
                if not line.startswith('http'):
                    try:
                        formatted_lines.append(line)
                        i += 1
                    except Exception as e:
                        log_error(logger, e, f"Failed to format tweet line: {line}")
                        formatted_lines.append(line)
                        i += 1
                    continue
                    
                # Keep URLs as is
                if line.startswith('http'):
                    formatted_lines.append(line)
                    i += 1
                    continue
                    
                # Default case - keep line as is
                formatted_lines.append(line)
                i += 1
                
            return '\n'.join(formatted_lines)
            
        except Exception as e:
            log_error(logger, e, "Failed to format text")
            raise DataProcessingError(f"Text formatting failed: {str(e)}")

    @with_retry(RetryConfig(max_retries=3, base_delay=1.0))
    async def send_message(self, webhook_url: str, text: str) -> bool:
        """Send a message to a Discord channel via webhook"""
        if not text or not webhook_url:
            return False
            
        try:
            formatted_text = await self.format_text(text)
            if not formatted_text:
                logger.error("Empty formatted text")
                return False
                
            # Split message if too long for Discord (2000 char limit)
            chunks = self._split_message(formatted_text)
            
            async with aiohttp.ClientSession() as session:
                for chunk in chunks:
                    payload = {'content': chunk}
                    async with session.post(webhook_url, json=payload) as response:
                        if response.status not in (200, 204):
                            error_text = await response.text()
                            logger.error(f"Discord API error: {response.status} - {error_text}")
                            return False
                        # Rate limit handling
                        if response.status == 429:
                            retry_after = float(response.headers.get('Retry-After', 1))
                            await asyncio.sleep(retry_after)
                            return await self.send_message(webhook_url, text)
                            
            return True
            
        except aiohttp.ClientError as e:
            logger.error(f"Discord webhook error: {str(e)}")
            raise DataProcessingError(f"Failed to send message: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error sending message: {str(e)}")
            raise DataProcessingError(f"Failed to send message: {str(e)}")

    def _split_message(self, text: str, limit: int = 2000) -> list:
        """Split message into chunks that fit Discord's limit, splitting by subcategories"""
        if len(text) <= limit:
            return [text]
            
        lines = text.split('\n')
        chunks = []
        
        # First chunk is just the header
        header = lines[0]
        chunks.append(header)
        
        # Process remaining lines by subcategory
        current_chunk = []
        current_length = 0
        
        for line in lines[1:]:  # Skip header
            line_length = len(line) + 1  # +1 for newline
            
            # If this is a subcategory header
            if line.startswith('__**'):
                # If we have content and adding this subcategory would exceed limit
                if current_chunk and current_length + line_length > limit:
                    chunks.append('\n'.join(current_chunk))
                    current_chunk = []
                    current_length = 0
                
            # Add line to current chunk
            current_chunk.append(line)
            current_length += line_length
            
        # Add any remaining content
        if current_chunk:
            chunks.append('\n'.join(current_chunk))
            
        return chunks

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

    async def format_category_summary(self, category: str, summary: dict) -> str:
        """Format category summary into a Discord message"""
        try:
            # Reset used emojis for new summary
            self._reset_used_emojis()
            
            # Find the actual category key that matches case-insensitively
            category_key = None
            for key in summary.keys():
                if key.lower() == category.lower():
                    category_key = key
                    break
                    
            if not summary or not category_key:
                logger.error(f"Invalid summary format for {category}")
                return ""
            
            category_data = summary[category_key]
            
            if not isinstance(category_data, dict):
                logger.error(f"Invalid category data format for {category}")
                return ""
            
            # Build message with header using the original category key to preserve case
            lines = [f"__**{category_key} News Drop**__ ~\n"]
            
            # Add each subcategory and its tweets
            for subcategory, tweets in category_data.items():
                try:
                    # Add newline before each subcategory (except the first one)
                    if len(lines) > 1:
                        lines.append("")
                    
                    # Get unique emoji for subcategory
                    emoji = self._get_emoji_for_subcategory(subcategory)
                    
                    # Add subcategory header with emoji
                    subcategory_text = f"__**{subcategory}**__"
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
                            # Format as: "- **Attribution** content [link]"
                            formatted_line = f"- **{attribution}** {content} {url}"
                            lines.append(formatted_line)
                    
                except Exception as e:
                    logger.error(f"Error in subcategory {subcategory}: {str(e)}")
                    continue
            
            return '\n'.join(lines)
            
        except Exception as e:
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
        """Process news summary file and send to all configured webhooks"""
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
            
            # Format message once using the original category
            formatted_text = await self.format_category_summary(category, data)
            if not formatted_text:
                logger.error(f"Failed to format summary for {category}")
                return False
            
            success = True
            # Send to each configured webhook
            for channel_name, webhook_url in DISCORD_WEBHOOKS.items():
                if not webhook_url:
                    logger.warning(f"No webhook URL configured for {channel_name}, skipping...")
                    continue
                
                logger.info(f"Sending to Discord channel {channel_name}")
                
                # Send the formatted message
                if await self.send_message(webhook_url, formatted_text):
                    logger.info(f"✅ Successfully sent summary to {channel_name} Discord channel")
                else:
                    logger.error(f"Failed to send summary to {channel_name} Discord channel")
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

if __name__ == "__main__":
    try:
        # Initialize sender
        sender = DiscordSender()
        
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