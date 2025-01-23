"""Telegram message formatting and sending service"""

import logging
import html
from telegram import Bot
from telegram.constants import ParseMode
import asyncio
import os
from pathlib import Path
import json
from datetime import datetime
from dotenv import load_dotenv
import re
import sys
from error_handler import RetryConfig, with_retry, TelegramError, log_error, DataProcessingError
from category_mapping import TELEGRAM_CHANNEL_MAP

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Reduce httpx logging
logging.getLogger('httpx').setLevel(logging.WARNING)

class TelegramSender:
    def __init__(self, bot_token):
        if not bot_token:
            raise ValueError("Bot token is required")
        self.bot = Bot(token=bot_token)
        
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
                        formatted_header = f"{formatted_date} - {rest}"
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
                    
                # Format tweet lines (author: text) with URL on next line
                if ':' in line and not line.startswith('http'):
                    try:
                        author, content = line.split(':', 1)
                        url = ""
                        
                        # Check next line for URL
                        if i + 1 < len(lines) and lines[i + 1].strip().startswith('http'):
                            url = lines[i + 1].strip()
                            i += 2
                        else:
                            i += 1
                            
                        formatted_lines.append(f"- <b>{html.escape(author.strip())}</b>: <a href='{url}'>{html.escape(content.strip())}</a>")
                            
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
            
        except Exception as e:
            log_error(logger, e, f"Failed to send message to channel {channel_id}")
            raise TelegramError(f"Failed to send message: {str(e)}")

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
        if not isinstance(content, dict) or 'text' not in content:
            logger.error(f"Invalid content structure for {category}")
            return False
            
        raw_text = content['text']
        if not raw_text:
            logger.error(f"Empty text for {category}")
            return False
            
        formatted_text = await sender.format_text(raw_text)
        if not formatted_text:
            logger.error(f"Empty formatted text for {category}")
            return False
            
        return await sender.send_message(channel_id=channel_id, text=formatted_text)
        
    except Exception as e:
        log_error(logger, e, f"Failed to process category: {category}")
        raise DataProcessingError(f"Failed to process category: {str(e)}")

async def test_sender():
    """Test the TelegramSender"""
    load_dotenv()
    
    try:
        # Get date from command line argument or use today's date
        date_str = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y%m%d')
        logger.info(f"Processing summaries for date: {date_str}")
        
        # Validate environment variables
        bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        if not bot_token:
            raise ValueError("TELEGRAM_BOT_TOKEN not found in environment variables")
            
        # Use centralized channel mapping
        channel_mapping = {
            category: os.getenv(f'TELEGRAM_{channel_key.upper()}_CHANNEL_ID')
            for category, channel_key in TELEGRAM_CHANNEL_MAP.items()
        }
        
        # Debug logging for channel mapping
        for category, channel_id in channel_mapping.items():
            logger.info(f"Channel mapping - Category: {category}, Channel ID: {channel_id}")
            
        # Load summaries
        summaries_file = Path('data') / 'summaries' / f'summaries_{date_str}.json'
        if not summaries_file.exists():
            logger.error(f"No summaries file found for date {date_str}")
            return
            
        try:
            data = await load_json_file(summaries_file)
                
            if not data or 'summaries' not in data:
                logger.error("No summaries found in data")
                return
                
            summaries = data['summaries']
            logger.info(f"Found summaries for: {', '.join(summaries.keys())}")
            
            sender = TelegramSender(bot_token)
            valid_categories = set(channel_mapping.keys())
            
            for category, content in summaries.items():
                if category not in valid_categories:
                    logger.warning(f"Category {category} not in valid_categories: {valid_categories}")
                    continue
                    
                try:
                    channel_id = channel_mapping.get(category)
                    logger.info(f"Processing {category} with channel ID: {channel_id}")
                    if not channel_id:
                        logger.error(f"No channel ID found for {category} in mapping: {channel_mapping}")
                        continue
                        
                    logger.info(f"Sending {category} summary...")
                    
                    success = await process_category(sender, category, content, channel_id)
                    
                    if success:
                        logger.info(f"Successfully sent {category} summary")
                    else:
                        logger.error(f"Failed to send {category} summary")
                        
                    await asyncio.sleep(2)
                    
                except Exception as e:
                    log_error(logger, e, f"Error processing {category}")
                    continue
                    
        except json.JSONDecodeError as e:
            log_error(logger, e, "Failed to parse summaries file")
            raise DataProcessingError(f"Failed to parse summaries file: {str(e)}")
            
    except Exception as e:
        log_error(logger, e, "Error in sender")
        raise

if __name__ == "__main__":
    asyncio.run(test_sender()) 