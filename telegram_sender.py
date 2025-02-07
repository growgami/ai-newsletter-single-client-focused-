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
from dotenv import load_dotenv
import re
import sys
from error_handler import RetryConfig, with_retry, TelegramError, log_error, DataProcessingError
from category_mapping import TELEGRAM_CHANNEL_MAP, EMOJI_MAP, CATEGORY
import time

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
        self.used_emojis = set()  # Track used emojis per summary
        
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
            channel_id = TELEGRAM_CHANNEL_MAP[category]
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

    async def format_category_summary(self, category: str, summary: dict) -> str:
        """Format category summary into a Telegram message"""
        try:
            # Reset used emojis for new summary
            self._reset_used_emojis()
            
            # Case-insensitive check for category
            category_key = next(
                (k for k in summary.keys() if k.upper() == category),
                None
            )
            
            if not summary or not category_key:
                logger.error(f"Invalid summary format for {category}")
                return ""
            
            category_data = summary[category_key]
            if not isinstance(category_data, dict):
                logger.error(f"Invalid category data format for {category}")
                return ""
            
            # Build message with header
            lines = [f"<u><b><i>{category} Rollup</i></b></u> ~\n"]
            
            # Add each subcategory and its tweets
            for subcategory, tweets in category_data.items():
                # Get unique emoji for subcategory
                emoji = self._get_emoji_for_subcategory(subcategory)
                
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
                
                # Format consolidated tweets for each attribution
                for attribution, tweet_list in attribution_tweets.items():
                    if len(tweet_list) == 1:
                        # Single tweet format
                        content, url = tweet_list[0]
                        lines.append(f"- <b>{attribution}</b> <a href='{url}'>{html.escape(content)}</a>")
                    else:
                        # Multiple tweets consolidated format
                        consolidated = []
                        for content, url in tweet_list:
                            # Create short summary (first part of content up to a sensible break)
                            summary = content.split('.')[0].split(';')[0].strip()
                            if len(summary) > 50:  # Truncate if too long
                                summary = summary[:47] + "..."
                            consolidated.append(f"<a href='{url}'>{html.escape(summary)}</a>")
                        
                        lines.append(f"- <b>{attribution}</b> {' • '.join(consolidated)}")
                
                lines.append("")  # Empty line between subcategories
            
            return "\n".join(lines)
            
        except Exception as e:
            log_error(logger, e, f"Failed to format {category} summary")
            return ""

    def _get_input_file(self):
        """Get input file path - always polkadot_summary.json"""
        return Path('data/filtered/news_filtered/polkadot_summary.json')

    async def process_news_summary(self):
        """Process news summary file and send to all configured channels"""
        try:
            # Get and validate input file
            input_file = self._get_input_file()
            if not await self._validate_summary_file(input_file):
                logger.error("Summary file validation failed")
                return False
            
            # Load summary data
            data = await load_json_file(input_file)
            if not data:
                logger.error("Failed to load summary data")
                return False
            
            # Format summary once
            formatted_text = await self.format_category_summary(CATEGORY, data)
            if not formatted_text:
                logger.error("Failed to format summary")
                return False
            
            success = True
            # Send to each configured channel
            for category, channel_id in TELEGRAM_CHANNEL_MAP.items():
                if not channel_id:
                    logger.warning(f"No channel ID configured for {category}, skipping...")
                    continue
                
                # Validate channel ID format
                if not channel_id.strip('-').isdigit():
                    logger.warning(f"Invalid channel ID format for {category}, skipping...")
                    continue
                
                # Send message
                if await self.send_message(channel_id, formatted_text):
                    logger.info(f"✅ Successfully sent summary to {category} channel")
                else:
                    logger.error(f"Failed to send summary to {category} channel")
                    success = False
                
                # Brief pause between sends
                await asyncio.sleep(2)
            
            return success
            
        except Exception as e:
            log_error(logger, e, "Failed to process news summary")
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
        
        # Process all summary files
        summary_dir = Path('data/filtered/news_filtered')
        summary_files = list(summary_dir.glob('*_summary.json'))
        
        if not summary_files:
            logger.warning("No summary files found to process")
            sys.exit(0)
            
        logger.info(f"Found {len(summary_files)} summary files to process")
        
        async def process_all_files():
            for summary_file in summary_files:
                try:
                    logger.info(f"Processing {summary_file.name}")
                    await sender.process_news_summary()
                    # Brief pause between messages
                    await asyncio.sleep(2)
                except Exception as e:
                    logger.error(f"Failed to process {summary_file.name}: {str(e)}")
                    continue
        
        # Run everything in a single event loop
        asyncio.run(process_all_files())
                
    except Exception as e:
        logger.error(f"Script error: {str(e)}")
        sys.exit(1) 