"""News generation process orchestrator with scheduled processing"""

import logging
import asyncio
import signal
import sys
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
import json
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv
import zoneinfo

from processors.data_processor import DataProcessor
from processors.alpha_filter import AlphaFilter
from processors.content_filter import ContentFilter
from processors.news_filter import NewsFilter
from senders.telegram_sender import TelegramSender
from senders.discord_sender import DiscordSender
from category_mapping import CATEGORY, TELEGRAM_CHANNELS, DISCORD_WEBHOOKS

# Configure root logger with detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - [%(levelname)s] - %(message)s',
    handlers=[
        logging.FileHandler('logs/news_generator.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Force immediate flushing of stdout for PM2
sys.stdout.reconfigure(line_buffering=True)

# Get the root logger
root_logger = logging.getLogger()

# Configure subprocess loggers to ensure visibility in PM2
subprocess_loggers = [
    'data_processor',
    'alpha_filter',
    'content_filter',
    'news_filter',
    'telegram_sender',
    'discord_sender'
]

# Configure each subprocess logger
for logger_name in subprocess_loggers:
    logger = logging.getLogger(logger_name)
    logger.handlers = []  # Remove any existing handlers
    logger.addHandler(logging.StreamHandler(sys.stdout))
    logger.addHandler(logging.FileHandler('logs/news_generator.log', encoding='utf-8'))
    if logger_name in ['httpx', 'openai', 'apscheduler']:
        logger.setLevel(logging.WARNING)  # Reduce noise from external libraries
    else:
        logger.setLevel(logging.INFO)
    logger.propagate = False  # Prevent duplicate logs

# Get the main logger
logger = logging.getLogger(__name__)
logger.info("🔧 Logging system initialized with subprocess capture")

class NewsGenerator:
    def __init__(self):
        logger.info("🚀 Initializing News Generator Process")
        
        # Load environment variables
        load_dotenv()
        logger.info("✓ Environment variables loaded")
        
        # Initialize scheduler
        self.scheduler = AsyncIOScheduler()
        logger.info("✓ Scheduler initialized")
        
        # Get absolute path to project root
        self.base_dir = Path(__file__).parent.absolute()
        
        # Update paths to be absolute
        self.data_dir = self.base_dir / 'data'
        self.log_dir = self.base_dir / 'logs'
        logger.info(f"✓ Working directories set: {self.base_dir}")
        
        # Initialize directories first
        self._initialize_directories()
        
        # Initialize components
        logger.info("🔄 Initializing pipeline components...")
        config = self._get_config()
        self.data_processor = DataProcessor()
        logger.info("✓ Data Processor initialized")
        self.alpha_filter = AlphaFilter(config)
        logger.info("✓ Alpha Filter initialized")
        self.content_filter = ContentFilter(config)
        logger.info("✓ Content Filter initialized")
        self.news_filter = NewsFilter(config)
        logger.info("✓ News Filter initialized")
        
        # Initialize senders if configured
        self.telegram_sender = None
        if any(channel for channel in TELEGRAM_CHANNELS.values()):
            self.telegram_sender = TelegramSender(config['telegram_token'])
            logger.info("✓ Telegram Sender initialized")
        else:
            logger.info("ℹ️ No Telegram channels configured, Telegram sending disabled")
            
        self.discord_sender = None
        if any(webhook for webhook in DISCORD_WEBHOOKS.values()):
            self.discord_sender = DiscordSender()
            logger.info("✓ Discord Sender initialized")
        else:
            logger.info("ℹ️ No Discord webhooks configured, Discord sending disabled")
        
        # Control flags
        self.is_running = True
        self.is_processing = False
        
        logger.info("✅ News Generator Process initialization complete")

    def _initialize_directories(self):
        """Initialize all required directories with proper permissions"""
        try:
            required_dirs = {
                'logs': self.log_dir,
                'data': self.data_dir,
                'alpha_filtered': self.data_dir / 'filtered' / 'alpha_filtered',
                'content_filtered': self.data_dir / 'filtered' / 'content_filtered',
                'news_filtered': self.data_dir / 'filtered' / 'news_filtered',
                'news_history': self.data_dir / 'news_history'
            }
            
            for name, dir_path in required_dirs.items():
                if not dir_path.exists():
                    logger.info(f"Creating directory: {dir_path}")
                    dir_path.mkdir(parents=True, exist_ok=True)
                    os.chmod(dir_path, 0o755)
            
            log_file = self.log_dir / 'news_generator.log'
            if not log_file.exists():
                log_file.touch()
            os.chmod(log_file, 0o644)
            
            logger.info("Successfully initialized all required directories")
            
        except Exception as e:
            logger.error(f"Error initializing directories: {str(e)}")
            raise

    def _get_config(self):
        """Get configuration from environment variables"""
        return {
            'deepseek_api_key': os.getenv('DEEPSEEK_API_KEY'),
            'openai_api_key': os.getenv('OPENAI_API_KEY'),
            'alpha_threshold': float(os.getenv('ALPHA_THRESHOLD', '0.8')),
            'risk_threshold': float(os.getenv('RISK_THRESHOLD', '0.4')),
            'telegram_token': os.getenv('TELEGRAM_BOT_TOKEN')
        }

    def _get_yesterday_date(self):
        """Get yesterday's date in YYYYMMDD format"""
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        return yesterday.strftime('%Y%m%d')

    async def _clear_input_file(self, file_path: Path):
        """Clear input file after successful processing"""
        try:
            if file_path.exists():
                file_path.unlink()
            logger.info(f"Cleared input file: {file_path}")
        except Exception as e:
            logger.error(f"Error clearing input file: {str(e)}")

    async def _check_content_overlap(self, content_file: Path) -> bool:
        """Check if content from previous summary exists in new content"""
        try:
            date_str = self._get_yesterday_date()
            news_file = self.data_dir / 'filtered' / 'news_filtered' / f'{CATEGORY.lower()}_summary_{date_str}.json'
            history_file = self.data_dir / 'news_history' / f'{CATEGORY.lower()}_summary_{date_str}.json'

            # Load new content
            if not content_file.exists():
                logger.warning("No content file found to check")
                return False

            with open(content_file, 'r') as f:
                new_content = json.load(f)

            # Get new tweets
            new_tweets = new_content.get(CATEGORY, {}).get('tweets', [])
            if not new_tweets:
                logger.warning("No tweets found in content file")
                return False

            # Check both current news file and history file
            for check_file in [news_file, history_file]:
                if check_file.exists():
                    with open(check_file, 'r') as f:
                        previous_content = json.load(f)
                    
                    previous_tweets = previous_content.get(CATEGORY, {}).get('tweets', [])
                    if previous_tweets:
                        # Create sets of tweet identifiers (url + content combination)
                        previous_ids = {f"{t['url']}:{t['content']}" for t in previous_tweets}
                        new_ids = {f"{t['url']}:{t['content']}" for t in new_tweets}
                        
                        # Check for overlap
                        overlap = previous_ids.intersection(new_ids)
                        if overlap:
                            logger.warning(f"Found {len(overlap)} overlapping tweets with {check_file.name}")
                            logger.info("Overlapping content:")
                            for tweet_id in overlap:
                                logger.info(f"- {tweet_id.split(':')[1][:100]}...")
                            return True

            logger.info("✅ No content overlap found with previous summaries")
            return False

        except Exception as e:
            logger.error(f"Error checking content overlap: {str(e)}")
            return False

    async def process_data(self):
        """Run data processor for yesterday's data"""
        if self.is_processing:
            logger.warning("⚠️ Another process is running, skipping data processor")
            return

        try:
            self.is_processing = True
            date_str = self._get_yesterday_date()
            logger.info(f"🔄 Starting data processing for {date_str}")
            
            # Run data processor - accumulate tweets
            processed_count = await self.data_processor.process_tweets(date_str)
            if processed_count > 0:
                logger.info(f"✅ Processed {processed_count} new tweets")
            else:
                logger.info("ℹ️ No new tweets to process")
            
        except Exception as e:
            logger.error(f"❌ Error in data processing: {str(e)}")
        finally:
            self.is_processing = False

    async def process_alpha_filter(self):
        """Run data processor and alpha filter for yesterday's data"""
        if self.is_processing:
            logger.warning("⚠️ Another process is running, skipping alpha filter")
            return

        try:
            self.is_processing = True
            date_str = self._get_yesterday_date()
            logger.info(f"🔄 Starting data processing and alpha filter for {date_str}")
            
            # First run data processor
            processed_count = await self.data_processor.process_tweets(date_str)
            if processed_count > 0:
                logger.info(f"✅ Processed {processed_count} new tweets")
                
                # Check data processor output
                data_file = self.data_dir / 'processed' / f'{date_str}_processed.json'
                if not data_file.exists():
                    logger.error("❌ Data processor did not create output")
                    return
                    
                # Run alpha filter on new data
                alpha_result = await self.alpha_filter.process_content(date_str)
                if alpha_result:
                    # Clear processed input after successful alpha filtering
                    await self._clear_input_file(data_file)
                    logger.info("✅ Alpha filter processing complete")
            else:
                logger.info("ℹ️ No new tweets to process")
            
        except Exception as e:
            logger.error(f"❌ Error in data processing or alpha filtering: {str(e)}")
        finally:
            self.is_processing = False

    async def process_content_filter(self):
        """Run content filter processing"""
        if self.is_processing:
            logger.warning("⚠️ Another process is running, skipping content filter")
            return

        try:
            self.is_processing = True
            date_str = self._get_yesterday_date()
            logger.info(f"🔄 Starting content filter processing for {date_str}")
            
            # Check alpha filter output
            alpha_file = self.data_dir / 'filtered' / 'alpha_filtered' / 'combined_filtered.json'
            if not alpha_file.exists():
                logger.warning("No alpha filter output found")
                return
                
            # Verify alpha file is not empty
            try:
                with open(alpha_file, 'r') as f:
                    alpha_data = json.load(f)
                if not alpha_data.get('tweets', []):
                    logger.warning("Alpha filter output is empty")
                    return
            except json.JSONDecodeError:
                logger.error("❌ Invalid JSON in alpha filter output")
                return
            
            # Run content filter - accumulate filtered content
            content_result = await self.content_filter.filter_content()
            if content_result:
                # Clear alpha filter input after successful content filtering
                await self._clear_input_file(alpha_file)
                self.alpha_filter.reset_state()
                logger.info("✅ Content filter processing complete")
            
        except Exception as e:
            logger.error(f"❌ Error in content filter processing: {str(e)}")
        finally:
            self.is_processing = False

    async def process_news_filter(self):
        """Run news filter processing and send notifications"""
        if self.is_processing:
            logger.warning("⚠️ Another process is running, skipping news filter")
            return

        try:
            self.is_processing = True
            date_str = self._get_yesterday_date()
            logger.info(f"🔄 Starting news filter processing for {date_str}")
            
            # Check content filter output
            content_file = self.data_dir / 'filtered' / 'content_filtered' / 'combined_filtered.json'
            if not content_file.exists():
                logger.warning("No content filter output found")
                return

            # Verify content file is not empty and valid
            try:
                with open(content_file, 'r') as f:
                    content_data = json.load(f)
                if not content_data.get(CATEGORY, {}).get('tweets', []):
                    logger.warning("Content filter output is empty")
                    return
            except json.JSONDecodeError:
                logger.error("❌ Invalid JSON in content filter output")
                return

            # Check for content overlap before processing
            has_overlap = await self._check_content_overlap(content_file)
            if has_overlap:
                logger.warning("❌ Found overlapping content with previous summary, skipping processing")
                return
            
            # Run news filter - process accumulated content
            news_result = await self.news_filter.process_all()
            if news_result:
                # Verify news filter output
                news_file = self.data_dir / 'filtered' / 'news_filtered' / f'{CATEGORY.lower()}_summary_{date_str}.json'
                if not news_file.exists():
                    logger.error("❌ News filter failed to create output")
                    return
                
                # Send notifications if configured
                send_success = True
                
                # Send to Telegram if configured
                if self.telegram_sender:
                    logger.info("📤 Sending to Telegram channels...")
                    telegram_result = await self.telegram_sender.process_news_summary()
                    if telegram_result:
                        logger.info("✅ Telegram sending complete")
                    else:
                        logger.error("❌ Telegram sending failed")
                        send_success = False
                
                # Send to Discord if configured
                if self.discord_sender:
                    logger.info("📤 Sending to Discord channels...")
                    discord_result = await self.discord_sender.process_news_summary()
                    if discord_result:
                        logger.info("✅ Discord sending complete")
                    else:
                        logger.error("❌ Discord sending failed")
                        send_success = False
                
                if send_success:
                    # Move news file to history
                    try:
                        history_dir = self.data_dir / 'news_history'
                        history_file = history_dir / f'{CATEGORY.lower()}_summary_{date_str}.json'
                        
                        # Ensure history directory exists
                        history_dir.mkdir(parents=True, exist_ok=True)
                        os.chmod(history_dir, 0o755)  # Set proper permissions
                        
                        if news_file.exists():
                            # Verify news file is valid before moving
                            try:
                                with open(news_file, 'r') as f:
                                    json.load(f)  # Validate JSON
                                news_file.replace(history_file)
                                logger.info(f"✅ Moved news file to history: {history_file.name}")
                            except json.JSONDecodeError:
                                logger.error("❌ Invalid JSON in news filter output")
                                return
                        
                        # Clear content filter input after successful processing and sending
                        await self._clear_input_file(content_file)
                        self.content_filter.reset_state()
                        logger.info("✅ News filter processing, sending, and archiving complete")
                    except Exception as e:
                        logger.error(f"❌ Error moving news file to history: {str(e)}")
                else:
                    logger.error("❌ Some sending operations failed")
            
        except Exception as e:
            logger.error(f"❌ Error in news filter processing: {str(e)}")
        finally:
            self.is_processing = False

    async def run(self):
        """Main process runner"""
        logger.info("🚀 Starting News Generator main process")
        try:
            # Schedule alpha filter (with data processor) - every hour
            self.scheduler.add_job(
                self.process_alpha_filter,
                CronTrigger(hour='0-23', minute=0, timezone=timezone.utc),
                id='alpha_filter'
            )
            logger.info("✓ Alpha filter (with data processor) scheduled for every hour")
            
            # Schedule content filter - every 5 hours
            self.scheduler.add_job(
                self.process_content_filter,
                CronTrigger(hour='0,5,10,15,20', minute=0, timezone=timezone.utc),
                id='content_filter'
            )
            logger.info("✓ Content filter scheduled for every 5 hours")
            
            # Schedule news filter - at 23:00
            self.scheduler.add_job(
                self.process_news_filter,
                CronTrigger(hour=23, minute=0, timezone=timezone.utc),
                id='news_filter'
            )
            logger.info("✓ News filter scheduled for 23:00 UTC")
            
            # Start the scheduler
            self.scheduler.start()
            logger.info("✓ Scheduler started successfully")
            
            # Keep the process running
            while self.is_running:
                await asyncio.sleep(1)
            
        except Exception as e:
            logger.error(f"❌ Error in main process: {str(e)}")
        finally:
            logger.info("🛑 Shutting down scheduler")
            self.scheduler.shutdown()

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info("Shutdown signal received")
    news_generator.is_running = False

if __name__ == "__main__":
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create and run process
    news_generator = NewsGenerator()
    try:
        asyncio.run(news_generator.run())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)
