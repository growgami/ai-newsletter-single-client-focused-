"""Tweet summary process orchestrator"""

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

from data_processor import DataProcessor
from alpha_filter import AlphaFilter
from content_filter import ContentFilter
from news_filter import NewsFilter
from telegram_sender import TelegramSender
from category_mapping import CATEGORY

# Setup logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - [%(levelname)s] - %(message)s',
    handlers=[
        logging.FileHandler('logs/tweet_summary.log'),
        logging.StreamHandler(sys.stdout)  # Explicitly use sys.stdout
    ]
)

# Force immediate flushing of stdout
sys.stdout.reconfigure(line_buffering=True)  # For Python 3.7+

logger = logging.getLogger(__name__)

# Ensure child loggers also log to stdout
for name in ['data_processor', 'alpha_filter', 'content_filter', 'news_filter', 'telegram_sender']:
    child_logger = logging.getLogger(name)
    child_logger.addHandler(logging.StreamHandler(sys.stdout))
    child_logger.propagate = True  # Ensure logs propagate to parent

class TweetSummary:
    def __init__(self):
        logger.info("🚀 Initializing Tweet Summary Process")
        # Load environment variables
        load_dotenv()
        logger.info("✓ Environment variables loaded")
        
        # Initialize scheduler for midnight UTC cron
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
        self.telegram_sender = TelegramSender(config['telegram_token'])
        logger.info("✓ Telegram Sender initialized")
        
        # Control flags
        self.is_running = True
        self.is_processing = False
        self.alpha_filter_running = False
        
        # Monitoring thresholds
        self.thresholds = {
            'content_filter': 15,
            'news_filter': 15
        }
        logger.info(f"✓ Thresholds configured: Content Filter: {self.thresholds['content_filter']}, News Filter: {self.thresholds['news_filter']}")
        
        self.check_interval = 3600  # Changed from 120 to 3600 seconds (1 hour)
        logger.info(f"✓ Check interval set to {self.check_interval} seconds (hourly checks)")
        logger.info("✅ Tweet Summary Process initialization complete")

    def _initialize_directories(self):
        """Initialize all required directories with proper permissions"""
        try:
            # Define required directories
            required_dirs = {
                'logs': self.log_dir,
                'data': self.data_dir,
                'alpha_filtered': self.data_dir / 'filtered' / 'alpha_filtered',
                'content_filtered': self.data_dir / 'filtered' / 'content_filtered',
                'news_filtered': self.data_dir / 'filtered' / 'news_filtered'
            }
            
            # Create directories with proper permissions
            for name, dir_path in required_dirs.items():
                if not dir_path.exists():
                    logger.info(f"Creating directory: {dir_path}")
                    dir_path.mkdir(parents=True, exist_ok=True)
                    # Set 755 permissions (rwxr-xr-x)
                    os.chmod(dir_path, 0o755)
                    
            # Set up log file
            log_file = self.log_dir / 'tweet_summary.log'
            if not log_file.exists():
                log_file.touch()
            # Set 644 permissions for log file (rw-r--r--)
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
            'telegram_token': os.getenv('TELEGRAM_BOT_TOKEN'),
            'alpha_threshold': float(os.getenv('ALPHA_THRESHOLD', '0.8')),
            'risk_threshold': float(os.getenv('RISK_THRESHOLD', '0.4'))
        }

    async def scheduled_processing(self):
        """Run processing every 6 hours"""
        if self.is_processing:
            logger.warning("⚠️ Previous processing still running, skipping scheduled processing")
            return

        try:
            self.is_processing = True
            self.alpha_filter_running = True
            current_time = datetime.now(zoneinfo.ZoneInfo("UTC"))
            logger.info(f"🕒 SCHEDULED PROCESSING - Starting at {current_time.strftime('%H:%M:%S UTC')}")
            
            # Get yesterday's date
            yesterday = current_time - timedelta(days=1)
            date_str = yesterday.strftime('%Y%m%d')
            logger.info(f"📅 SCHEDULED PROCESSING - Target date: {date_str}")
            
            # Reset alpha filter state before processing
            logger.info("🔄 SCHEDULED PROCESSING - Resetting alpha filter state")
            self.alpha_filter.reset_state()
            
            # Step 1: Run data processor first
            logger.info("📥 SCHEDULED PROCESSING - Starting data processing")
            processed_count = await self.data_processor.process_tweets(date_str)
            if processed_count > 0:
                logger.info(f"✅ SCHEDULED PROCESSING - Processed {processed_count} tweets")
                
                # Step 2: Run alpha filter on the same date's data
                logger.info(f"🔄 SCHEDULED PROCESSING - Starting alpha filtering")
                await self.alpha_filter.process_content(date_str)
                logger.info("✅ SCHEDULED PROCESSING - Alpha filtering complete")
                
                # Clear processed file after successful run
                input_file = self.data_dir / 'processed' / f'{date_str}_processed.json'
                if input_file.exists():
                    input_file.unlink()
                    logger.info(f"🗑️ SCHEDULED PROCESSING - Cleared processed file for date: {date_str}")
            else:
                logger.info("ℹ️ SCHEDULED PROCESSING - No new tweets to process")
            
        except Exception as e:
            logger.error(f"❌ SCHEDULED PROCESSING - Error: {str(e)}")
        finally:
            self.is_processing = False
            self.alpha_filter_running = False
            logger.info("📝 SCHEDULED PROCESSING - Cycle completed")

    def _count_tweets_in_file(self, file_path: Path) -> int:
        """Count tweets in a file"""
        try:
            if not file_path.exists():
                return 0
                
            with open(file_path) as f:
                data = json.load(f)
                
            # Handle both file structures
            if 'tweets' in data:
                # Alpha filter output structure
                return len(data.get('tweets', []))
            elif CATEGORY in data:
                # Content/News filter output structure
                return len(data[CATEGORY].get('tweets', []))
            else:
                logger.error(f"Unknown file structure in {file_path}")
                return 0
                
        except Exception as e:
            logger.error(f"Error reading {file_path}: {str(e)}")
            return 0

    async def _clear_input_file(self, file_path: Path):
        """Clear input file after successful processing"""
        try:
            if file_path.exists():
                file_path.unlink()
            logger.info(f"Cleared input file: {file_path}")
        except Exception as e:
            logger.error(f"Error clearing input file: {str(e)}")

    async def continuous_monitoring(self):
        """Monitor output files and trigger processing"""
        logger.info("🔄 Starting continuous monitoring process (hourly checks)")
        while self.is_running:
            try:
                if not self.is_processing:
                    current_time = datetime.now(zoneinfo.ZoneInfo("UTC"))
                    logger.info(f"🕒 HOURLY CHECK - Starting at {current_time.strftime('%H:%M:%S UTC')}")
                    logger.info("🔍 Checking filter outputs...")
                    # Check alpha filter output only if alpha filter isn't running
                    if not self.alpha_filter_running:
                        alpha_file = self.data_dir / 'filtered' / 'alpha_filtered' / 'combined_filtered.json'
                        alpha_count = self._count_tweets_in_file(alpha_file)
                        logger.info(f"📊 Alpha filter tweet count: {alpha_count}")
                        
                        if alpha_count >= self.thresholds['content_filter']:
                            logger.info(f"🎯 Alpha filter threshold met ({alpha_count} tweets), initiating content filter")
                            self.is_processing = True
                            
                            # Run content filter
                            content_result = await self.content_filter.filter_content()
                            
                            if content_result:
                                await self._clear_input_file(alpha_file)
                                self.alpha_filter.reset_state()  # Reset alpha filter state
                                logger.info("✅ Content filter completed successfully")
                            else:
                                logger.error("❌ Content filter failed")
                            
                            self.is_processing = False
                    
                    # Check content filter output
                    content_file = self.data_dir / 'filtered' / 'content_filtered' / 'combined_filtered.json'
                    content_count = self._count_tweets_in_file(content_file)
                    logger.info(f"📊 Content filter tweet count: {content_count}")
                    
                    if content_count >= self.thresholds['news_filter']:
                        logger.info(f"🎯 Content filter threshold met ({content_count} tweets), initiating news filter")
                        self.is_processing = True
                        
                        # Run news filter and check result
                        news_result = await self.news_filter.process_all()
                        
                        if news_result:
                            logger.info("✅ News filter completed successfully, initiating Telegram send")
                            telegram_result = await self.telegram_sender.process_news_summary()
                            
                            if telegram_result:
                                await self._clear_input_file(content_file)
                                self.content_filter.reset_state()  # Reset content filter state
                                logger.info("✅ News processing and Telegram sending completed successfully")
                            else:
                                logger.error("❌ Telegram sending failed")
                        else:
                            logger.error("❌ News filter failed")
                        
                        self.is_processing = False

                logger.info(f"💤 Hourly check complete at {datetime.now(zoneinfo.ZoneInfo('UTC')).strftime('%H:%M:%S UTC')}, next check in 1 hour")
                await asyncio.sleep(self.check_interval)
                
            except Exception as e:
                logger.error(f"❌ Error in continuous monitoring: {str(e)}")
                self.is_processing = False
                await asyncio.sleep(self.check_interval)

    async def run(self):
        """Main process runner"""
        logger.info("🚀 Starting Tweet Summary main process")
        try:
            # Setup scheduled processing job (every 6 hours)
            self.scheduler.add_job(
                self.scheduled_processing,
                CronTrigger(hour='0,6,12,18', minute=0, timezone=timezone.utc),
                id='scheduled_processing'
            )
            logger.info("✓ Scheduled processing job set for 00:00, 06:00, 12:00, 18:00 UTC")
            
            # Start the scheduler
            self.scheduler.start()
            logger.info("✓ Scheduler started successfully")
            
            # Run continuous monitoring
            logger.info("👀 Starting continuous monitoring...")
            await self.continuous_monitoring()
            
        except Exception as e:
            logger.error(f"❌ Error in main process: {str(e)}")
        finally:
            logger.info("🛑 Shutting down scheduler")
            self.scheduler.shutdown()

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info("Shutdown signal received")
    tweet_summary.is_running = False

if __name__ == "__main__":
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create and run process
    tweet_summary = TweetSummary()
    try:
        asyncio.run(tweet_summary.run())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)
