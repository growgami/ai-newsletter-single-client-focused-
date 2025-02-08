"""Tweet summary process orchestrator"""

import logging
import asyncio
import signal
import sys
import os
from datetime import datetime, timezone
from pathlib import Path
import json
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

from alpha_filter import AlphaFilter
from content_filter import ContentFilter
from news_filter import NewsFilter
from telegram_sender import TelegramSender

# Setup logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - [%(levelname)s] - %(message)s',
    handlers=[
        logging.FileHandler('logs/tweet_summary.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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
            'content_filter': 20,  # Run content filter if alpha filter has > 20 tweets
            'news_filter': 10      # Run news filter if content filter has > 10 tweets
        }
        logger.info(f"✓ Thresholds configured: Content Filter: {self.thresholds['content_filter']}, News Filter: {self.thresholds['news_filter']}")
        
        self.check_interval = 120
        logger.info(f"✓ Check interval set to {self.check_interval} seconds")
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

    async def midnight_processing(self):
        """Run alpha filter at midnight UTC"""
        if self.is_processing:
            logger.warning("⚠️ Previous processing still running, skipping midnight processing")
            return

        try:
            self.is_processing = True
            self.alpha_filter_running = True
            logger.info("🌙 Starting midnight alpha filter processing")
            
            # Run alpha filter
            await self.alpha_filter.process_all_dates()
            logger.info("✅ Alpha filtering complete")
            
        except Exception as e:
            logger.error(f"❌ Error in midnight processing: {str(e)}")
        finally:
            self.is_processing = False
            self.alpha_filter_running = False
            logger.info("📝 Midnight processing cycle completed")

    def _count_tweets_in_file(self, file_path: Path) -> int:
        """Count tweets in a file"""
        try:
            if not file_path.exists():
                return 0
                
            with open(file_path) as f:
                data = json.load(f)
                return len(data.get('tweets', []))
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
        logger.info("🔄 Starting continuous monitoring process")
        while self.is_running:
            try:
                if not self.is_processing:
                    # Check alpha filter output only if alpha filter isn't running
                    if not self.alpha_filter_running:
                        alpha_file = self.data_dir / 'filtered' / 'alpha_filtered' / 'combined_filtered.json'
                        alpha_count = self._count_tweets_in_file(alpha_file)
                        logger.debug(f"📊 Alpha filter tweet count: {alpha_count}")
                        
                        if alpha_count >= self.thresholds['content_filter']:
                            logger.info(f"🎯 Alpha filter threshold met ({alpha_count} tweets), initiating content filter")
                            self.is_processing = True
                            
                            # Run content filter
                            content_result = await self.content_filter.filter_content()
                            
                            if content_result:
                                await self._clear_input_file(alpha_file)
                                logger.info("✅ Content filter completed successfully")
                            else:
                                logger.error("❌ Content filter failed")
                            
                            self.is_processing = False
                    
                    # Check content filter output
                    content_file = self.data_dir / 'filtered' / 'content_filtered' / 'combined_filtered.json'
                    content_count = self._count_tweets_in_file(content_file)
                    logger.debug(f"📊 Content filter tweet count: {content_count}")
                    
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
                                logger.info("✅ News processing and Telegram sending completed successfully")
                            else:
                                logger.error("❌ Telegram sending failed")
                        else:
                            logger.error("❌ News filter failed")
                        
                        self.is_processing = False

                await asyncio.sleep(self.check_interval)
                
            except Exception as e:
                logger.error(f"❌ Error in continuous monitoring: {str(e)}")
                self.is_processing = False
                await asyncio.sleep(self.check_interval)

    async def run(self):
        """Main process runner"""
        logger.info("🚀 Starting Tweet Summary main process")
        try:
            # Setup midnight cron job
            self.scheduler.add_job(
                self.midnight_processing,
                CronTrigger(hour=0, minute=0, timezone=timezone.utc),
                id='midnight_alpha_filter'
            )
            logger.info("✓ Midnight UTC cron job scheduled")
            
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
