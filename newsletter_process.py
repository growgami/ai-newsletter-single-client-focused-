"""Newsletter process orchestrator"""

import logging
import asyncio
import signal
import sys
import os
from datetime import datetime, timedelta
import zoneinfo
from pathlib import Path
import json
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

from data_processor import DataProcessor
from alpha_filter import AlphaFilter
from content_filter import ContentFilter
from news_filter import NewsFilter
from telegram_sender import TelegramSender
from error_handler import with_retry, RetryConfig, log_error
from garbage_collector import GarbageCollector

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/newsletter.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class NewsletterProcess:
    def __init__(self):
        # Load environment variables
        load_dotenv()
        
        # Initialize scheduler
        self.scheduler = AsyncIOScheduler()
        
        # Get absolute path to project root
        self.base_dir = Path(__file__).parent.absolute()
        
        # Update paths to be absolute
        self.data_dir = self.base_dir / 'data'
        self.log_dir = self.base_dir / 'logs'
        
        # Initialize directories first
        self._initialize_directories()
        
        # Initialize components
        self.data_processor = DataProcessor()
        self.alpha_filter = AlphaFilter(self._get_config())
        self.content_filter = ContentFilter(self._get_config())
        self.news_filter = NewsFilter(self._get_config())
        self.telegram_sender = TelegramSender(self._get_config()['telegram_token'])
        
        # Control flags
        self.is_running = True
        self.is_processing = False
        
        # Monitoring thresholds
        self.thresholds = {
            'content_filter': {
                'any_column': 20,
                'all_columns': 10
            },
            'news_filter': {
                'any_column': 15,
                'all_columns': 10
            }
        }

        # Add garbage collector
        self.gc = GarbageCollector(self._get_config())

    def _initialize_directories(self):
        """Initialize all required directories with proper permissions"""
        try:
            # Define required directories
            required_dirs = {
                'logs': self.log_dir,
                'data': self.data_dir,
                'raw': self.data_dir / 'raw',
                'processed': self.data_dir / 'processed',
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
            log_file = self.log_dir / 'newsletter.log'
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

    async def daily_processing(self):
        """Run daily processing at 4 UTC"""
        if self.is_processing:
            logger.warning("Previous processing still running, skipping")
            return

        try:
            self.is_processing = True
            logger.info("Starting daily processing")

            # Process raw tweets
            processed_count = await self.data_processor.process_tweets()
            if processed_count > 0:
                logger.info(f"Processed {processed_count} tweets")
                
                # Run alpha filter
                await self.alpha_filter.process_all_dates()
                logger.info("Alpha filtering complete")
            
            self.is_processing = False
            
        except Exception as e:
            logger.error(f"Error in daily processing: {str(e)}")
            self.is_processing = False

    def _count_tweets_in_files(self, directory: Path, pattern: str = 'column_*.json') -> dict:
        """Count tweets in each column file"""
        counts = {}
        try:
            # Add error handling for individual files
            for file in directory.glob(pattern):
                try:
                    with open(file) as f:
                        data = json.load(f)
                        tweets = data.get('tweets', [])
                        counts[file.stem] = len(tweets)
                except Exception as e:
                    logger.error(f"Error reading {file}: {str(e)}")
                    continue
            return counts
        except Exception as e:
            logger.error(f"Error counting tweets: {str(e)}")
            return {}

    def _should_run_content_filter(self) -> bool:
        """Check if content filter should run based on tweet counts"""
        counts = self._count_tweets_in_files(Path('data/filtered/alpha_filtered'))
        if not counts:
            return False

        thresholds = self.thresholds['content_filter']
        
        # Check if any column exceeds threshold
        if any(count >= thresholds['any_column'] for count in counts.values()):
            return True
            
        # Check if all columns meet minimum threshold
        if all(count >= thresholds['all_columns'] for count in counts.values()):
            return True
            
        return False

    def _should_run_news_filter(self) -> bool:
        """Check if news filter should run based on tweet counts"""
        counts = self._count_tweets_in_files(Path('data/filtered/content_filtered'))
        if not counts:
            return False

        thresholds = self.thresholds['news_filter']
        
        # Check if any column exceeds threshold
        if any(count >= thresholds['any_column'] for count in counts.values()):
            return True
            
        # Check if all columns meet minimum threshold
        if all(count >= thresholds['all_columns'] for count in counts.values()):
            return True
            
        return False

    async def _clear_input_files(self, directory: Path):
        """Clear input files after successful processing"""
        try:
            for file in directory.glob('column_*.json'):
                file.unlink()
            logger.info(f"Cleared input files in {directory}")
        except Exception as e:
            logger.error(f"Error clearing input files: {str(e)}")

    async def continuous_monitoring(self):
        """Monitor output files and trigger processing"""
        while self.is_running:
            try:
                if not self.is_processing:
                    # Check content filter conditions
                    if self._should_run_content_filter():
                        logger.info("Content filter conditions met")
                        self.is_processing = True
                        
                        # Run content filter
                        await self.content_filter.filter_content()
                        
                        # Clear alpha filter output
                        await self._clear_input_files(Path('data/filtered/alpha_filtered'))
                        
                        self.is_processing = False
                    
                    # Check news filter conditions
                    elif self._should_run_news_filter():
                        logger.info("News filter conditions met")
                        self.is_processing = True
                        
                        # Run news filter
                        await self.news_filter.process_all()
                        
                        # Send to Telegram
                        summary_dir = Path('data/filtered/news_filtered')
                        for summary_file in summary_dir.glob('*_summary.json'):
                            await self.telegram_sender.process_news_summary(summary_file)
                            await asyncio.sleep(2)  # Brief pause between messages
                        
                        # Clear content filter output
                        await self._clear_input_files(Path('data/filtered/content_filtered'))
                        
                        self.is_processing = False

                # Brief pause between checks
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Error in continuous monitoring: {str(e)}")
                self.is_processing = False
                await asyncio.sleep(60)  # Wait before retrying

    def setup_jobs(self):
        """Setup scheduled jobs"""
        # Daily processing at 4 AM UTC
        self.scheduler.add_job(
            self.daily_processing,
            CronTrigger(hour=4, minute=0),
            id='daily_processing',
            replace_existing=True
        )
        
        # Continuous monitoring
        self.scheduler.add_job(
            self.continuous_monitoring,
            'date',  # Run once when scheduler starts
            id='continuous_monitoring',
            replace_existing=True
        )
        
        # Add GC job
        self.scheduler.add_job(
            self.gc.run_cleanup,
            'interval',
            hours=1,
            id='garbage_collection',
            replace_existing=True
        )
        
        logger.info("Scheduled jobs setup complete")

    async def shutdown(self):
        """Cleanup and shutdown"""
        logger.info("Shutting down...")
        self.is_running = False
        self.scheduler.shutdown()
        await asyncio.sleep(1)  # Brief pause for cleanup

    async def run(self):
        """Main process entry point"""
        pid_file = Path('/tmp/newsletter_process.pid')
        
        try:
            # Write PID file
            pid_file.write_text(str(os.getpid()))
            
            self.setup_jobs()
            self.scheduler.start()
            
            # Keep running until interrupted
            while True:
                await asyncio.sleep(1)
                
        except (KeyboardInterrupt, SystemExit):
            logger.info("Shutdown requested")
            await self.shutdown()
        finally:
            # Clean up PID file
            if pid_file.exists():
                pid_file.unlink()

def handle_interrupt(signum=None, frame=None):
    """Handle keyboard interrupt"""
    logger.info("Received interrupt signal")
    sys.exit(0)

def handle_sighup(signum=None, frame=None):
    """Handle SIGHUP for log rotation"""
    logger.info("Received SIGHUP signal - reopening log files")
    
    # Close all handlers
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)
    
    # Reinitialize logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/newsletter.log'),
            logging.StreamHandler()
        ]
    )

async def main():
    """Main entry point"""
    try:
        # Setup signal handlers
        signal.signal(signal.SIGINT, handle_interrupt)
        signal.signal(signal.SIGHUP, handle_sighup)  # Add SIGHUP handler
        signal.signal(signal.SIGTERM, handle_interrupt)
            
        process = NewsletterProcess()
        await process.run()
        
    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0) 