"""News generation process orchestrator"""

import logging
import asyncio
import sys
import os
from datetime import datetime, timezone, time
from pathlib import Path
import json
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import signal

from processors.data_processor import DataProcessor
from processors.alpha_filter import AlphaFilter
from processors.content_filter import ContentFilter
from processors.news_filter import NewsFilter
from senders.telegram_sender import TelegramSender
from senders.discord_sender import DiscordSender
from category_mapping import CATEGORY, DISCORD_WEBHOOKS

# Ensure logs directory exists
logs_dir = Path('logs')
if not logs_dir.exists():
    logs_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(logs_dir, 0o755)  # Set directory permissions

# Force immediate flushing of stdout
sys.stdout.reconfigure(line_buffering=True)

# Configure logging format
log_format = logging.Formatter('[%(levelname).1s] %(message)s')

# Configure main logger
logger = logging.getLogger('news_generator')
logger.setLevel(logging.INFO)
logger.handlers = []  # Clear any existing handlers

# Add handlers for main logger
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(log_format)
logger.addHandler(console_handler)

file_handler = logging.FileHandler('logs/news_generator.log', encoding='utf-8')
file_handler.setFormatter(log_format)
logger.addHandler(file_handler)

# Configure subprocess loggers
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
    sub_logger = logging.getLogger(logger_name)
    sub_logger.handlers = []  # Clear any existing handlers
    sub_logger.setLevel(logging.INFO)
    
    # Add handlers with same format
    sub_console = logging.StreamHandler(sys.stdout)
    sub_console.setFormatter(log_format)
    sub_logger.addHandler(sub_console)
    
    sub_file = logging.FileHandler('logs/news_generator.log', encoding='utf-8')
    sub_file.setFormatter(log_format)
    sub_logger.addHandler(sub_file)
    
    sub_logger.propagate = False  # Prevent duplicate logs

# Set external libraries to WARNING level
for lib_logger in ['httpx', 'openai', 'apscheduler']:
    logging.getLogger(lib_logger).setLevel(logging.WARNING)

class NewsGenerator:
    def __init__(self):
        logger.info("🚀 Initializing News Generator Process")
        # Load environment variables
        load_dotenv()
        logger.info("✓ Environment variables loaded")
        
        # Initialize scheduler with explicit timezone
        self.scheduler = AsyncIOScheduler(timezone=timezone.utc)
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
        
        # Initialize Discord sender if any webhooks are configured
        self.discord_sender = None
        if any(webhook for webhook in DISCORD_WEBHOOKS.values()):
            self.discord_sender = DiscordSender()
            logger.info("✓ Discord Sender initialized")
        else:
            logger.info("ℹ️ No Discord webhooks configured, Discord sending disabled")
        
        # Processing state flags
        self.is_processing = {
            'data_alpha': False,
            'content': False,
            'news': False
        }
        
        # Shutdown flag
        self.should_shutdown = False
        
        logger.info("✅ News Generator Process initialization complete")

    def _initialize_directories(self):
        """Initialize all required directories with proper permissions"""
        try:
            required_dirs = {
                'logs': self.log_dir,
                'data': self.data_dir,
                'processed': self.data_dir / 'processed',
                'alpha_filtered': self.data_dir / 'filtered' / 'alpha_filtered',
                'content_filtered': self.data_dir / 'filtered' / 'content_filtered',
                'news_filtered': self.data_dir / 'filtered' / 'news_filtered'
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
            
            # Clear any existing state files on startup
            state_files = [
                self.data_dir / 'filtered' / 'alpha_filtered' / 'state.json',
                self.data_dir / 'filtered' / 'content_filtered' / 'state.json',
                self.data_dir / 'filtered' / 'news_filtered' / 'state.json'
            ]
            
            for state_file in state_files:
                if state_file.exists():
                    state_file.unlink()
                    logger.info(f"Cleared existing state file: {state_file}")
            
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

    async def _clear_input_file(self, file_path: Path):
        """Clear input file after successful processing"""
        try:
            if file_path.exists():
                file_path.unlink()
            logger.info(f"Cleared input file: {file_path}")
        except Exception as e:
            logger.error(f"Error clearing input file: {str(e)}")

    async def _clear_state_files(self, stage):
        """Clear state files for a specific processing stage"""
        try:
            state_files = {
                'data_alpha': [
                    self.data_dir / 'filtered' / 'alpha_filtered' / 'state.json'
                ],
                'content': [
                    self.data_dir / 'filtered' / 'content_filtered' / 'state.json'
                ],
                'news': [
                    self.data_dir / 'filtered' / 'news_filtered' / 'state.json'
                ]
            }
            
            if stage in state_files:
                for state_file in state_files[stage]:
                    if state_file.exists():
                        state_file.unlink()
                        logger.info(f"Cleared state file: {state_file}")
        except Exception as e:
            logger.error(f"Error clearing state files for {stage}: {str(e)}")

    async def _clear_raw_files(self, date_str):
        """Clear raw input files after successful processing"""
        try:
            raw_dir = self.data_dir / 'raw' / date_str
            if raw_dir.exists():
                for file in raw_dir.glob('column_*.json'):
                    file.unlink()
                if not any(raw_dir.iterdir()):
                    raw_dir.rmdir()
                logger.info(f"Cleared raw files for date: {date_str}")
        except Exception as e:
            logger.error(f"Error clearing raw files: {str(e)}")

    async def process_data_and_alpha(self, date_str=None):
        """Process tweets through data processor and alpha filter"""
        if self.is_processing['data_alpha']:
            logger.warning("⚠️ Data and alpha processing already in progress")
            return False

        try:
            self.is_processing['data_alpha'] = True
            if not date_str:
                date_str = datetime.now(timezone.utc).strftime('%Y%m%d')
            
            logger.info(f"🔄 Starting data and alpha processing for date: {date_str}")
            
            # Clear any existing state files before processing
            await self._clear_state_files('data_alpha')
            
            # Step 1: Process tweets
            processed_count = await self.data_processor.process_tweets(date_str)
            if processed_count > 0:
                logger.info(f"✅ Processed {processed_count} tweets")
                
                # Step 2: Run alpha filter
                alpha_result = await self.alpha_filter.process_content(date_str)
                if alpha_result:
                    logger.info("✅ Alpha filtering complete")
                    
                    # Clear processed file after successful run
                    processed_file = self.data_dir / 'processed' / f'{date_str}_processed.json'
                    await self._clear_input_file(processed_file)
                    
                    # Clear raw input files
                    await self._clear_raw_files(date_str)
                    
                    return True
            else:
                logger.info("ℹ️ No new tweets to process")
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Error in data and alpha processing: {str(e)}")
            return False
        finally:
            self.is_processing['data_alpha'] = False

    async def process_content_filter(self):
        """Process accumulated alpha-filtered tweets through content filter"""
        if self.is_processing['content']:
            logger.warning("⚠️ Content filtering already in progress")
            return False

        try:
            self.is_processing['content'] = True
            logger.info("🔄 Starting content filtering of accumulated tweets")
            
            # Check if we have alpha-filtered content
            alpha_file = self.data_dir / 'filtered' / 'alpha_filtered' / 'combined_filtered.json'
            if not alpha_file.exists():
                logger.info("ℹ️ No alpha-filtered content to process")
                return False
            
            # Run content filter
            content_result = await self.content_filter.filter_content()
            if content_result:
                logger.info("✅ Content filtering complete")
                # Clear alpha filter output after successful processing
                await self._clear_input_file(alpha_file)
                # Clear state files
                await self._clear_state_files('content')
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Error in content filtering: {str(e)}")
            return False
        finally:
            self.is_processing['content'] = False

    async def process_news_and_send(self):
        """Process accumulated content-filtered tweets and send notifications"""
        if self.is_processing['news']:
            logger.warning("⚠️ News processing already in progress")
            return False

        try:
            self.is_processing['news'] = True
            current_date = datetime.now(timezone.utc).strftime('%Y%m%d')
            logger.info(f"🔄 Starting news processing and sending for date: {current_date}")
            
            # Check if we have content-filtered tweets
            content_file = self.data_dir / 'filtered' / 'content_filtered' / 'combined_filtered.json'
            if not content_file.exists():
                logger.info("ℹ️ No content-filtered tweets to process")
                return False
            
            # Run news filter
            news_result = await self.news_filter.process_all()
            if not news_result:
                logger.error("❌ News filtering failed")
                return False
            
            logger.info("✅ News filtering complete")
            
            # Verify news summary file exists
            news_file = self.data_dir / 'filtered' / 'news_filtered' / f'{CATEGORY.lower()}_summary_{current_date}.json'
            if not news_file.exists():
                logger.error("❌ News summary file not found")
                return False
            
            # Send notifications
            send_success = True
            
            # Send to Telegram
            logger.info("📤 Sending to Telegram")
            telegram_result = await self.telegram_sender.process_news_summary()
            if telegram_result:
                logger.info("✅ Telegram sending complete")
            else:
                logger.error("❌ Telegram sending failed")
                send_success = False
            
            # Send to Discord if configured
            if self.discord_sender:
                logger.info("📤 Sending to Discord")
                discord_result = await self.discord_sender.process_news_summary()
                if discord_result:
                    logger.info("✅ Discord sending complete")
                else:
                    logger.error("❌ Discord sending failed")
                    send_success = False
            
            if send_success:
                # Clear content filter output after successful processing
                await self._clear_input_file(content_file)
                # Clear state files
                await self._clear_state_files('news')
                logger.info("✅ News processing and sending complete")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Error in news processing and sending: {str(e)}")
            return False
        finally:
            self.is_processing['news'] = False

    def setup_schedules(self):
        """Setup scheduled jobs"""
        try:
            # Schedule data and alpha processing every 5 hours
            self.scheduler.add_job(
                self.process_data_and_alpha,
                CronTrigger(hour='*/5', minute='0', timezone=timezone.utc),
                id='data_alpha_job',
                replace_existing=True,
                misfire_grace_time=3600  # Allow job to run up to 1 hour late
            )
            logger.info("✓ Scheduled data and alpha processing every 5 hours")
            
            # Schedule content filtering to run 5 minutes after data_alpha
            self.scheduler.add_job(
                self.process_content_filter,
                CronTrigger(hour='*/5', minute='5', timezone=timezone.utc),  # Run 5 minutes after data_alpha
                id='content_filter_job',
                replace_existing=True,
                misfire_grace_time=3600  # Allow job to run up to 1 hour late
            )
            logger.info("✓ Scheduled content filtering 5 minutes after data_alpha")
            
            # Schedule news processing and sending at 23:00 (11 PM)
            self.scheduler.add_job(
                self.process_news_and_send,
                CronTrigger(hour='23', minute='0', timezone=timezone.utc),
                id='news_send_job',
                replace_existing=True,
                misfire_grace_time=3600  # Allow job to run up to 1 hour late
            )
            logger.info("✓ Scheduled news processing and sending at 23:00 UTC")
            
        except Exception as e:
            logger.error(f"Error setting up schedules: {str(e)}")
            raise

    async def shutdown(self):
        """Graceful shutdown handler"""
        logger.info("🛑 Initiating graceful shutdown...")
        self.should_shutdown = True
        
        # Wait for any ongoing processes to complete
        while any(self.is_processing.values()):
            logger.info("⏳ Waiting for ongoing processes to complete...")
            await asyncio.sleep(1)
        
        # Shutdown scheduler
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("✓ Scheduler shutdown complete")
        
        logger.info("✅ Graceful shutdown complete")

    async def run(self):
        """Main process runner"""
        try:
            # Setup signal handlers
            for sig in (signal.SIGTERM, signal.SIGINT):
                asyncio.get_event_loop().add_signal_handler(
                    sig,
                    lambda s=sig: asyncio.create_task(self.shutdown())
                )
            
            # Run initial processing cycle on startup
            logger.info("🔄 Running initial processing cycle on startup...")
            
            # Initial data and alpha processing
            data_alpha_result = await self.process_data_and_alpha()
            if data_alpha_result:
                logger.info("✅ Initial data and alpha processing complete")
                
                # Wait for 5 minutes before content filtering
                logger.info("⏳ Waiting 5 minutes before content filtering...")
                await asyncio.sleep(300)
                
                # Initial content filtering
                content_result = await self.process_content_filter()
                if content_result:
                    logger.info("✅ Initial content filtering complete")
            
            # Setup and start scheduler
            self.setup_schedules()
            self.scheduler.start()
            logger.info("🚀 News Generator started successfully")
            
            # Keep the process running until shutdown
            while not self.should_shutdown:
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"❌ Error in main process: {str(e)}")
        finally:
            await self.shutdown()

if __name__ == "__main__":
    try:
        generator = NewsGenerator()
        asyncio.run(generator.run())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)
