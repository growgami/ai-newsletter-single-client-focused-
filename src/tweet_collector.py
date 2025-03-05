#!/usr/bin/env python
"""
Twitter List Scraper - Scheduled Collector

This script scrapes tweets from Twitter lists organized by categories
and saves them to JSON files. It's designed to run on a schedule at midnight.
"""
import os
import sys
import time
import asyncio
import concurrent.futures
import signal
import logging
from typing import Dict, List, Any
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from core.list_scraper import ListScraper
from core.file_handler import FileHandler

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)

# Create logs directory if it doesn't exist
Path('logs').mkdir(exist_ok=True)

# Add file handler after ensuring directory exists
logging.getLogger().addHandler(logging.FileHandler('logs/tweet_collector.log'))

logger = logging.getLogger('tweet_collector')

# Reduce logger verbosity for other modules
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)

class TweetCollector:
    def __init__(self):
        # Explicitly load .env from project root, not src/
        env_path = Path(__file__).parent.parent / '.env'
        logger.info(f"Looking for .env at: {env_path.absolute()}")
        
        # Try loading with absolute path
        if env_path.exists():
            logger.info(f".env file exists, loading with absolute path: {env_path.absolute()}")
            load_dotenv(dotenv_path=str(env_path.absolute()))
            
            # Debug check
            api_key = os.getenv('TWITTER_API_KEY')
            if api_key:
                masked_key = api_key[:4] + "..." + api_key[-4:] if len(api_key) > 8 else "***"
                logger.info(f"Successfully loaded API key: {masked_key}")
            else:
                logger.warning("API key not found after loading .env, trying direct file loading")
                try:
                    with open(env_path, 'r') as f:
                        for line in f:
                            if line.strip().startswith('TWITTER_API_KEY='):
                                api_key = line.strip().split('=', 1)[1]
                                os.environ['TWITTER_API_KEY'] = api_key
                                logger.info("Manually loaded API key from .env file")
                                break
                except Exception as e:
                    logger.error(f"Error manually loading .env: {str(e)}")
        else:
            logger.error(f".env file not found at: {env_path.absolute()}")
            # Try src directory as fallback
            src_env_path = Path(__file__).parent / '.env'
            if src_env_path.exists():
                logger.info(f"Found .env in src directory: {src_env_path}")
                load_dotenv(dotenv_path=str(src_env_path.absolute()))
        
        # Log environment settings
        logger.info(f"Current working directory: {os.getcwd()}")
        
        # Initialize configuration
        self.config = {
            'api_key': os.getenv('TWITTER_API_KEY'),
            'output_dir': os.getenv('OUTPUT_DIR', 'data/raw'),
            'categories_config': os.getenv('CATEGORIES_CONFIG', 'categories.json'),
            'max_workers': int(os.getenv('MAX_SCRAPER_WORKERS', '5')),
            'days_to_scrape': int(os.getenv('DAYS_TO_SCRAPE', '1')),
            'category_workers': int(os.getenv('CATEGORY_WORKERS', '1'))
        }
        
        # Log API key (masked)
        if self.config['api_key']:
            masked_key = self.config['api_key'][:4] + "..." + self.config['api_key'][-4:]
            logger.info(f"API Key loaded: {masked_key}")
        else:
            logger.error("No API key found in environment variables!")
        
        # Validate configuration
        self._validate_config()
        
        # Initialize tracking state
        self.is_running = True
        
        # Use yesterday's date by default (collecting past 24h data)
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        self.current_date = yesterday.strftime('%Y%m%d')
        logger.info(f"Using yesterday's date for tweet collection: {self.current_date}")
        
        self.date_file = Path('data/session/current_date.txt')
        self._load_or_initialize_date()
        
        # Setup file handler
        self.file_handler = FileHandler()
        
        # Error tracking
        self.error_count = 0
        self.max_errors = 3
        
        # Setup scheduler for midnight runs
        self.scheduler = AsyncIOScheduler()
    
    def _load_or_initialize_date(self):
        """Load saved date or initialize with yesterday's date"""
        try:
            # Create session directory if it doesn't exist
            Path('data/session').mkdir(parents=True, exist_ok=True)
            
            # If date file exists, use it
            if self.date_file.exists():
                saved_date = self.date_file.read_text().strip()
                
                # Calculate yesterday's date for comparison
                yesterday = datetime.now(timezone.utc) - timedelta(days=1)
                yesterday_str = yesterday.strftime('%Y%m%d')
                
                # Verify the saved date is not in the future
                try:
                    saved_dt = datetime.strptime(saved_date, '%Y%m%d')
                    current_dt = datetime.now()
                    
                    if saved_dt > current_dt:
                        logger.warning(f"Saved date {saved_date} is in the future! Using yesterday's date instead.")
                        self.current_date = yesterday_str
                        self._save_current_date()
                    else:
                        self.current_date = saved_date
                        logger.info(f"Loaded saved date: {self.current_date}")
                except ValueError:
                    # Invalid date format in file
                    logger.warning(f"Invalid date format in saved file: {saved_date}. Using yesterday's date.")
                    self.current_date = yesterday_str
                    self._save_current_date()
            else:
                # If no date file, create one with yesterday's date
                yesterday = datetime.now(timezone.utc) - timedelta(days=1)
                self.current_date = yesterday.strftime('%Y%m%d')
                self._save_current_date()
                
        except Exception as e:
            logger.error(f"Error loading date: {str(e)}")
            # Fall back to yesterday's date if there's an error
            yesterday = datetime.now(timezone.utc) - timedelta(days=1)
            self.current_date = yesterday.strftime('%Y%m%d')
    
    def _save_current_date(self):
        """Save current date to file"""
        try:
            self.date_file.write_text(self.current_date)
            logger.info(f"Saved current date: {self.current_date}")
        except Exception as e:
            logger.error(f"Error saving date: {str(e)}")
    
    def _check_and_update_date(self):
        """Check if date should be updated (at midnight)"""
        # Always use yesterday's date for tweet collection
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        yesterday_str = yesterday.strftime('%Y%m%d')
        
        if yesterday_str != self.current_date:
            logger.info(f"Date changed from {self.current_date} to {yesterday_str}")
            self.current_date = yesterday_str
            self._save_current_date()
            
            # Create the new date directory
            Path(f"{self.config['output_dir']}/{self.current_date}").mkdir(parents=True, exist_ok=True)
            
    def _validate_config(self):
        """Validate required configuration values"""
        if not self.config['api_key']:
            logger.error("Missing Twitter API key")
            raise ValueError("TWITTER_API_KEY must be provided in .env file")
        
        # Set environment variable for components that read directly from env
        os.environ['TWITTER_API_KEY'] = self.config['api_key']
        os.environ['OUTPUT_DIR'] = self.config['output_dir']
        os.environ['MAX_SCRAPER_WORKERS'] = str(self.config['max_workers'])
        os.environ['DAYS_TO_SCRAPE'] = str(self.config['days_to_scrape'])
        
        logger.info("Configuration validated")
            
    def setup_directories(self):
        """Ensure all required directories exist"""
        directories = [
            self.config['output_dir'],
            f"{self.config['output_dir']}/{self.current_date}",
            'logs'
        ]
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
            
        logger.info("Directories setup complete")

    def process_category(self, category: str, list_urls: List[str]) -> Dict[str, Any]:
        """Process a single category and return summary stats"""
        start_time = time.time()
        
        logger.info(f"Processing category: {category}")
        logger.info(f"Lists to scrape: {len(list_urls)}")
        
        # Create list scraper for this category
        list_scraper = ListScraper()
        
        try:
            # Scrape tweets from all lists in this category
            category_tweets = list_scraper.scrape_category_lists(category, list_urls)
            
            # Save category tweets to a JSON file with current date
            os.environ['TARGET_DATE'] = self.current_date
            output_file = self.file_handler.save_category_tweets(category, category_tweets)
            
            # Return stats
            return {
                "category": category,
                "status": "success",
                "tweet_count": len(category_tweets),
                "list_count": len(list_urls),
                "processing_time": time.time() - start_time,
                "output_file": output_file
            }
        except Exception as e:
            logger.error(f"Error processing category {category}: {str(e)}")
            return {
                "category": category,
                "status": "error",
                "error": str(e),
                "processing_time": time.time() - start_time
            }

    async def collect_tweets(self):
        """Collect tweets from all categories"""
        try:
            # Update the date tracking
            self._check_and_update_date()
            
            # Set the target date for output
            os.environ['TARGET_DATE'] = self.current_date
            
            # Load category configuration
            categories = self.file_handler.load_category_configuration(self.config['categories_config'])
            
            if not categories:
                logger.error("No categories found. Please check your configuration file.")
                return
            
            logger.info(f"Found {len(categories)} categories in configuration")
            
            # Process categories
            category_results = []
            
            if self.config['category_workers'] > 1 and len(categories) > 1:
                logger.info(f"Processing {len(categories)} categories in parallel using {self.config['category_workers']} workers")
                
                # Process categories in parallel using Python's ThreadPoolExecutor
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.config['category_workers']) as executor:
                    # Submit all category processing jobs
                    future_to_category = {
                        executor.submit(self.process_category, category, list_urls): category 
                        for category, list_urls in categories.items()
                    }
                    
                    # Process results as they come in
                    for future in concurrent.futures.as_completed(future_to_category):
                        category = future_to_category[future]
                        try:
                            result = future.result()
                            category_results.append(result)
                        except Exception as e:
                            logger.error(f"Exception processing category {category}: {str(e)}")
                            category_results.append({
                                "category": category,
                                "status": "error",
                                "error": str(e)
                            })
            else:
                # Process categories sequentially
                for category, list_urls in categories.items():
                    result = self.process_category(category, list_urls)
                    category_results.append(result)
            
            # Print summary
            logger.info("=== Summary ===")
            success_count = sum(1 for r in category_results if r.get("status") == "success")
            error_count = sum(1 for r in category_results if r.get("status") == "error")
            total_tweets = sum(r.get("tweet_count", 0) for r in category_results)
            
            logger.info(f"Total categories processed: {len(category_results)}")
            logger.info(f"Successful categories: {success_count}")
            logger.info(f"Failed categories: {error_count}")
            logger.info(f"Total tweets collected: {total_tweets}")
            
            if error_count > 0:
                self.error_count += 1
                if self.error_count >= self.max_errors:
                    await self.handle_critical_error(f"Too many errors ({error_count} categories failed)")
                    
            return success_count, error_count, total_tweets
            
        except Exception as e:
            logger.error(f"Error in collect_tweets: {str(e)}")
            self.error_count += 1
            if self.error_count >= self.max_errors:
                await self.handle_critical_error(f"Critical error in collect_tweets: {str(e)}")
            return 0, 1, 0

    async def handle_critical_error(self, reason):
        """Handle critical errors by logging and potentially shutting down"""
        logger.critical(f"CRITICAL ERROR: {reason}")
        logger.critical("Please check the application logs and fix the issue.")
        # Keep running, but log the critical error
        self.error_count = 0  # Reset error count to avoid immediate shutdown loop
        
    def setup_schedules(self):
        """Setup scheduled jobs"""
        # Run at midnight every day
        self.scheduler.add_job(
            self.collect_tweets,
            'cron',
            hour=0,
            minute=0,
            second=0,
            id='midnight_collection'
        )
        
        # Also run immediately on startup
        self.scheduler.add_job(
            self.collect_tweets,
            'date',
            run_date=datetime.now() + timedelta(seconds=10),
            id='startup_collection'
        )
        
        logger.info("Scheduled jobs setup complete")
        
    async def shutdown(self):
        """Graceful shutdown procedure"""
        logger.info("Shutting down...")
        
        # Cancel all pending tasks
        self.is_running = False
        
        # Stop the scheduler
        if self.scheduler.running:
            self.scheduler.shutdown()
        
        logger.info("Shutdown complete")

    async def run(self):
        """Main entry point to run the collector"""
        try:
            # Setup initial state
            self.setup_directories()
            
            # Setup scheduled jobs
            self.setup_schedules()
            self.scheduler.start()
            
            logger.info("Tweet collector started. Running on schedule (midnight daily).")
            
            # Keep the process running
            while self.is_running:
                await asyncio.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
        except Exception as e:
            logger.critical(f"Unexpected error: {str(e)}")
        finally:
            await self.shutdown()

# Global instance for signal handling
collector = None

def signal_handler(signum, frame):
    """Handle termination signals"""
    if collector:
        logger.info(f"Received signal {signum}, shutting down...")
        collector.is_running = False

if __name__ == "__main__":
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create and run collector
    collector = TweetCollector()
    asyncio.run(collector.run())
