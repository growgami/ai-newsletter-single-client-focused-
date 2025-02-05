import os
import asyncio
import signal
import sys
import logging
from datetime import datetime
import zoneinfo
from pathlib import Path
from dotenv import load_dotenv
from browser_automation import BrowserAutomation
from tweet_scraper import TweetScraper
from error_handler import with_retry, RetryConfig, BrowserError

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TweetScraperProcess:
    def __init__(self):
        # Load environment variables
        load_dotenv()
        
        # Initialize configuration
        self.config = {
            'twitter_username': os.getenv('TWITTER_USERNAME'),
            'twitter_password': os.getenv('TWITTER_PASSWORD'),
            'twitter_2fa': os.getenv('TWITTER_VERIFICATION_CODE'),
            'tweetdeck_url': os.getenv('TWEETDECK_URL'),
            'monitor_interval': float(os.getenv('MONITOR_INTERVAL', '0.1')),
            'max_retries': int(os.getenv('MAX_RETRIES', '3')),
            'retry_delay': float(os.getenv('RETRY_DELAY', '2.0'))
        }
        
        # Initialize components
        self.browser = None
        self.scraper = None
        self.is_running = True
        
        # Track monitoring stats
        self.monitor_stats = {
            'start_time': datetime.now(zoneinfo.ZoneInfo("UTC")),
            'total_checks': 0,
            'total_tweets_found': 0,
            'errors': 0
        }
        
        # Ensure data directories exist
        self.setup_directories()
        
    def setup_directories(self):
        """Create necessary directories if they don't exist"""
        directories = ['data/raw', 'data/processed', 'data/session', 'logs']
        for dir_path in directories:
            Path(dir_path).mkdir(parents=True, exist_ok=True)
            
    @with_retry(RetryConfig(max_retries=3, base_delay=2.0))
    async def initialize_browser(self):
        """Initialize and setup the browser for scraping with retry logic"""
        logger.info("Initializing browser...")
        
        try:
            # Initialize browser components
            self.browser = BrowserAutomation(self.config)
            
            # Initialize browser
            if not await self.browser.init_browser():
                raise BrowserError("Failed to initialize browser")
            
            # Handle login
            if not await self.browser.handle_login():
                raise BrowserError("Failed to login to Twitter")
            
            # Initialize tweet scraper
            self.scraper = TweetScraper(self.browser.page, self.config)
            if not await self.scraper.identify_columns():
                raise BrowserError("Failed to identify TweetDeck columns")
            
            logger.info("Browser initialization complete")
            return True
            
        except Exception as e:
            logger.error(f"Browser initialization error: {str(e)}")
            if self.browser:
                await self.browser.close()
            raise BrowserError(f"Failed to initialize browser: {str(e)}")
            
    async def initial_scrape(self):
        """Initial scraping of all tweets from all columns"""
        logger.info("Starting initial tweet scrape...")
        
        # Load any existing latest tweet IDs
        self.scraper.load_latest_tweets()
        
        # Scrape all columns concurrently
        results = await self.scraper.scrape_all_columns(is_monitoring=False)
        
        # Log results
        total_tweets = sum(count for _, count in results)
        for column_id, count in results:
            column = self.scraper.columns[column_id]
            logger.info(f"Initially saved {count} tweets from column {column['title']}")
            
        logger.info(f"Initial scrape complete. Total tweets saved: {total_tweets}")
        
    async def monitor_tweets(self):
        """Check all columns concurrently for updates"""
        try:
            # Scrape all columns concurrently
            results = await self.scraper.scrape_all_columns(is_monitoring=True)
            
            # Log results only if new tweets found
            if results:
                total_new_tweets = sum(count for _, count in results)
                if total_new_tweets > 0:
                    for column_id, count in results:
                        if count > 0:
                            column = self.scraper.columns[column_id]
                            logger.info(f"Found {count} new tweets in column {column['title']}")
                    logger.info(f"Total new tweets found: {total_new_tweets}")
                return results
            return None
            
        except Exception as e:
            logger.error(f"Error monitoring tweets: {str(e)}")
            return None

    async def continuous_scraping(self):
        """Continuously monitor for new tweets"""
        consecutive_errors = 0
        max_consecutive_errors = 3
        
        while self.is_running:
            try:
                # Monitor for new tweets
                self.monitor_stats['total_checks'] += 1
                results = await self.monitor_tweets()
                
                if results:
                    total_new_tweets = sum(count for _, count in results)
                    self.monitor_stats['total_tweets_found'] += total_new_tweets
                    
                # Reset error counter on success
                consecutive_errors = 0
                
                # Brief pause between checks
                await asyncio.sleep(self.config['monitor_interval'])
                
            except Exception as e:
                self.monitor_stats['errors'] += 1
                consecutive_errors += 1
                logger.error(f"Error in scraping loop: {str(e)}")
                
                if consecutive_errors >= max_consecutive_errors:
                    logger.critical("Too many consecutive errors, attempting browser reinitialization")
                    try:
                        await self.initialize_browser()
                        consecutive_errors = 0
                    except Exception as reinit_error:
                        logger.error(f"Failed to reinitialize browser: {str(reinit_error)}")
                        
                # Exponential backoff on error
                await asyncio.sleep(min(60, 2 ** consecutive_errors))  # Max 60 seconds

    async def shutdown(self):
        """Cleanup and shutdown"""
        logger.info("Shutting down...")
        self.is_running = False
        
        # Close browser if open
        if self.browser:
            await self.browser.close()
            
        logger.info("Shutdown complete")

    async def run(self):
        """Main scraper process loop"""
        try:
            # Initialize browser and scraper
            await self.initialize_browser()
            
            # Initial scrape of all columns
            await self.initial_scrape()
            
            # Start continuous scraping
            await self.continuous_scraping()
            
        except Exception as e:
            logger.error(f"Error in main loop: {str(e)}")
            await self.shutdown()
            
        finally:
            await self.shutdown()

def handle_interrupt(signum=None, frame=None):
    """Handle keyboard interrupt"""
    logger.info("Received interrupt signal - shutting down")
    sys.exit(0)

async def main():
    """Main entry point"""
    scraper = None
    
    try:
        # Setup signal handlers
        signal.signal(signal.SIGINT, handle_interrupt)
        if sys.platform != 'win32':
            signal.signal(signal.SIGTERM, handle_interrupt)
            
        scraper = TweetScraperProcess()
        await scraper.run()
        
    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        if scraper:
            await scraper.shutdown()
        sys.exit(1)
        
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0) 