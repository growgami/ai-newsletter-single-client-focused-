"""Tweet collection process for continuous background scraping"""

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
import psutil

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/tweet_collection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TweetCollector:
    def __init__(self):
        # Load environment variables
        load_dotenv()
        
        # Initialize configuration
        self.config = {
            'twitter_username': os.getenv('TWITTER_USERNAME'),
            'twitter_password': os.getenv('TWITTER_PASSWORD'),
            'twitter_2fa': os.getenv('TWITTER_VERIFICATION_CODE'),
            'tweetdeck_url': os.getenv('TWEETDECK_URL')
        }
        
        # Initialize components
        self.browser = None
        self.scraper = None
        self.is_running = True
        
        # Error tracking
        self.error_count = 0
        self.max_errors = 2  # Reset browser after 5 errors
        
        # Ensure directories exist
        self.setup_directories()
        
    def setup_directories(self):
        """Create necessary directories if they don't exist"""
        directories = ['data/raw', 'data/processed', 'data/session', 'logs']
        for dir_path in directories:
            Path(dir_path).mkdir(parents=True, exist_ok=True)
            
    async def initialize_browser(self):
        """Initialize and setup the browser"""
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
            
    async def validate_tweetdeck(self):
        """Validate we're on TweetDeck and columns are identified"""
        try:
            if not self.browser or not self.scraper:
                logger.error("Browser or scraper not initialized")
                self.error_count += 1  # Increment error count
                return False
                
            # Verify TweetDeck URL with incremental delays
            max_url_attempts = 3
            initial_delay = 1
            for attempt in range(max_url_attempts):
                current_url = self.browser.page.url
                if self.config['tweetdeck_url'] in current_url:
                    break
                    
                delay = initial_delay * (2 ** attempt)
                logger.warning(f"Not on TweetDeck (attempt {attempt + 1}). Current URL: {current_url}")
                logger.info(f"Waiting {delay} seconds before next attempt...")
                
                if attempt < max_url_attempts - 1:
                    await asyncio.sleep(delay)
                else:
                    self.error_count += 1  # Increment error count
                    return False
                
            # Verify columns are identified with incremental delays
            max_column_attempts = 3
            initial_delay = 2
            for attempt in range(max_column_attempts):
                if self.scraper.columns:
                    break
                    
                delay = initial_delay * (2 ** attempt)
                logger.warning(f"No columns identified (attempt {attempt + 1})")
                logger.info(f"Waiting {delay} seconds before next attempt...")
                
                if attempt < max_column_attempts - 1:
                    # Try to identify columns again
                    await self.scraper.identify_columns()
                    await asyncio.sleep(delay)
                else:
                    self.error_count += 1  # Increment error count
                    return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error validating TweetDeck: {str(e)}")
            self.error_count += 1  # Increment error count
            return False
            
    async def collect_tweets(self):
        """Main tweet collection loop"""
        while self.is_running:
            try:
                # Single try block for all operations
                results = await self.scraper.scrape_all_columns(is_monitoring=True)
                # Reset count only on successful scrape
                if results:
                    self.error_count = 0
                    logger.info("tweet_scraper - INFO - Successful scrape - resetting error count to 0")
                    
            except Exception as e:
                # Increment error count first
                self.error_count += 1
                error_msg = str(e)
                
                # Let the original error propagate from scraper
                if "Timeout" in error_msg and "ElementHandle.get_attribute" in error_msg:
                    # Error already logged by scraper
                    pass
                else:
                    logger.error(f"tweet_scraper - ERROR - Scraping error (error {self.error_count}/{self.max_errors}): {error_msg}")
                
                # Check max errors - if reached, exit process
                if self.error_count >= self.max_errors:
                    logger.critical(f"tweet_scraper - CRITICAL - Error threshold reached ({self.max_errors} errors), shutting down...")
                    await self.shutdown()
                    logger.critical("tweet_scraper - CRITICAL - Shutdown complete, exiting with code 1")
                    os._exit(1)  # Force exit to ensure PM2 restart
                    
                # If not max errors yet, wait and retry
                # Use longer delay for timeouts since they're often loading-related
                wait_time = 5 if "Timeout" in error_msg else 2
                logger.info(f"tweet_scraper - INFO - Waiting {wait_time} seconds before retry (error count: {self.error_count}/{self.max_errors})")
                await asyncio.sleep(wait_time)

    async def shutdown(self):
        """Cleanup and shutdown"""
        logger.info("Shutting down...")
        self.is_running = False
        
        # Close browser if open
        if self.browser:
            await self.browser.close()
            
        logger.info("Shutdown complete")

    async def run(self):
        """Main process entry point"""
        try:
            # Initialize browser and scraper
            await self.initialize_browser()
            
            # Start continuous collection
            await self.collect_tweets()
            
        except Exception as e:
            logger.error(f"Error in main loop: {str(e)}")
            await self.shutdown()
            sys.exit(1)  # Ensure PM2 restart for any unhandled errors
            
        finally:
            await self.shutdown()

def handle_interrupt(signum=None, frame=None):
    """Handle keyboard interrupt (Ctrl+C)"""
    logger.info("Received keyboard interrupt - performing clean shutdown")
    sys.exit(0)  # Clean exit - don't trigger PM2 restart

async def main():
    """Main entry point"""
    collector = None
    
    try:
        # Setup signal handlers
        signal.signal(signal.SIGINT, handle_interrupt)
        if sys.platform != 'win32':
            signal.signal(signal.SIGTERM, handle_interrupt)
            
        collector = TweetCollector()
        await collector.run()
        
    except SystemExit as e:
        # Handle the exit here, outside the async context
        logger.critical("SystemExit caught in main, initiating process termination...")
        sys.exit(e.code)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)
        
if __name__ == "__main__":
    try:
        logger.info("Starting tweet collection process...")
        asyncio.run(main())
    except SystemExit as e:
        # Handle the exit here, outside the async context
        logger.critical("SystemExit caught in process root, exiting with code {}...".format(e.code))
        sys.exit(e.code)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received at process root")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error at process root: {str(e)}")
        sys.exit(1)
