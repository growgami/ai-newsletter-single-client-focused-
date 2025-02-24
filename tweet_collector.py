"""Tweet collection process for continuous background scraping"""

import os
import asyncio
import signal
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv
from browser_automation import BrowserAutomation
from deck_scraper import DeckScraper, ScrapingError

# Setup logging with UTF-8 encoding for Windows compatibility
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/tweet_collection.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)  # stdout for proper encoding
    ]
)

# Configure root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# Get logger for this module
logger = logging.getLogger(__name__)

# Configure child loggers
deck_scraper_logger = logging.getLogger('deck_scraper')
deck_scraper_logger.setLevel(logging.INFO)
deck_scraper_logger.propagate = True

# Reduce external library logging
logging.getLogger('playwright').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)

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
        
        # Simple error tracking - only resets on restart
        self.error_count = 0
        self.max_errors = 3
        
        # Ensure directories exist
        self.setup_directories()
        
    def _validate_config(self):
        """Validate all required configuration is present"""
        required_vars = [
            'twitter_username',
            'twitter_password',
            'twitter_2fa',
            'tweetdeck_url'
        ]
        
        missing = [var for var in required_vars if not self.config.get(var)]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
            
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
            logger.info("Initializing browser...")
            if not await self.browser.init_browser():
                raise Exception("Failed to initialize browser")
            
            # Handle login
            logger.info("Handling login...")
            if not await self.browser.handle_login():
                raise Exception("Failed to login to Twitter")
            
            # Initialize tweet scraper
            self.scraper = DeckScraper(self.browser.page, self.config)
            if not await self.scraper.identify_columns():
                raise Exception("Failed to identify TweetDeck columns")
            
            logger.info("Browser initialization complete")
            return True
            
        except Exception as e:
            logger.error(f"Browser initialization error: {str(e)}")
            if self.browser:
                await self.browser.close()
            return False

    async def validate_tweetdeck(self):
        """Validate we're on TweetDeck and columns are identified"""
        try:
            if not self.browser or not self.scraper:
                raise Exception("Browser or scraper not initialized")
                
            # Verify TweetDeck URL
            current_url = self.browser.page.url
            if self.config['tweetdeck_url'] not in current_url:
                raise Exception(f"Not on TweetDeck. Current URL: {current_url}")
                
            # Verify columns are identified
            if not self.scraper.columns:
                raise Exception("No columns identified")
                
            return True
            
        except Exception as e:
            logger.error(f"TweetDeck validation error: {str(e)}")
            return False

    async def collect_tweets(self):
        """Main loop for collecting tweets"""
        while self.is_running:
            try:
                # Validate TweetDeck state before scraping
                if not await self.validate_tweetdeck():
                    self.error_count += 1
                    logger.error(f"[Error {self.error_count}/{self.max_errors}] TweetDeck validation failed")
                    if self.error_count >= self.max_errors:
                        await self.handle_critical_error("TweetDeck validation failed")
                    await asyncio.sleep(1)
                    continue

                # Scrape columns
                try:
                    results = await self.scraper.scrape_all_columns(is_monitoring=True)
                    if results:
                        logger.info(f"Found new tweets in {len(results)} columns")
                except ScrapingError as e:
                    self.error_count += 1
                    logger.error(f"[Error {self.error_count}/{self.max_errors}] {str(e)}")
                    if self.error_count >= self.max_errors:
                        await self.handle_critical_error("Error threshold reached")
                    continue  # Skip the sleep below and start next iteration

                # Wait 1 second before next cycle
                await asyncio.sleep(1)

            except Exception as e:
                # Only count unexpected errors not already caught above
                if not isinstance(e, ScrapingError):
                    self.error_count += 1
                    logger.error(f"[Error {self.error_count}/{self.max_errors}] Unexpected error: {str(e)}")
                    if self.error_count >= self.max_errors:
                        await self.handle_critical_error("Error threshold reached")
                await asyncio.sleep(1)

    async def handle_critical_error(self, reason):
        """Handle critical error state with guaranteed cleanup"""
        logger.critical(f"[CRITICAL] RESTARTING - {reason}")
        try:
            # Ensure browser cleanup with timeout
            if self.browser:
                try:
                    await asyncio.wait_for(self.browser.close(), timeout=10.0)
                except asyncio.TimeoutError:
                    logger.error("Browser shutdown timed out")
                except Exception as e:
                    logger.error(f"Browser shutdown error: {str(e)}")

            # Ensure scraper cleanup
            if self.scraper:
                try:
                    await asyncio.wait_for(self.shutdown(), timeout=5.0)
                except asyncio.TimeoutError:
                    logger.error("Scraper shutdown timed out")
                except Exception as e:
                    logger.error(f"Scraper shutdown error: {str(e)}")

        except Exception as e:
            logger.error(f"Critical error during shutdown: {str(e)}")
        finally:
            # Log final error count and force exit
            logger.critical(f"Exiting after {self.error_count} errors: {reason}")
            os._exit(1)  # Force exit to ensure PM2 restart

    async def shutdown(self):
        """Cleanup and shutdown with timeout"""
        logger.info("Shutting down...")
        self.is_running = False
        
        try:
            # Add timeout to browser close
            if self.browser:
                await asyncio.wait_for(self.browser.close(), timeout=10.0)
        except asyncio.TimeoutError:
            logger.error("Browser shutdown timed out")
        except Exception as e:
            logger.error(f"Error during browser shutdown: {str(e)}")
        finally:
            logger.info("Shutdown complete")

    async def run(self):
        """Main process entry point"""
        try:
            # Initialize browser and scraper
            if not await self.initialize_browser():
                logger.error("Failed to initialize browser, exiting...")
                return
                
            logger.info("Browser setup successful - press Ctrl+C to exit")
            
            # Start continuous collection
            await self.collect_tweets()
            
        except Exception as e:
            logger.error(f"Error in main loop: {str(e)}")
        finally:
            await self.shutdown()

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info("\nShutdown signal received")
    if collector:
        collector.is_running = False

if __name__ == "__main__":
    # Initialize collector
    collector = None
    
    try:
        # Setup signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        if sys.platform != 'win32':
            signal.signal(signal.SIGTERM, signal_handler)
            
        # Create and run collector
        collector = TweetCollector()
        asyncio.run(collector.run())
        
    except KeyboardInterrupt:
        logger.info("\nKeyboard interrupt received")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1) 