import os
import asyncio
import signal
import sys
import logging
from datetime import datetime, timedelta
import zoneinfo
from pathlib import Path
from dotenv import load_dotenv
from content_filter import ContentFilter
from news_filter import NewsFilter
from telegram_sender import TelegramSender
from error_handler import DataProcessingError, TelegramError

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/summary.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SummaryProcess:
    def __init__(self):
        # Load environment variables
        load_dotenv()
        
        # Initialize configuration
        self.config = {
            'deepseek_api_key': os.getenv('DEEPSEEK_API_KEY'),
            'openai_api_key': os.getenv('OPENAI_API_KEY'),
            'telegram_token': os.getenv('TELEGRAM_BOT_TOKEN'),
            'telegram_channels': {
                'polkadot': os.getenv('TELEGRAM_POLKADOT_CHANNEL_ID'),
                'iota': os.getenv('TELEGRAM_IOTA_CHANNEL_ID'),
                'arbitrum': os.getenv('TELEGRAM_ARBITRUM_CHANNEL_ID'),
                'near': os.getenv('TELEGRAM_NEAR_CHANNEL_ID'),
                'ai_agent': os.getenv('TELEGRAM_AI_AGENT_CHANNEL_ID'),
                'defi': os.getenv('TELEGRAM_DEFI_CHANNEL_ID'),
                'test': os.getenv('TELEGRAM_TEST_CHANNEL_ID')
            }
        }
        
        # Initialize components
        self.content_filter = ContentFilter(self.config)
        self.news_filter = NewsFilter(self.config)
        self.telegram_sender = TelegramSender(self.config['telegram_token'])
        
        # Get processing date (yesterday by default)
        self.processing_date = (
            datetime.now(zoneinfo.ZoneInfo("UTC")) - timedelta(days=1)
        ).strftime('%Y%m%d')
        
    async def run_content_filter(self):
        """Run content filtering process"""
        logger.info("Starting content filtering...")
        try:
            await self.content_filter.filter_content(self.processing_date)
            logger.info("Content filtering completed successfully")
            return True
        except Exception as e:
            logger.error(f"Content filtering failed: {str(e)}")
            raise DataProcessingError(f"Content filtering failed: {str(e)}")

    async def run_news_filter(self):
        """Run news filtering process"""
        logger.info("Starting news filtering...")
        try:
            await self.news_filter.process_all()
            logger.info("News filtering completed successfully")
            return True
        except Exception as e:
            logger.error(f"News filtering failed: {str(e)}")
            raise DataProcessingError(f"News filtering failed: {str(e)}")

    async def send_summaries(self):
        """Send filtered summaries to Telegram"""
        logger.info("Starting to send summaries...")
        try:
            # Find all summary files
            summary_dir = Path('data/filtered/news_filtered')
            summary_files = list(summary_dir.glob('*_summary.json'))
            
            if not summary_files:
                logger.warning("No summary files found to process")
                return False
            
            logger.info(f"Found {len(summary_files)} summary files to process")
            
            # Process each summary file
            for summary_file in summary_files:
                try:
                    logger.info(f"Processing {summary_file.name}")
                    await self.telegram_sender.process_news_summary(summary_file)
                    # Brief pause between messages
                    await asyncio.sleep(2)
                except Exception as e:
                    logger.error(f"Failed to process {summary_file.name}: {str(e)}")
                    continue
            
            logger.info("All summaries sent successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send summaries: {str(e)}")
            raise TelegramError(f"Failed to send summaries: {str(e)}")

    async def run(self):
        """Main summary process loop"""
        try:
            # Run content filter
            await self.run_content_filter()
            
            # Run news filter
            await self.run_news_filter()
            
            # Send summaries
            await self.send_summaries()
            
            logger.info("Summary process completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Summary process failed: {str(e)}")
            return False

def handle_interrupt(signum=None, frame=None):
    """Handle keyboard interrupt"""
    logger.info("Received interrupt signal - shutting down")
    sys.exit(0)

async def main():
    """Main entry point"""
    try:
        # Setup signal handlers
        signal.signal(signal.SIGINT, handle_interrupt)
        if sys.platform != 'win32':
            signal.signal(signal.SIGTERM, handle_interrupt)
            
        summary_process = SummaryProcess()
        success = await summary_process.run()
        
        # Exit with appropriate code
        sys.exit(0 if success else 1)
        
    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        sys.exit(1)
        
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0) 