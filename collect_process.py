"""Daily tweet collection and processing service"""

import logging
import asyncio
from datetime import datetime, timedelta
import zoneinfo
from data_processor import DataProcessor
from alpha_filter import AlphaFilter
import os
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/collect.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CollectProcess:
    def __init__(self):
        # Load environment variables
        load_dotenv()
        
        # Initialize components
        self.data_processor = DataProcessor()
        
        # Initialize AlphaFilter with config
        self.alpha_filter = AlphaFilter({
            'deepseek_api_key': os.getenv('DEEPSEEK_API_KEY'),
            'openai_api_key': os.getenv('OPENAI_API_KEY'),
            'alpha_threshold': float(os.getenv('ALPHA_THRESHOLD', '0.8')),
            'risk_threshold': float(os.getenv('RISK_THRESHOLD', '0.4'))
        })
        
    async def process_daily_tweets(self, date_str=None):
        """Process tweets for a given date through data_processor and alpha_filter"""
        try:
            if not date_str:
                # Default to yesterday's date
                current_time = datetime.now(zoneinfo.ZoneInfo("UTC"))
                yesterday = current_time - timedelta(days=1)
                date_str = yesterday.strftime('%Y%m%d')
                
            logger.info(f"Starting daily tweet processing for date: {date_str}")
            
            # Step 1: Process raw tweets
            logger.info("Running data processor...")
            processed_count = await self.data_processor.process_tweets(date_str)
            if processed_count == 0:
                logger.warning("No tweets processed by data_processor")
                return False
            
            logger.info(f"Successfully processed {processed_count} tweets")
            
            # Step 2: Run alpha filter
            logger.info("Running alpha filter...")
            alpha_state = await self.alpha_filter.process_content(date_str)
            if not alpha_state:
                logger.error("Alpha filter processing failed")
                return False
                
            logger.info("Alpha filtering complete")
            return True
            
        except Exception as e:
            logger.error(f"Error in daily tweet processing: {str(e)}")
            return False
            
    async def run(self):
        """Main process entry point"""
        try:
            success = await self.process_daily_tweets()
            return success
            
        except Exception as e:
            logger.error(f"Error in collect process: {str(e)}")
            return False

if __name__ == "__main__":
    import sys
    
    collect_process = CollectProcess()
    
    # Get date from command line argument if provided
    date_to_process = sys.argv[1] if len(sys.argv) > 1 else None
    
    try:
        success = asyncio.run(collect_process.process_daily_tweets(date_to_process))
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1) 