"""Process scheduler and orchestrator"""

import logging
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import json
from pathlib import Path
from scraper_process import TweetScraperProcess
from collect_process import CollectProcess
from summary_process import SummaryProcess

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ProcessScheduler:
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.scraper = TweetScraperProcess()
        self.collect_process = CollectProcess()
        self.summary_process = SummaryProcess()
        
    async def run_scraper(self):
        """Run the tweet scraper process"""
        try:
            await self.scraper.run()
        except Exception as e:
            logger.error(f"Error in scraper process: {str(e)}")
            
    async def run_collect_process(self):
        """Run the daily collection process"""
        try:
            success = await self.collect_process.run()
            if success:
                # Check if we should run summary process
                await self.check_and_run_summary()
        except Exception as e:
            logger.error(f"Error in collect process: {str(e)}")
            
    async def check_and_run_summary(self):
        """Check alpha filtered output and run summary if conditions met"""
        try:
            # Check alpha filtered output directory
            alpha_dir = Path('data/filtered/alpha_filtered')
            if not alpha_dir.exists():
                logger.warning("Alpha filtered directory not found")
                return
                
            # Check each column file
            min_tweets_required = 15
            all_columns_ready = True
            
            for col_file in alpha_dir.glob('column_*.json'):
                try:
                    with open(col_file, 'r') as f:
                        data = json.load(f)
                        tweet_count = len(data.get('tweets', []))
                        if tweet_count < min_tweets_required:
                            logger.info(f"{col_file.name} has {tweet_count} tweets (need {min_tweets_required})")
                            all_columns_ready = False
                            break
                except Exception as e:
                    logger.error(f"Error reading {col_file}: {str(e)}")
                    all_columns_ready = False
                    break
                    
            if all_columns_ready:
                logger.info("All columns have sufficient tweets, running summary process")
                await self.run_summary_process()
            else:
                logger.info("Not all columns have sufficient tweets yet")
                
        except Exception as e:
            logger.error(f"Error checking alpha filtered output: {str(e)}")
            
    async def run_summary_process(self):
        """Run the summary process"""
        try:
            await self.summary_process.run()
        except Exception as e:
            logger.error(f"Error in summary process: {str(e)}")
            
    def setup_jobs(self):
        """Setup scheduled jobs"""
        # Ensure scraper is running (will use its own internal monitoring interval)
        self.scheduler.add_job(
            self.run_scraper,
            'date',  # Run once when scheduler starts
            id='scraper',
            replace_existing=True
        )
        
        # Run collect process daily at 4 AM UTC
        self.scheduler.add_job(
            self.run_collect_process,
            CronTrigger(hour=4, minute=0),  # 4 AM UTC
            id='collect_process',
            replace_existing=True
        )
        
        logger.info("Scheduled jobs setup complete")
        
    async def run(self):
        """Main scheduler entry point"""
        try:
            self.setup_jobs()
            self.scheduler.start()
            
            # Keep running until interrupted
            while True:
                await asyncio.sleep(1)
                
        except (KeyboardInterrupt, SystemExit):
            logger.info("Scheduler shutdown requested")
            self.scheduler.shutdown()
            
if __name__ == "__main__":
    scheduler = ProcessScheduler()
    
    try:
        asyncio.run(scheduler.run())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise 