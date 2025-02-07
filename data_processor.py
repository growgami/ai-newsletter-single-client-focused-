import logging
import json
from pathlib import Path
import re
import asyncio
from datetime import datetime, timedelta
import zoneinfo
from error_handler import with_retry, DataProcessingError, log_error, RetryConfig

logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self):
        self.data_dir = Path('data')
        self.raw_dir = self.data_dir / 'raw'
        self.processed_dir = self.data_dir / 'processed'
        self.retry_config = RetryConfig(max_retries=3, base_delay=1.0, max_delay=15.0)
        self.min_words = 2
        self.min_tweets_per_category = 3
        
        # Ensure directories exist
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
    def load_column_tweets(self, column_file):
        """Load tweets from a column file"""
        try:
            with open(column_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading tweets from {column_file}: {str(e)}")
            return []
            
    def normalize_text(self, text):
        """Normalize special characters and symbols from text"""
        if not text:
            return text
            
        # Replace multiple spaces with single space
        text = re.sub(r'\s+', ' ', text)
        
        # Remove non-printable characters
        text = ''.join(char for char in text if char.isprintable())
        
        # Normalize unicode characters
        text = text.replace('"', '"').replace('"', '"')  # Smart quotes
        text = text.replace(''', "'").replace(''', "'")  # Smart apostrophes
        text = text.replace('…', '...')  # Ellipsis
        text = text.replace('–', '-')    # En dash
        text = text.replace('—', '-')    # Em dash
        
        # Remove URLs (optional, but they often contain special chars)
        text = re.sub(r'http[s]?://\S+', '', text)
        
        # Remove leading/trailing whitespace
        text = text.strip()
        
        return text
        
    def is_valid_tweet(self, tweet):
        """Check if a tweet is valid according to our criteria"""
        # Must have text
        if not tweet.get('text'):
            return False
            
        # Text must be at least 2 words (after normalization)
        normalized_text = self.normalize_text(tweet['text'])
        words = [w for w in normalized_text.split() if w.strip()]  # Remove empty strings
        if len(words) < 2:
            return False
            
        return True
        
    @with_retry(RetryConfig(max_retries=3, base_delay=1.0))
    async def process_tweets(self, date_str=None):
        """Process tweets with retry logic"""
        try:
            if not date_str:
                # Default to yesterday's date
                current_time = datetime.now(zoneinfo.ZoneInfo("UTC"))
                yesterday = current_time - timedelta(days=1)
                date_str = yesterday.strftime('%Y%m%d')
                
            logger.info(f"Processing tweets for date: {date_str}")
            
            # Set the date-specific directory
            self.today_dir = self.raw_dir / date_str
            if not self.today_dir.exists():
                logger.error(f"Raw directory not found: {self.today_dir}")
                return 0
            
            # Load and combine raw tweets
            raw_columns = await self._load_raw_tweets(date_str)
            if not raw_columns:
                logger.warning("No raw tweets found to process")
                return 0
                
            # Process tweets by column
            processed_data = self.process_columns(raw_columns)
            logger.info(f"Final count: {processed_data['metadata']['total_tweets']} tweets remaining")
            
            # Save processed tweets
            await self._save_processed_tweets(processed_data, date_str)
            
            return processed_data['metadata']['total_tweets']
            
        except Exception as e:
            log_error(logger, e, f"Failed to process tweets for date {date_str}")
            raise DataProcessingError(f"Tweet processing failed: {str(e)}")
            
    async def _load_raw_tweets(self, date_str):
        """Load raw tweets with error handling"""
        try:
            columns = {}
            total_tweets = 0
            
            # Check if directory exists
            if not self.today_dir.exists():
                logger.error(f"Directory not found: {self.today_dir}")
                return {}
                
            # List all files in directory
            files = list(self.today_dir.glob('column_*.json'))
            if not files:
                logger.error(f"No column_*.json files found in {self.today_dir}")
                return {}
                
            logger.info(f"Found {len(files)} column files: {[f.name for f in files]}")
            
            for file in files:
                try:
                    column_id = file.stem.split('_')[1]  # Get column number from filename
                    logger.info(f"Loading tweets from {file.name}")
                    
                    with open(file, 'r', encoding='utf-8') as f:
                        tweets = json.load(f)
                        columns[column_id] = tweets
                        total_tweets += len(tweets)
                        logger.info(f"Loaded {len(tweets)} tweets from {file.name}")
                        
                except Exception as e:
                    log_error(logger, e, f"Failed to load tweets from {file}")
                    continue
                    
            logger.info(f"Loaded {total_tweets} raw tweets from {len(columns)} columns in {self.today_dir}")
            return columns
            
        except Exception as e:
            log_error(logger, e, "Failed to load raw tweets")
            raise DataProcessingError(f"Raw tweet loading failed: {str(e)}")
            
    def deduplicate(self, columns):
        """Step 3.1: Remove duplicate tweets by ID across all columns while maintaining original column structure"""
        initial_total = sum(len(t) for t in columns.values())
        seen_ids = set()
        deduped = {}
        
        # Process columns in order, keeping first occurrence of each tweet
        for col_id in sorted(columns.keys()):
            deduped[col_id] = []
            for tweet in columns[col_id]:
                tweet_id = tweet.get('id')
                if tweet_id and tweet_id not in seen_ids:
                    seen_ids.add(tweet_id)
                    deduped[col_id].append(tweet)
            
            logger.info(f"Column {col_id}: {len(columns[col_id])}→{len(deduped[col_id])} tweets after deduplication")
        
        deduped_total = sum(len(t) for t in deduped.values())
        logger.info(f"Cross-column deduplication: {initial_total} → {deduped_total} tweets (-{initial_total - deduped_total})")
        return deduped
            
    def _remove_duplicates(self, tweets):
        """Remove duplicate tweets based on tweet ID"""
        try:
            seen_ids = set()
            unique_tweets = []
            
            for tweet in tweets:
                tweet_id = tweet.get('id')
                if not tweet_id or tweet_id in seen_ids:
                    continue
                    
                seen_ids.add(tweet_id)
                unique_tweets.append(tweet)
                
            logger.info(f"Removed {len(tweets) - len(unique_tweets)} duplicate tweets")
            return unique_tweets
            
        except Exception as e:
            log_error(logger, e, "Failed to remove duplicates")
            raise DataProcessingError(f"Duplicate removal failed: {str(e)}")
            
    def _normalize_tweet(self, tweet):
        """Normalize tweet text and metadata"""
        try:
            normalized = tweet.copy()
            
            # Normalize text
            if 'text' in normalized:
                normalized['text'] = self.normalize_text(normalized['text'])
                
            # Add processing metadata
            normalized['processed_at'] = datetime.now().isoformat()
            
            return normalized
            
        except Exception as e:
            log_error(logger, e, f"Failed to normalize tweet: {tweet.get('id', 'unknown')}")
            raise DataProcessingError(f"Tweet normalization failed: {str(e)}")
            
    def _process_raw_tweets(self, raw_columns):
        """Add metadata to processed data"""
        try:
            processed_data = {
                'metadata': {
                    'processed_at': datetime.now().isoformat(),
                    'columns_processed': len(raw_columns)
                },
                'columns': {},
                'total_tweets': 0
            }
            
            for column_id, tweets in raw_columns.items():
                logger.info(f"Processing column {column_id} with {len(tweets)} tweets")
                
                # Remove duplicates based on tweet ID
                seen_ids = set()
                unique_tweets = []
                for tweet in tweets:
                    if tweet.get('id') not in seen_ids:
                        seen_ids.add(tweet.get('id'))
                        unique_tweets.append(tweet)
                        
                logger.info(f"Removed {len(tweets) - len(unique_tweets)} duplicate tweets")
                
                # Filter and normalize remaining tweets
                normalized_tweets = []
                for tweet in unique_tweets:
                    if self.is_valid_tweet(tweet):
                        tweet['text'] = self.normalize_text(tweet['text'])
                        normalized_tweets.append(tweet)
                        
                processed_data['columns'][column_id] = normalized_tweets
                processed_data['total_tweets'] += len(normalized_tweets)
                
                logger.info(f"Column {column_id}: {len(tweets)} to {len(normalized_tweets)} tweets after processing")
                
            return processed_data
            
        except Exception as e:
            log_error(logger, e, "Failed to process raw tweets")
            raise DataProcessingError(f"Raw tweet processing failed: {str(e)}")
            
    def _is_valid_tweet(self, tweet):
        """Validate tweet with error handling"""
        try:
            if not tweet.get('text'):
                return False
                
            text = tweet['text'].strip()
            words = text.split()
            
            return len(words) >= 2
            
        except Exception as e:
            log_error(logger, e, f"Failed to validate tweet: {tweet.get('id', 'unknown')}")
            return False
            
    async def _save_processed_tweets(self, processed_data, date_str):
        """Save processed tweets into a single combined file since all columns are one category"""
        try:
            date_dir = self.processed_dir / date_str
            date_dir.mkdir(parents=True, exist_ok=True)
            
            # Combine all tweets from all columns into a single list
            all_tweets = []
            for col_id, tweets in processed_data['columns'].items():
                all_tweets.extend(tweets)
            
            # Create combined data structure
            combined_data = {
                'metadata': {
                    'processed_at': processed_data['metadata']['processed_at'],
                    'total_tweets': len(all_tweets)
                },
                'tweets': all_tweets
            }
            
            # Save to a single file
            output_file = date_dir / 'combined_tweets.json'
            with open(output_file, 'w') as f:
                json.dump(combined_data, f, indent=2)
            
            logger.info(f"Saved {len(all_tweets)} tweets to {output_file}")
            
        except Exception as e:
            log_error(logger, e, f"Failed to save processed tweets for date {date_str}")
            raise DataProcessingError(f"Failed to save processed tweets: {str(e)}")

    def process_columns(self, raw_columns):
        """Core processing pipeline"""
        processed = {}
        
        # Step 3.1: Deduplication
        deduped = self.deduplicate(raw_columns)
        
        # Step 3.2-3.4: Text cleaning
        cleaned = self.clean_tweets(deduped)
        
        # Step 3.5-3.6: Structured output
        structured = self.structure_output(cleaned)
        
        return structured

    def clean_tweets(self, columns):
        """Steps 3.2-3.4: Text validation and normalization"""
        valid_tweets = {}
        initial_total = sum(len(t) for t in columns.values())
        
        for col_id, tweets in columns.items():
            cleaned = []
            for t in tweets:
                if self._is_valid(t) and self._normalize(t):
                    cleaned.append(t)
            valid_tweets[col_id] = cleaned
            logger.info(f"Column {col_id}: Cleaned {len(tweets)}→{len(cleaned)}")
            
        cleaned_total = sum(len(t) for t in valid_tweets.values())
        logger.info(f"Cleaning: {initial_total} → {cleaned_total} tweets (-{initial_total - cleaned_total})")
        return valid_tweets

    def _is_valid(self, tweet):
        """Check minimum text requirements"""
        text = tweet.get('text', '')
        return len(text.split()) >= self.min_words

    def _normalize(self, tweet):
        """Special character normalization"""
        text = tweet['text']
        # Add normalization logic from existing implementation
        tweet['text'] = self.normalize_text(text)  
        return True

    def structure_output(self, columns):
        """Steps 3.5-3.6: Create unified JSON structure with total count"""
        total = sum(len(tweets) for tweets in columns.values())
        return {
            'metadata': {
                'processed_at': datetime.now().isoformat(),
                'columns_processed': len(columns),
                'total_tweets': total
            },
            'columns': columns
        }

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    processor = DataProcessor()
    
    # Get date from command line argument
    import sys
    date_to_process = sys.argv[1] if len(sys.argv) > 1 else None
    
    # Run async processing
    asyncio.run(processor.process_tweets(date_to_process)) 