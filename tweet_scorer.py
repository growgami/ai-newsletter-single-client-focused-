"""Tweet scoring and relevance analysis service"""

import logging
import json
from pathlib import Path
from datetime import datetime, timedelta
import zoneinfo
import asyncio
from openai import OpenAI, AsyncOpenAI
from error_handler import with_retry, APIError, log_error, RetryConfig
from category_mapping import CATEGORY_MAP

logger = logging.getLogger(__name__)

# Reduce httpx logging
logging.getLogger('httpx').setLevel(logging.WARNING)

class CircuitBreaker:
    def __init__(self, max_failures=5, reset_timeout=60):
        self.max_failures = max_failures
        self.reset_timeout = reset_timeout
        self.failure_count = 0
        self.last_failure_time = None
        
    async def check(self):
        if self.failure_count >= self.max_failures:
            if (datetime.now() - self.last_failure_time).seconds < self.reset_timeout:
                raise Exception("Circuit breaker open")
            else:
                self.reset()
                
    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        
    def reset(self):
        self.failure_count = 0
        self.last_failure_time = None

class TweetScorer:
    def __init__(self, config):
        self.config = config
        self.data_dir = Path('data')
        self.processed_dir = self.data_dir / 'processed'
        self.api_key = config['deepseek_api_key']
        self.client = AsyncOpenAI(api_key=self.api_key, base_url="https://api.deepseek.com/v1")
        
        # Use centralized category mapping
        self.categories = CATEGORY_MAP
        
        self.circuit_breaker = CircuitBreaker()
        
    async def score_tweet(self, tweet, category):
        """Score a tweet for relevance to its category"""
        try:
            await self.circuit_breaker.check()
            # Prepare prompt for scoring
            prompt = self._prepare_scoring_prompt(tweet, category)
            
            # Retry configuration
            max_retries = 3
            base_delay = 2  # Start with 2 second delay
            
            for attempt in range(max_retries):
                try:
                    response = await self.client.chat.completions.create(
                        model="deepseek-chat",
                        messages=[{"role": "user", "content": prompt}],
                        temperature=0.1,
                        response_format={"type": "json_object"},
                        max_tokens=500
                    )
                    
                    response_text = response.choices[0].message.content
                    
                    # Validate JSON completeness
                    if not response_text.strip().endswith('}'):
                        raise ValueError("Truncated JSON response")
                    
                    result = self._validate_score_response(response_text)
                    
                    # Calculate average score if not provided
                    if 'average_score' not in result:
                        scores = [
                            result.get('relevance', 0),
                            result.get('significance', 0),
                            result.get('impact', 0),
                            result.get('ecosystem_relevance', 0)
                        ]
                        result['average_score'] = sum(scores) / len(scores)
                    
                    # Add metadata
                    result['tweet_id'] = tweet['id']
                    result['category'] = category
                    
                    # Log success
                    logger.debug(f"Successfully scored tweet {tweet['id']} (avg: {result['average_score']:.2f})")
                    return result
                        
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout on attempt {attempt + 1} for tweet {tweet['id']}")
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON response on attempt {attempt + 1} for tweet {tweet['id']}: {str(e)}")
                except Exception as e:
                    logger.warning(f"Error on attempt {attempt + 1} for tweet {tweet['id']}: {str(e)}")
                
                # Exponential backoff if not last attempt
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)  # 2s, 4s, 8s
                    logger.info(f"Retrying in {delay} seconds...")
                    await asyncio.sleep(delay)
            
            logger.error(f"Failed to score tweet {tweet['id']} after {max_retries} attempts")
            return None
            
        except Exception as e:
            self.circuit_breaker.record_failure()
            logger.error(f"Unexpected error scoring tweet {tweet['id']}: {str(e)}")
            return None
            
    async def process_tweets(self, date_str=None):
        """Process all tweets for a given date"""
        try:
            if not date_str:
                current_time = datetime.now(zoneinfo.ZoneInfo("UTC"))
                yesterday = current_time - timedelta(days=1)
                date_str = yesterday.strftime('%Y%m%d')
                
            logger.info(f"Processing tweets for date: {date_str}")
            
            # Load from date-specific directory
            date_dir = self.processed_dir / date_str
            if not date_dir.exists():
                logger.error(f"Processed directory not found: {date_dir}")
                return
            
            # Load all column files
            column_files = list(date_dir.glob('column_*.json'))
            if not column_files:
                logger.error(f"No column files found in {date_dir}")
                return
            
            data = {
                'date': date_str,
                'columns': {},
                'metadata': {
                    'total_tweets': 0,
                    'columns_processed': 0
                }
            }
            
            for col_file in column_files:
                try:
                    with open(col_file, 'r') as f:
                        col_data = json.load(f)
                        column_id = col_file.stem.split('_')[1]
                        tweets = col_data.get('tweets', [])
                        
                        data['columns'][column_id] = tweets
                        data['metadata']['total_tweets'] += len(tweets)
                        data['metadata']['columns_processed'] += 1
                        
                        logger.info(f"Loaded {len(tweets)} tweets from column {column_id}")
                        
                except Exception as e:
                    logger.error(f"Failed to load {col_file.name}: {str(e)}")
                    continue
                
            if data['metadata']['total_tweets'] == 0:
                logger.warning("No tweets found to process")
                return
            
            logger.info(f"Processing {data['metadata']['total_tweets']} tweets from {len(data['columns'])} columns")
            
            # Process columns in batches of 3
            column_ids = list(data['columns'].keys())
            batch_size = 3
            
            for i in range(0, len(column_ids), batch_size):
                batch_columns = column_ids[i:i+batch_size]
                logger.info(f"Processing batch {i//batch_size + 1}: Columns {batch_columns}")
                
                # Create tasks for all columns in batch
                batch_tasks = []
                for col_id in batch_columns:
                    batch_tasks.append(
                        self._process_column(
                            data['columns'][col_id],
                            self.categories.get(col_id),
                            col_id
                        )
                    )
                
                # Run batch processing with timeout
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*batch_tasks),
                        timeout=300  # 5 minutes per batch
                    )
                    logger.info(f"Completed batch {i//batch_size + 1}")
                    
                except asyncio.TimeoutError:
                    logger.error(f"Timeout processing batch {i//batch_size + 1}")
                    
                # Delay between batches
                await asyncio.sleep(5)
            
            # Save filtered data
            with open(date_dir / f'processed_tweets_{date_str}.json', 'w') as f:
                json.dump(data, f, indent=2)
                
            logger.info(f"Saved {data['metadata']['total_tweets']} high-scoring tweets to {date_dir / f'processed_tweets_{date_str}.json'}")
            
            # Replace existing filtering with:
            filtered_data = {'columns': {}}
            all_scores = []
            
            # Collect results from completed batches
            for task in asyncio.all_tasks():
                if task.done() and not task.exception():
                    all_scores.extend(task.result())
            
            # Apply filtering using all_scores
            # ... rest of filtering logic ...
            
        except Exception as e:
            logger.error(f"Error processing tweets: {str(e)}")
            
    def _prepare_scoring_prompt(self, tweet, category):
        """Prepare the prompt for scoring a tweet"""
        return f"""
        Please analyze this tweet's importance specifically for the {category} category and provide scores in JSON format.

        STRICT SCORING RULES:
        1. Tweet MUST explicitly mention the ecosystem name or its official projects/protocols
        2. Generic blockchain/Web3 mentions are NOT sufficient
        3. Do not make assumptions about project affiliations
        4. Require clear evidence of ecosystem connection
        5. When in doubt, score lower

        Tweet Content:
        Text: {tweet['text']}
        Author: {tweet['authorHandle']}
        {"Quoted content: " + tweet['quotedContent']['text'] if tweet.get('quotedContent') else ""}
        {"Reposted content: " + tweet['repostedContent']['text'] if tweet.get('repostedContent') else ""}

        Scoring criteria:
        1. Relevance (0-1): Does the tweet EXPLICITLY mention {category} or its verified projects? Score 0 if no direct mention.
           - Score 0.9-1.0: Direct mention of ecosystem name + significant update
           - Score 0.7-0.8: Direct mention of verified ecosystem project + update
           - Score 0.0-0.3: Generic blockchain/Web3 content or unverified projects
           - Score 0: No explicit mention of ecosystem or verified projects

        2. Significance (0-1): How important is this verified update for {category}?
           - Must be about confirmed ecosystem projects
           - Score based on concrete impact, not potential
           - Lower score if relationship is unclear

        3. Impact (0-1): What measurable effects will this have on {category}?
           - Require specific metrics or clear outcomes
           - Must directly relate to ecosystem growth
           - Lower score for indirect or assumed benefits

        4. Ecosystem relevance (0-1): How does this contribute to {category}'s development?
           - Must demonstrate clear ecosystem connection
           - Score 0 if relationship is assumed
           - Higher scores only for official integrations/partnerships

        Your reasoning must:
        1. Identify the EXPLICIT mention of ecosystem or verified project
        2. Explain why you're confident about ecosystem connection
        3. Point out any assumptions you made (and lower score accordingly)
        4. Be skeptical of unverified relationships

        EXAMPLE JSON OUTPUT:
        {{
            "relevance": 0.8,
            "significance": 0.7,
            "impact": 0.9,
            "ecosystem_relevance": 0.85,
            "average_score": 0.81,
            "reasoning": "This tweet EXPLICITLY mentions {category} by [exact reference]. The ecosystem connection is verified through [specific evidence]. The impact is clear because [concrete metrics/outcomes]. Note: I assumed [any assumptions] and lowered the score accordingly."
        }}
        """
            
    def _validate_score_response(self, response_text: str) -> dict:
        try:
            result = json.loads(response_text)
            if not all(key in result for key in ['relevance', 'significance', 'impact', 'ecosystem_relevance']):
                raise ValueError("Missing required score fields")
            return result
        except json.JSONDecodeError as e:
            logger.warning(f"JSON decode error: {str(e)}")
            raise
        except Exception as e:
            logger.warning(f"Invalid score format: {str(e)}")
            raise
            
    async def _process_column(self, tweets, category, col_id):
        """Process a single column's tweets"""
        try:
            logger.info(f"Starting column {col_id} ({len(tweets)} tweets)")
            
            # Process tweets in chunks
            chunk_size = 5
            tasks = []
            
            for i in range(0, len(tweets), chunk_size):
                chunk = tweets[i:i+chunk_size]
                chunk_tasks = [self.score_tweet(t, category) for t in chunk]
                tasks.extend(chunk_tasks)
                
                # Delay between chunks
                if i + chunk_size < len(tweets):
                    await asyncio.sleep(2)
            
            # Process with concurrency control
            semaphore = asyncio.Semaphore(3)
            async def score_with_semaphore(task):
                async with semaphore:
                    return await task
                    
            return await asyncio.gather(*[score_with_semaphore(t) for t in tasks])
            
        except Exception as e:
            logger.error(f"Failed processing column {col_id}: {str(e)}")
            return []

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Get date to process - either from args or use today's date
    import sys
    date_to_process = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y%m%d')
    
    # Load config and run scorer
    import os
    from dotenv import load_dotenv
    load_dotenv()
    
    config = {
        'deepseek_api_key': os.getenv('DEEPSEEK_API_KEY')
    }
    
    scorer = TweetScorer(config)
    asyncio.run(scorer.process_tweets(date_to_process)) 