"""Alpha filtering and relevance analysis service"""

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

class AlphaFilter:
    def __init__(self, config):
        self.config = config
        self.data_dir = Path('data')
        self.processed_dir = self.data_dir / 'processed'
        self.filtered_dir = self.data_dir / 'filtered' / 'alpha_filtered'
        self.is_shutting_down = False
        
        # Initialize both API clients
        self.deepseek_api_key = config['deepseek_api_key']
        self.openai_api_key = config['openai_api_key']
        
        self.deepseek_client = AsyncOpenAI(
            api_key=self.deepseek_api_key,
            base_url="https://api.deepseek.com"
        )
        self.openai_client = AsyncOpenAI(api_key=self.openai_api_key)
        
        # Use centralized category mapping
        self.categories = CATEGORY_MAP
        
        # Alpha filtering thresholds
        self.alpha_threshold = config.get('alpha_threshold', 0.8)
        self.risk_threshold = config.get('risk_threshold', 0.4)
        
        self.circuit_breaker = CircuitBreaker()
        
    async def _try_deepseek_request(self, prompt):
        """Attempt to get a response from Deepseek"""
        try:
            # Add 3 second timeout for Deepseek
            response = await asyncio.wait_for(
                self.deepseek_client.chat.completions.create(
                    model="deepseek-chat",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.1,
                    response_format={"type": "json_object"},
                    max_tokens=500
                ),
                timeout=3  # 3 second timeout
            )
            
            if not response.choices:
                logger.warning("Deepseek response contains no choices")
                return None
                
            return response.choices[0].message.content
            
        except asyncio.TimeoutError:
            return None
        except Exception as e:
            logger.warning(f"Deepseek request failed: {str(e)}")
            return None
            
    async def _try_openai_request(self, prompt):
        """Attempt to get a response from OpenAI"""
        try:
            # Add 10 second timeout for OpenAI
            response = await asyncio.wait_for(
                self.openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.1,
                    response_format={"type": "json_object"},
                    max_tokens=500
                ),
                timeout=10  # 10 second timeout
            )
            
            return response.choices[0].message.content
            
        except asyncio.TimeoutError:
            logger.warning("OpenAI request timed out after 10 seconds")
            return None
        except Exception as e:
            logger.warning(f"OpenAI request failed: {str(e)}")
            return None
        
    async def filter_content(self, content, category):
        """Filter and score content for alpha signals in its category"""
        try:
            await self.circuit_breaker.check()
            
            # Prepare prompt with safe content access
            prompt = self._prepare_filtering_prompt(content, category)
            if prompt is None:
                logger.warning(f"Skipping content {content.get('id', 'unknown')} due to invalid content structure")
                return None
            
            # Retry configuration
            max_retries = 3
            base_delay = 2  # Start with 2 second delay
            
            for attempt in range(max_retries):
                try:
                    # First try Deepseek
                    response_text = await self._try_deepseek_request(prompt)
                    
                    # If Deepseek fails, try OpenAI as fallback
                    if response_text is None:
                        response_text = await self._try_openai_request(prompt)
                        
                    # If both failed, raise exception
                    if response_text is None:
                        raise Exception("Both Deepseek and OpenAI requests failed")
                    
                    # Validate JSON completeness
                    if not response_text.strip().endswith('}'):
                        raise ValueError("Truncated JSON response")
                    
                    result = self._validate_filter_response(response_text)
                    
                    # If content didn't meet criteria, return None
                    if result is None:
                        logger.debug(f"Content {content.get('id', 'unknown')} did not meet criteria")
                        return None
                    
                    # Add metadata
                    result['content_id'] = content.get('id', 'unknown')
                    result['category'] = category
                    
                    # Log success
                    logger.debug(f"Successfully filtered content {content.get('id', 'unknown')}")
                    return result
                        
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout on attempt {attempt + 1} for content {content.get('id', 'unknown')}")
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON response on attempt {attempt + 1} for content {content.get('id', 'unknown')}: {str(e)}")
                except Exception as e:
                    logger.warning(f"Error on attempt {attempt + 1} for content {content.get('id', 'unknown')}: {str(e)}")
                
                # Exponential backoff if not last attempt
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)  # 2s, 4s, 8s
                    logger.info(f"Retrying in {delay} seconds...")
                    await asyncio.sleep(delay)
            
            logger.error(f"Failed to filter content {content.get('id', 'unknown')} after {max_retries} attempts with both models")
            return None
            
        except Exception as e:
            if "Circuit breaker open" in str(e):
                logger.warning(f"Circuit breaker open for content {content.get('id', 'unknown')} - skipping")
                return None
            else:
                self.circuit_breaker.record_failure()
                logger.error(f"Unexpected error filtering content {content.get('id', 'unknown')}: {str(e)}")
                return None
            
    async def process_content(self, date_str=None):
        """Process all content for a given date"""
        try:
            if not date_str:
                current_time = datetime.now(zoneinfo.ZoneInfo("UTC"))
                yesterday = current_time - timedelta(days=1)
                date_str = yesterday.strftime('%Y%m%d')
                
            logger.info(f"Processing content for date: {date_str}")
            
            # Load from date-specific directory
            date_dir = self.processed_dir / date_str
            if not date_dir.exists():
                logger.error(f"Processed directory not found: {date_dir}")
                return
            
            # Create filtered output directory
            filtered_date_dir = self.filtered_dir / date_str
            filtered_date_dir.mkdir(parents=True, exist_ok=True)
            
            # Check summary file first
            summary_file = filtered_date_dir / 'summary.json'
            completed_columns = set()
            if summary_file.exists():
                try:
                    with open(summary_file, 'r') as f:
                        summary_data = json.load(f)
                        for col_id, stats in summary_data['metadata']['tweets_per_column'].items():
                            if stats['chunks_processed'] == stats['total_chunks']:
                                completed_columns.add(col_id)
                                logger.info(f"Column {col_id} already fully processed ({stats['chunks_processed']}/{stats['total_chunks']} chunks)")
                except Exception as e:
                    logger.warning(f"Error reading summary file: {str(e)}")
            
            # Load all column files
            column_files = list(date_dir.glob('column_*.json'))
            if not column_files:
                logger.error(f"No column files found in {date_dir}")
                return
            
            data = {
                'date': date_str,
                'columns': {},
                'metadata': {
                    'total_items': 0,
                    'columns_processed': 0,
                    'processing_start': datetime.now(zoneinfo.ZoneInfo("UTC")).isoformat()
                }
            }
            
            # Process and collect all content
            all_scores = []
            
            for col_file in column_files:
                try:
                    column_id = col_file.stem.split('_')[1]
                    
                    # Skip if column is already completed
                    if column_id in completed_columns:
                        logger.info(f"Skipping column {column_id} - already completed")
                        continue
                    
                    with open(col_file, 'r') as f:
                        col_data = json.load(f)
                        content_items = col_data.get('tweets', [])
                        
                        data['columns'][column_id] = content_items
                        data['metadata']['total_items'] += len(content_items)
                        data['metadata']['columns_processed'] += 1
                        
                        logger.info(f"Loaded {len(content_items)} items from column {column_id}")
                        
                except Exception as e:
                    logger.error(f"Failed to load {col_file.name}: {str(e)}")
                    continue
                
            if data['metadata']['total_items'] == 0:
                logger.warning("No content found to process")
                return
            
            logger.info(f"Processing {data['metadata']['total_items']} items from {len(data['columns'])} columns")
            
            # Process one column at a time
            for col_id in data['columns'].keys():
                logger.info(f"Processing column {col_id}")
                
                try:
                    # Process single column without timeout
                    results = await self._process_column(
                        data['columns'][col_id],
                        self.categories.get(col_id),
                        col_id,
                        date_str
                    )
                    
                    # Add results to total
                    if results:
                        all_scores.extend(results)
                        logger.info(f"Column {col_id} completed with {len(results)} tweets")
                    
                except Exception as e:
                    logger.error(f"Error processing column {col_id}: {str(e)}")
                
                # Delay between columns
                await asyncio.sleep(5)
            
            # Save summary metadata
            summary_data = {
                'date': date_str,
                'metadata': {
                    'total_processed': data['metadata']['total_items'],
                    'processing_start': data['metadata']['processing_start'],
                    'processing_end': datetime.now(zoneinfo.ZoneInfo("UTC")).isoformat(),
                    'alpha_threshold': self.alpha_threshold,
                    'risk_threshold': self.risk_threshold,
                    'columns_processed': len(data['columns']),
                    'total_tweets_found': len(all_scores),
                    'tweets_per_column': {}
                }
            }
            
            # Add per-column statistics
            for col_id in data['columns'].keys():
                try:
                    col_file = filtered_date_dir / f'column_{col_id}.json'
                    if col_file.exists():
                        with open(col_file, 'r') as f:
                            col_data = json.load(f)
                            summary_data['metadata']['tweets_per_column'][col_id] = {
                                'total_chunks': col_data['metadata']['total_chunks'],
                                'chunks_processed': col_data['metadata']['chunks_processed'],
                                'tweets_found': len(col_data['tweets'])
                            }
                except Exception as e:
                    logger.error(f"Error reading column {col_id} stats: {str(e)}")
            
            with open(filtered_date_dir / 'summary.json', 'w') as f:
                json.dump(summary_data, f, indent=2)
                
            logger.info(f"Alpha filtering complete. Found {len(all_scores)} tweets across {len(data['columns'])} columns.")
            logger.info(f"Results saved to {filtered_date_dir}")
            
            return summary_data
            
        except Exception as e:
            logger.error(f"Error processing content: {str(e)}")
            return None
            
    def _prepare_filtering_prompt(self, content, category):
        """Prepare the prompt for filtering content"""
        # Safely get content fields with defaults
        try:
            tweet_text = content.get('text', '')
            author = content.get('authorHandle', '')
            quoted_text = content.get('quotedContent', {}).get('text', '') if content.get('quotedContent') else ''
            reposted_text = content.get('repostedContent', {}).get('text', '') if content.get('repostedContent') else ''
            
            if not tweet_text or not author:
                logger.warning(f"Missing required content fields for ID {content.get('id', 'unknown')}")
                return None
                
            return f"""
            Please analyze this content for HIGH-QUALITY ALPHA SIGNALS in the {category} category.
            If the content meets the criteria, return ONLY the original content in JSON format.
            If it doesn't meet the criteria, return an empty JSON object {{}}.
    
            Content Details:
            Text: {tweet_text}
            Author: {author}
            {f"Quoted content: {quoted_text}" if quoted_text else ""}
            {f"Reposted content: {reposted_text}" if reposted_text else ""}
    
            Scoring criteria (DO NOT include these in output):
            1. Relevance (0-1): Does the content contain ACTIONABLE alpha?
               - Must score 0.8+ to be included
               - Concrete, time-sensitive alpha with clear action points
               - Direct mention of ecosystem name + significant update
    
            2. Significance (0-1): How important is this for {category}?
               - Must score 0.7+ to be included
               - Must impact ecosystem value or token price
               - Consider timing and exclusivity of information
    
            3. Impact (0-1): What measurable effects will this have?
               - Must score 0.7+ to be included
               - Focus on quantifiable metrics (TVL, price, volume)
               - Higher score for immediate impact opportunities
    
            4. Ecosystem relevance (0-1): How does this contribute to market dynamics?
               - Must score 0.7+ to be included
               - Consider market positioning and competitive advantage
               - Score based on ecosystem value creation
    
            If ALL criteria are met, return ONLY this JSON structure:
            {{
                "tweet": "{tweet_text}",
                "author": "{author}",
                "quoted_content": "{quoted_text}",
                "reposted_content": "{reposted_text}"
            }}
    
            If ANY criteria is not met, return: {{}}
            """
        except Exception as e:
            logger.error(f"Error preparing prompt for content {content.get('id', 'unknown')}: {str(e)}")
            return None
            
    def _validate_filter_response(self, response_text: str) -> dict:
        """Validate the filter response"""
        try:
            result = json.loads(response_text)
            
            # If empty result, content didn't meet criteria
            if not result:
                return None
            
            # Check for required content fields
            required_fields = ['tweet', 'author']
            if not all(key in result for key in required_fields):
                raise ValueError(f"Missing required fields: {[k for k in required_fields if k not in result]}")
            
            return result
            
        except json.JSONDecodeError as e:
            logger.warning(f"JSON decode error: {str(e)}")
            raise
        except Exception as e:
            logger.warning(f"Invalid response format: {str(e)}")
            raise
            
    async def _process_column(self, content_items, category, col_id, date_str):
        """Process a single column's content"""
        try:
            logger.info(f"Starting column {col_id} ({len(content_items)} items)")
            results = []
            
            # Setup output files
            output_dir = self.filtered_dir / date_str
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / f'column_{col_id}.json'
            summary_file = output_dir / 'summary.json'
            
            # Calculate total chunks
            chunk_size = 9  # Process 9 tweets at a time
            total_chunks = (len(content_items) + chunk_size - 1) // chunk_size
            
            # Load or initialize summary data
            summary_data = {}
            if summary_file.exists():
                try:
                    with open(summary_file, 'r') as f:
                        summary_data = json.load(f)
                except Exception as e:
                    logger.warning(f"Error reading summary file: {str(e)}")
            
            if not summary_data:
                summary_data = {
                    'date': date_str,
                    'metadata': {
                        'total_processed': 0,
                        'processing_start': datetime.now(zoneinfo.ZoneInfo("UTC")).isoformat(),
                        'alpha_threshold': self.alpha_threshold,
                        'risk_threshold': self.risk_threshold,
                        'columns_processed': 0,
                        'total_tweets_found': 0,
                        'tweets_per_column': {}
                    }
                }
            
            # Check summary file first for accurate progress
            chunks_processed = 0
            col_stats = summary_data.get('metadata', {}).get('tweets_per_column', {}).get(col_id, {})
            if col_stats:
                stored_total = col_stats.get('total_chunks', 0)
                if stored_total == total_chunks:  # Only use if chunk count matches
                    chunks_processed = col_stats.get('chunks_processed', 0)
                    logger.info(f"Found progress in summary: chunk {chunks_processed}/{total_chunks}")
            
            # Initialize or load existing file
            if output_file.exists():
                with open(output_file, 'r') as f:
                    file_data = json.load(f)
                    results = file_data.get('tweets', [])
                    
                    # Only consider complete if we processed ALL chunks
                    if chunks_processed >= total_chunks and total_chunks > 0:
                        logger.info(f"Column {col_id} already fully processed ({chunks_processed}/{total_chunks} chunks)")
                        return results
                    else:
                        logger.info(f"Resuming column {col_id} from chunk {chunks_processed + 1}/{total_chunks}")
            else:
                file_data = {
                    'date': date_str,
                    'column_id': col_id,
                    'category': category,
                    'tweets': results,
                    'metadata': {
                        'chunks_processed': chunks_processed,
                        'total_chunks': total_chunks,
                        'last_update': datetime.now(zoneinfo.ZoneInfo("UTC")).isoformat()
                    }
                }
            
            # Process remaining chunks
            for i in range(0, len(content_items), chunk_size):
                if self.is_shutting_down:
                    logger.info("Graceful shutdown requested, saving progress...")
                    break

                chunk = content_items[i:i+chunk_size]
                chunk_number = i // chunk_size + 1
                
                # Skip already processed chunks
                if chunk_number <= chunks_processed:
                    logger.debug(f"Skipping already processed chunk {chunk_number}/{total_chunks}")
                    continue
                
                logger.info(f"Processing chunk {chunk_number}/{total_chunks} in column {col_id}")
                
                # Process chunk in parallel
                chunk_tasks = [self.filter_content(item, category) for item in chunk]
                chunk_results = await asyncio.gather(*chunk_tasks)
                
                # Add successful results
                new_results = [r for r in chunk_results if r is not None]
                if new_results:
                    results.extend(new_results)
                    logger.info(f"Found {len(new_results)} new tweets in chunk {chunk_number}")
                
                # Update file with new results and chunk progress
                chunks_processed = chunk_number
                file_data.update({
                    'tweets': results,
                    'metadata': {
                        'chunks_processed': chunks_processed,
                        'total_chunks': total_chunks,
                        'last_update': datetime.now(zoneinfo.ZoneInfo("UTC")).isoformat(),
                        'tweet_count': len(results)
                    }
                })
                
                # Save updated results to column file
                with open(output_file, 'w') as f:
                    json.dump(file_data, f, indent=2)
                
                # Update and save summary.json
                summary_data['metadata']['tweets_per_column'][col_id] = {
                    'total_chunks': total_chunks,
                    'chunks_processed': chunks_processed,
                    'tweets_found': len(results)
                }
                summary_data['metadata']['total_tweets_found'] = sum(
                    stats.get('tweets_found', 0) 
                    for stats in summary_data['metadata']['tweets_per_column'].values()
                )
                summary_data['metadata']['processing_end'] = datetime.now(zoneinfo.ZoneInfo("UTC")).isoformat()
                
                with open(summary_file, 'w') as f:
                    json.dump(summary_data, f, indent=2)
                
                logger.info(f"Saved progress: chunk {chunk_number}/{total_chunks}, total tweets: {len(results)}")
                
                # Delay between chunks
                if i + chunk_size < len(content_items):
                    await asyncio.sleep(2)
            
            logger.info(f"Completed column {col_id} with {len(results)} total tweets")
            return results
            
        except Exception as e:
            logger.error(f"Failed processing column {col_id}: {str(e)}")
            return []

    async def cleanup(self):
        """Cleanup and save state before shutdown"""
        self.is_shutting_down = True
        logger.info("Cleaning up and saving state...")
        # Any additional cleanup can be added here

if __name__ == "__main__":
    import signal
    import sys
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Get date to process - either from args or use today's date
    date_to_process = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y%m%d')
    
    # Load config
    import os
    from dotenv import load_dotenv
    load_dotenv()
    
    config = {
        'deepseek_api_key': os.getenv('DEEPSEEK_API_KEY'),
        'openai_api_key': os.getenv('OPENAI_API_KEY'),
        'alpha_threshold': float(os.getenv('ALPHA_THRESHOLD', '0.8')),
        'risk_threshold': float(os.getenv('RISK_THRESHOLD', '0.4'))
    }
    
    alpha_filter = AlphaFilter(config)
    
    # Setup signal handlers
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, initiating graceful shutdown...")
        asyncio.create_task(alpha_filter.cleanup())
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        asyncio.run(alpha_filter.process_content(date_to_process))
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received, waiting for cleanup...")
        # Let the cleanup finish
        asyncio.run(alpha_filter.cleanup())
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1) 