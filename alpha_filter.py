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
        self.processed_dir = self.data_dir / 'processed'  # Input from raw tweets
        self.filtered_dir = self.data_dir / 'filtered' / 'alpha_filtered'  # Output without date subfolder
        self.state_file = self.filtered_dir / 'state.json'  # Track processing state
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
        
        # Create output directory
        self.filtered_dir.mkdir(parents=True, exist_ok=True)

    def _load_state(self):
        """Load processing state from file"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading state file: {str(e)}")
        return {
            'last_processed_date': None,
            'columns_state': {}  # Track progress per column
        }

    def _clear_column_files(self):
        """Clear all column output files for fresh processing"""
        try:
            for file in self.filtered_dir.glob('column_*.json'):
                file.unlink()
            logger.info("Cleared existing column files for new date")
        except Exception as e:
            logger.error(f"Error clearing column files: {str(e)}")

    def _get_column_state(self, date_str, col_id):
        """Get processing state for a specific column"""
        state = self._load_state()
        return state.get('columns_state', {}).get(str(col_id), {
            'completed': False,
            'last_chunk': 0,
            'total_chunks': 0
        })

    def _update_column_state(self, date_str, col_id, chunk_number, total_chunks, completed=False):
        """Update processing state for a specific column"""
        state = self._load_state()
        
        # Initialize column state if needed while preserving existing states
        if 'columns_state' not in state:
            state['columns_state'] = {}
        
        # Update only the specific column while keeping others intact
        state['columns_state'][str(col_id)] = {
            'completed': completed,
            'last_chunk': chunk_number,
            'total_chunks': total_chunks
        }
        
        # Update date without affecting columns_state
        state['last_processed_date'] = date_str
        
        self._save_state(state)

    def _save_state(self, state):
        """Save processing state to file"""
        try:
            self._atomic_write_json(self.state_file, state)
        except Exception as e:
            logger.error(f"Error saving state file: {str(e)}")

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
        """Process content for a given date"""
        try:
            if not date_str:
                current_time = datetime.now(zoneinfo.ZoneInfo("UTC"))
                yesterday = current_time - timedelta(days=1)
                date_str = yesterday.strftime('%Y%m%d')
                
            logger.info(f"Processing content for date: {date_str}")
            
            # Load state and check date
            state = self._load_state()
            last_date = state.get('last_processed_date')
            
            # Check if same date
            if last_date == date_str:
            # Check if all chunks done - but only if we have columns to process
                columns_state = state.get('columns_state', {})
                if not columns_state:
                    logger.info(f"No columns processed yet for {date_str}, starting processing")
                else:
                    all_complete = all(
                        col_state.get('completed', False) 
                        for col_state in columns_state.values()
                )
                if not all_complete:
                    # Not done, continue processing
                    logger.info(f"Incomplete chunks found for {date_str}, continuing processing")
                else:
                    # Done, skip
                    logger.info(f"All chunks completed for {date_str}, skipping")
                    return state
            else:
                # Not same date
                logger.info(f"New date detected (last: {last_date}, current: {date_str})")
                # Clear columns_state and update date
                state['columns_state'] = {}
                state['last_processed_date'] = date_str
                self._save_state(state)
                logger.info("Starting fresh processing for new date")
            
            # Load from date-specific input directory
            date_dir = self.processed_dir / date_str
            if not date_dir.exists():
                logger.error(f"Processed directory not found: {date_dir}")
                return state
            
            # Load all column files
            column_files = list(date_dir.glob('column_*.json'))
            if not column_files:
                logger.error(f"No column files found in {date_dir}")
                return state
            
            # Initialize processing data
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
                    
                    with open(col_file, 'r') as f:
                        col_data = json.load(f)
                        content_items = col_data.get('tweets', [])
                        
                        if content_items:
                            data['columns'][column_id] = content_items
                            data['metadata']['total_items'] += len(content_items)
                            data['metadata']['columns_processed'] += 1
                            logger.info(f"Loaded {len(content_items)} items from column {column_id}")
                        
                except Exception as e:
                    logger.error(f"Failed to load {col_file.name}: {str(e)}")
                    continue
            
            if data['metadata']['total_items'] == 0:
                logger.warning("No content found to process")
                return state
            
            logger.info(f"Processing {data['metadata']['total_items']} items from {len(data['columns'])} columns")
            
            # Process one column at a time
            for col_id in data['columns'].keys():
                logger.info(f"Processing column {col_id}")
                
                try:
                    # Process single column
                    results = await self._process_column(
                        data['columns'][col_id],
                        self.categories.get(col_id),
                        col_id,
                        date_str,
                        data
                    )
                    
                    # Add results to total
                    if results:
                        all_scores.extend(results)
                        logger.info(f"Column {col_id} completed with {len(results)} tweets")
                    
                except Exception as e:
                    logger.error(f"Error processing column {col_id}: {str(e)}")
                
                # Delay between columns
                await asyncio.sleep(5)
            
            logger.info(f"Alpha filtering complete. Found {len(all_scores)} tweets across {len(data['columns'])} columns")
            return state
            
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
            url = content.get('url', '')  # Get URL from content
            
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
            URL: {url}
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
                "url": "{url}",
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
            required_fields = ['tweet', 'author', 'url']  # Added url to required fields
            if not all(key in result for key in required_fields):
                raise ValueError(f"Missing required fields: {[k for k in required_fields if k not in result]}")
            
            return result
            
        except json.JSONDecodeError as e:
            logger.warning(f"JSON decode error: {str(e)}")
            raise
        except Exception as e:
            logger.warning(f"Invalid response format: {str(e)}")
            raise
            
    def _atomic_write_json(self, file_path: Path, data: dict):
        """Atomically write JSON data to file"""
        temp_file = file_path.with_suffix('.tmp')
        try:
            with open(temp_file, 'w') as f:
                json.dump(data, f, indent=2)
            temp_file.replace(file_path)  # Atomic on POSIX
        except Exception as e:
            logger.error(f"Error writing file {file_path}: {str(e)}")
            if temp_file.exists():
                temp_file.unlink()
            raise

    async def _process_column(self, content_items, category, col_id, date_str, data):
        """Process a single column's content"""
        try:
            logger.info(f"Starting column {col_id} ({len(content_items)} items)")
            results = []
            
            # Setup output file (no date subfolder)
            output_file = self.filtered_dir / f'column_{col_id}.json'
            
            # Calculate total chunks
            chunk_size = 9  # Process 9 tweets at a time
            total_chunks = (len(content_items) + chunk_size - 1) // chunk_size
            
            # Load existing tweets if any
            existing_tweets = []
            if output_file.exists():
                try:
                    with open(output_file, 'r') as f:
                        file_data = json.load(f)
                        existing_tweets = file_data.get('tweets', [])
                        logger.info(f"Loaded {len(existing_tweets)} existing tweets from column {col_id}")
                except Exception as e:
                    logger.error(f"Error loading existing tweets: {str(e)}")
            
            # Get column state
            col_state = self._get_column_state(date_str, col_id)
            start_chunk = col_state['last_chunk']
            
            # Process chunks
            for i in range(start_chunk * chunk_size, len(content_items), chunk_size):
                if self.is_shutting_down:
                    logger.info("Graceful shutdown requested, saving progress...")
                    self._update_column_state(date_str, col_id, i // chunk_size, total_chunks)
                    break

                chunk = content_items[i:i+chunk_size]
                chunk_number = i // chunk_size + 1
                
                logger.info(f"Processing chunk {chunk_number}/{total_chunks} in column {col_id}")
                
                # Process chunk in parallel
                chunk_tasks = [self.filter_content(item, category) for item in chunk]
                chunk_results = await asyncio.gather(*chunk_tasks)
                
                # Add successful results
                new_results = [r for r in chunk_results if r is not None]
                if new_results:
                    results.extend(new_results)
                    logger.info(f"Found {len(new_results)} new tweets in chunk {chunk_number}")
                
                # Update output file with accumulated results
                file_data = {
                    'tweets': existing_tweets + results,  # Combine existing and new tweets
                    'metadata': {
                        'total_tweets': len(existing_tweets) + len(results),
                        'last_update': datetime.now(zoneinfo.ZoneInfo("UTC")).isoformat(),
                        'last_processed_date': date_str,
                        'processing_dates': data.get('processing_dates', [date_str])
                    }
                }
                self._atomic_write_json(output_file, file_data)
                
                # Update state after each chunk
                self._update_column_state(date_str, col_id, chunk_number, total_chunks)
                
                # Rate limiting between chunks
                if i + chunk_size < len(content_items):
                    await asyncio.sleep(2)
            
            # Mark column as completed if not shutdown
            if not self.is_shutting_down:
                self._update_column_state(date_str, col_id, total_chunks, total_chunks, completed=True)
                logger.info(f"Completed column {col_id} with {len(results)} new tweets")
            
            return results
            
        except Exception as e:
            logger.error(f"Failed processing column {col_id}: {str(e)}")
            return []

    async def cleanup(self):
        """Cleanup and save state before shutdown"""
        self.is_shutting_down = True
        logger.info("Cleaning up and saving state...")
        # Any additional cleanup can be added here

    def _get_unprocessed_dates(self):
        """Get list of dates with unprocessed raw tweets"""
        processed_dates = set(self._load_state().get('processing_dates', []))
        raw_dates = set()
        
        # Find all date folders in processed directory
        for date_dir in self.processed_dir.glob('*'):
            if date_dir.is_dir() and date_dir.name.isdigit() and len(date_dir.name) == 8:
                raw_dates.add(date_dir.name)
        
        # Return dates that haven't been fully processed
        return sorted(list(raw_dates - processed_dates))

    async def process_all_dates(self):
        """Process all unprocessed dates"""
        try:
            unprocessed_dates = self._get_unprocessed_dates()
            if not unprocessed_dates:
                logger.info("No new dates to process")
                return
            
            logger.info(f"Found {len(unprocessed_dates)} dates to process: {', '.join(unprocessed_dates)}")
            
            for date_str in unprocessed_dates:
                if self.is_shutting_down:
                    break
                    
                try:
                    logger.info(f"Processing date: {date_str}")
                    await self.process_content(date_str)
                    await asyncio.sleep(5)  # Brief pause between dates
                except Exception as e:
                    logger.error(f"Error processing date {date_str}: {str(e)}")
                    continue
            
            logger.info("Completed processing all available dates")
            
        except Exception as e:
            logger.error(f"Error in process_all_dates: {str(e)}")

if __name__ == "__main__":
    import signal
    import sys
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
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
        alpha_filter.is_shutting_down = True
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # If date provided, process only that date
        if len(sys.argv) > 1:
            date_to_process = sys.argv[1]
            asyncio.run(alpha_filter.process_content(date_to_process))
        else:
            # Otherwise process all unprocessed dates
            asyncio.run(alpha_filter.process_all_dates())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received, waiting for cleanup...")
        # Let the cleanup finish
        asyncio.run(alpha_filter.cleanup())
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1) 