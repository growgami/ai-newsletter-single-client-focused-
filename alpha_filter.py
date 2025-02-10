"""Alpha filtering and relevance analysis service"""

import logging
import json
from pathlib import Path
from datetime import datetime, timedelta
import zoneinfo
import asyncio
from openai import OpenAI, AsyncOpenAI
from error_handler import with_retry, APIError, log_error, RetryConfig
from category_mapping import CATEGORY

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
        self.processed_dir = self.data_dir / 'processed'  # Input from data_processor
        self.filtered_dir = self.data_dir / 'filtered' / 'alpha_filtered'  # Output directory
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
        
        # Alpha filtering thresholds
        self.alpha_threshold = config.get('alpha_threshold', 0.6)
        
        self.circuit_breaker = CircuitBreaker()
        
        # Create output directory
        self.filtered_dir.mkdir(parents=True, exist_ok=True)

    def _load_state(self):
        """Load processing state from file"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    # Validate state before returning
                    if self._validate_state(state):
                        return state
                    else:
                        logger.warning("Invalid state file, returning default state")
            except Exception as e:
                logger.error(f"Error loading state file: {str(e)}")
        
        # Return default state if file doesn't exist or is invalid
        return {
            'last_processed_date': None,
            'columns_state': {},
            'last_chunk': 0,
            'total_chunks': 0,
            'completed': False
        }

    def _save_state(self, state):
        """Save processing state to file"""
        try:
            temp_file = self.state_file.with_suffix('.tmp')
            with open(temp_file, 'w') as f:
                json.dump(state, f, indent=2)
            temp_file.replace(self.state_file)  # Atomic write
        except Exception as e:
            logger.error(f"Error saving state file: {str(e)}")
            if temp_file.exists():
                temp_file.unlink()

    def _get_input_file(self, date_str):
        """Get input file path from data_processor"""
        return self.processed_dir / date_str / 'combined_tweets.json'

    def _get_output_file(self, date_str):
        """Get output file path"""
        return self.filtered_dir / f'combined_filtered.json'

    async def filter_content(self, tweet, category):
        """Filter and score content for alpha signals"""
        try:
            await self.circuit_breaker.check()
            
            # Prepare prompt with safe content access
            prompt = self._prepare_filtering_prompt(tweet, category)
            if prompt is None:
                logger.warning(f"Skipping tweet {tweet.get('id', 'unknown')} due to invalid content structure")
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
                        logger.debug(f"Tweet {tweet.get('id', 'unknown')} did not meet alpha criteria")
                        return None
                    
                    # Map fields from input tweet to output format
                    filtered_tweet = {
                        'tweet': tweet.get('text', ''),  # Original tweet text
                        'author': tweet.get('authorHandle', ''),  # Original author
                        'url': tweet.get('url', ''),  # Original URL
                        'tweet_id': tweet.get('id', ''),  # Original tweet ID
                        'quoted_content': tweet.get('quotedContent', {}).get('text', '') if tweet.get('quotedContent') else '',
                        'reposted_content': tweet.get('repostedContent', {}).get('text', '') if tweet.get('repostedContent') else '',
                        'category': category,
                        'processed_at': datetime.now(zoneinfo.ZoneInfo("UTC")).isoformat(),
                        'original_date': tweet.get('created_at', ''),
                        'column': tweet.get('column', '')  # Preserve column info
                    }
                    
                    # Log success
                    logger.debug(f"Successfully filtered tweet {tweet.get('id', 'unknown')}")
                    return filtered_tweet
                        
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout on attempt {attempt + 1} for tweet {tweet.get('id', 'unknown')}")
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON response on attempt {attempt + 1} for tweet {tweet.get('id', 'unknown')}: {str(e)}")
                except Exception as e:
                    logger.warning(f"Error on attempt {attempt + 1} for tweet {tweet.get('id', 'unknown')}: {str(e)}")
                
                # Exponential backoff if not last attempt
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)  # 2s, 4s, 8s
                    logger.info(f"Retrying in {delay} seconds...")
                    await asyncio.sleep(delay)
            
            logger.error(f"Failed to filter tweet {tweet.get('id', 'unknown')} after {max_retries} attempts with both models")
            return None
            
        except Exception as e:
            if "Circuit breaker open" in str(e):
                logger.warning(f"Circuit breaker open for tweet {tweet.get('id', 'unknown')} - skipping")
                return None
            else:
                self.circuit_breaker.record_failure()
                logger.error(f"Unexpected error filtering tweet {tweet.get('id', 'unknown')}: {str(e)}")
                return None
            
    def _prepare_filtering_prompt(self, tweet, category):
        """Prepare the prompt for filtering content"""
        try:
            # Map fields from data_processor format
            tweet_text = tweet.get('text', '')  # Changed from 'tweet' to 'text'
            author = tweet.get('authorHandle', '')  # Changed from 'author' to 'authorHandle'
            quoted_text = tweet.get('quotedContent', {}).get('text', '') if tweet.get('quotedContent') else ''  # Changed structure
            reposted_text = tweet.get('repostedContent', {}).get('text', '') if tweet.get('repostedContent') else ''  # Changed structure
            url = tweet.get('url', '')
            tweet_id = tweet.get('id', '')
            
            if not tweet_text or not author:
                logger.warning(f"Missing required content fields for tweet {tweet_id}")
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
               - Must score {self.alpha_threshold}+ to be included
               - Concrete, time-sensitive alpha with clear action points
               - Direct mention of ecosystem name + significant update
    
            2. Significance (0-1): How important is this for {category}?
               - Must score 0.7+ to be included
               - Must impact ecosystem value or token price
               - Consider timing and exclusivity of information
    
            3. Impact (0-1): What measurable effects will this have?
               - Must score 0.6+ to be included
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
                "reposted_content": "{reposted_text}",
                "tweet_id": "{tweet_id}"
            }}
    
            If ANY criteria is not met, return: {{}}
            """
        except Exception as e:
            logger.error(f"Error preparing prompt: {str(e)}")
            return None
            
    def _validate_filter_response(self, response_text: str) -> dict:
        """Validate the filter response"""
        try:
            result = json.loads(response_text)
            
            # If empty result, content didn't meet criteria
            if not result:
                return None
            
            # Check for required content fields
            required_fields = ['tweet', 'author', 'url', 'tweet_id']  # Added tweet_id
            if not all(key in result for key in required_fields):
                raise ValueError(f"Missing required fields: {[k for k in required_fields if k not in result]}")
            
            return result
            
        except json.JSONDecodeError as e:
            logger.warning(f"JSON decode error: {str(e)}")
            raise
        except Exception as e:
            logger.warning(f"Invalid response format: {str(e)}")
            raise
            
    async def _try_deepseek_request(self, prompt):
        """Attempt to get a response from Deepseek"""
        try:
            # Add 3 second timeout for Deepseek
            response = await asyncio.wait_for(
                self.deepseek_client.chat.completions.create(
                    model="deepseek-chat",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.5,
                    response_format={"type": "json_object"},
                    max_tokens=4096
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
                    temperature=0.5,
                    response_format={"type": "json_object"},
                    max_tokens=4096
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

    def _clear_output_files(self):
        """Clear all output files for fresh processing"""
        try:
            output_file = self._get_output_file(None)  # Get base output file
            if output_file.exists():
                output_file.unlink()
            logger.info("Cleared existing output files for new processing")
        except Exception as e:
            logger.error(f"Error clearing output files: {str(e)}")

    def _get_processing_state(self, date_str):
        """Get processing state for a specific date"""
        state = self._load_state()
        return {
            'completed': state.get('completed', False),  # Check actual completion flag
            'last_chunk': state.get('last_chunk', 0),
            'total_chunks': state.get('total_chunks', 0)  # Preserve total chunks from state
        }

    def _update_processing_state(self, date_str, chunk_number, total_chunks, completed=False):
        """Update processing state for a specific date"""
        try:
            state = self._load_state()
            
            # Update processing state
            state['last_processed_date'] = date_str
            state['last_chunk'] = chunk_number
            state['total_chunks'] = total_chunks
            state['completed'] = completed
            state['columns_state'] = {}  # Keep this for backward compatibility
            
            self._save_state(state)
            logger.info(f"Updated state: chunk {chunk_number}/{total_chunks} {'(completed)' if completed else ''}")
        except Exception as e:
            logger.error(f"Error updating state: {str(e)}")

    def _get_unprocessed_dates(self):
        """Get list of dates with unprocessed tweets"""
        processed_dates = set()
        if self.state_file.exists():
            try:
                state = self._load_state()
                if state.get('completed', False) and state.get('last_processed_date'):
                    processed_dates.add(state.get('last_processed_date'))
            except Exception as e:
                logger.error(f"Error loading state: {str(e)}")
        
        # Find all date folders in processed directory
        raw_dates = set()
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

    async def process_content(self, date_str=None):
        """Process content for a given date"""
        try:
            if not date_str:
                current_time = datetime.now(zoneinfo.ZoneInfo("UTC"))
                date_str = current_time.strftime('%Y%m%d')

            logger.info(f"Starting alpha filtering for {date_str}")
            
            # Check processing state
            state = self._get_processing_state(date_str)
            if state['completed']:
                logger.info(f"Date {date_str} already processed completely")
                return None

            # Load input data from data_processor
            input_file = self._get_input_file(date_str)
            if not input_file.exists():
                logger.error(f"No input file found at: {input_file}")
                return None

            try:
                with open(input_file, 'r') as f:
                    data = json.load(f)
            except Exception as e:
                logger.error(f"Error loading input file: {str(e)}")
                return None

            # Load existing output or create new structure
            output_file = self._get_output_file(date_str)
            if output_file.exists():
                try:
                    with open(output_file, 'r') as f:
                        output = json.load(f)
                except Exception as e:
                    logger.error(f"Error loading existing output: {str(e)}")
                    output = {
                        'tweets': [],
                        'metadata': {
                            'processed_date': date_str,
                            'total_tweets': 0,
                            'last_update': datetime.now().isoformat()
                        }
                    }
            else:
                output = {
                    'tweets': [],
                    'metadata': {
                        'processed_date': date_str,
                        'total_tweets': 0,
                        'last_update': datetime.now().isoformat()
                    }
                }

            # Process tweets in chunks for rate limiting
            tweets = data.get('tweets', [])
            chunk_size = 5  # Process 5 tweets at a time
            total_chunks = (len(tweets) + chunk_size - 1) // chunk_size
            
            # Resume from last processed chunk
            start_chunk = state['last_chunk']
            logger.info(f"Resuming from chunk {start_chunk + 1}/{total_chunks}")
            
            for i in range(start_chunk * chunk_size, len(tweets), chunk_size):
                if self.is_shutting_down:
                    logger.info("Graceful shutdown requested...")
                    self._update_processing_state(date_str, i // chunk_size, total_chunks)
                    break

                chunk = tweets[i:i+chunk_size]
                chunk_number = i // chunk_size + 1
                
                logger.info(f"Processing chunk {chunk_number}/{total_chunks}")
                
                # Process chunk in parallel
                chunk_tasks = [self.filter_content(tweet, CATEGORY) for tweet in chunk]
                chunk_results = await asyncio.gather(*chunk_tasks)
                
                # Track new tweets for this chunk
                new_tweets = []
                
                # Add successful results
                for result in chunk_results:
                    if result is not None:
                        new_tweets.append(result)
                
                if new_tweets:
                    # Add new tweets to output
                    output['tweets'].extend(new_tweets)
                    
                    # Update metadata
                    output['metadata']['total_tweets'] = len(output['tweets'])
                    output['metadata']['last_update'] = datetime.now().isoformat()
                    
                    # Save output atomically
                    try:
                        temp_file = output_file.with_suffix('.tmp')
                        with open(temp_file, 'w') as f:
                            json.dump(output, f, indent=2)
                        temp_file.replace(output_file)
                        logger.info(f"Saved {len(new_tweets)} new tweets (total: {len(output['tweets'])})")
                    except Exception as e:
                        logger.error(f"Error saving output: {str(e)}")
                        if temp_file.exists():
                            temp_file.unlink()
                else:
                    logger.info("No new tweets in this chunk met criteria")
                
                # Update state.json with progress
                self._update_processing_state(date_str, chunk_number, total_chunks)
                
                # Rate limiting between chunks
                if i + chunk_size < len(tweets):
                    await asyncio.sleep(2)

            # Mark as completed if not shutdown
            if not self.is_shutting_down:
                self._update_processing_state(date_str, total_chunks, total_chunks, completed=True)
                logger.info(f"Completed processing {date_str} with {len(output['tweets'])} filtered tweets")

            return output

        except Exception as e:
            logger.error(f"Error in process_content: {str(e)}")
            return None

    async def cleanup(self):
        """Cleanup before shutdown"""
        self.is_shutting_down = True
        logger.info("Cleaning up before shutdown...")
        await asyncio.sleep(0.5)  # Brief pause for cleanup

    def reset_state(self):
        """Reset processing state to start fresh"""
        try:
            if self.state_file.exists():
                self.state_file.unlink()
            # Also clear output files
            self._clear_output_files()
            logger.info("Reset processing state and cleared outputs")
        except Exception as e:
            logger.error(f"Error resetting state: {str(e)}")

    def _validate_state(self, state):
        """Validate state structure and values"""
        try:
            required_fields = ['last_processed_date', 'last_chunk', 'total_chunks', 'completed']
            if not all(field in state for field in required_fields):
                logger.error("Invalid state structure")
                return False
            
            # Validate numeric fields
            if not isinstance(state['last_chunk'], int) or state['last_chunk'] < 0:
                logger.error("Invalid last_chunk value")
                return False
            
            if not isinstance(state['total_chunks'], int) or state['total_chunks'] < 0:
                logger.error("Invalid total_chunks value")
                return False
            
            # Validate date format if exists
            if state['last_processed_date']:
                try:
                    datetime.strptime(state['last_processed_date'], '%Y%m%d')
                except ValueError:
                    logger.error("Invalid date format in state")
                    return False
            
            return True
        except Exception as e:
            logger.error(f"Error validating state: {str(e)}")
            return False

    def _validate_output_file(self, output_file):
        """Validate output file structure and content"""
        try:
            if not output_file.exists():
                return False
            
            with open(output_file, 'r') as f:
                data = json.load(f)
            
            # Check basic structure
            if not isinstance(data, dict):
                logger.error("Output file is not a valid JSON object")
                return False
            
            required_fields = ['tweets', 'metadata']
            if not all(field in data for field in required_fields):
                logger.error("Invalid output file structure")
                return False
            
            metadata_fields = ['processed_date', 'total_tweets', 'last_update']
            if not all(field in data['metadata'] for field in metadata_fields):
                logger.error("Invalid metadata structure")
                return False
            
            # Validate tweets array
            if not isinstance(data['tweets'], list):
                logger.error("Tweets field is not an array")
                return False
            
            # Validate tweet structure if any exist
            for tweet in data['tweets']:
                required_tweet_fields = ['tweet', 'author', 'url', 'tweet_id', 'category']
                if not all(field in tweet for field in required_tweet_fields):
                    logger.error("Invalid tweet structure")
                    return False
            
            return True
        except json.JSONDecodeError:
            logger.error("Invalid JSON in output file")
            return False
        except Exception as e:
            logger.error(f"Error validating output file: {str(e)}")
            return False

    async def recover_state(self):
        """Emergency recovery of processing state"""
        try:
            logger.info("Starting emergency state recovery")
            
            # Check output file
            output_file = self._get_output_file(None)
            if self._validate_output_file(output_file):
                with open(output_file, 'r') as f:
                    data = json.load(f)
                    date_str = data['metadata']['processed_date']
                    total_tweets = data['metadata']['total_tweets']
                
                # Load input file to get total chunks
                input_file = self._get_input_file(date_str)
                if input_file.exists():
                    with open(input_file, 'r') as f:
                        input_data = json.load(f)
                        total_tweets_input = len(input_data.get('tweets', []))
                        chunk_size = 5  # Match our chunk size
                        total_chunks = (total_tweets_input + chunk_size - 1) // chunk_size
                else:
                    logger.error("Cannot find input file for recovery")
                    return False
                
                # Reconstruct state
                state = {
                    'last_processed_date': date_str,
                    'last_chunk': 0,  # Reset to beginning to be safe
                    'total_chunks': total_chunks,
                    'completed': False,
                    'columns_state': {}  # Keep for backward compatibility
                }
                
                self._save_state(state)
                logger.info(f"Recovered state for date {date_str} with {total_tweets} tweets")
                logger.info(f"Processing will resume from the beginning to ensure completeness")
                return True
                
        except Exception as e:
            logger.error(f"Error during state recovery: {str(e)}")
        return False

    def get_processing_progress(self):
        """Get current processing progress"""
        try:
            state = self._load_state()
            if not self._validate_state(state):
                return None
            
            output_file = self._get_output_file(None)
            output_valid = self._validate_output_file(output_file)
            
            # Get tweet counts if output is valid
            total_tweets = 0
            if output_valid:
                with open(output_file, 'r') as f:
                    data = json.load(f)
                    total_tweets = data['metadata']['total_tweets']
            
            progress = {
                'date': state['last_processed_date'],
                'progress': f"{state['last_chunk']}/{state['total_chunks']} chunks",
                'percentage': round((state['last_chunk'] / state['total_chunks'] * 100) if state['total_chunks'] > 0 else 0, 2),
                'completed': state['completed'],
                'output_valid': output_valid,
                'total_tweets_processed': total_tweets,
                'last_update': datetime.now(zoneinfo.ZoneInfo("UTC")).isoformat()
            }
            
            logger.info(f"Progress: {progress['percentage']}% ({progress['progress']}) - {total_tweets} tweets processed")
            return progress
            
        except Exception as e:
            logger.error(f"Error getting progress: {str(e)}")
            return None

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )
    
    # Load config
    import os
    from dotenv import load_dotenv
    load_dotenv()
    
    config = {
        'deepseek_api_key': os.getenv('DEEPSEEK_API_KEY'),
        'openai_api_key': os.getenv('OPENAI_API_KEY')
    }
    
    # Setup signal handlers for graceful shutdown
    import signal
    
    def signal_handler(sig, frame):
        logger.info("Initiating graceful shutdown...")
        alpha_filter.is_shutting_down = True
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Get date to process
    import sys
    date_to_process = sys.argv[1] if len(sys.argv) > 1 else None

    # Run processor
    alpha_filter = AlphaFilter(config)
    try:
        if date_to_process:
            asyncio.run(alpha_filter.process_content(date_to_process))
        else:
            # Process all unprocessed dates
            asyncio.run(alpha_filter.process_all_dates())
    except KeyboardInterrupt:
        logger.info("Waiting for cleanup...")
        asyncio.run(alpha_filter.cleanup())
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1) 