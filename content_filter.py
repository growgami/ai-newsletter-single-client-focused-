import logging
import json
from pathlib import Path
from datetime import datetime, timedelta
import zoneinfo
import asyncio
import aiohttp
import re
from openai import AsyncOpenAI
import signal

logger = logging.getLogger(__name__)

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

class ContentFilter:
    def _calculate_chunk_size(self, items):
        """Calculate optimal chunk size based on content length"""
        if not items or len(items) <= 2:
            return 2  # Minimum chunk size
            
        # Calculate average content length including all text fields
        total_length = 0
        for item in items:
            text = item.get('text', '')
            quoted = item.get('quotedContent', {}).get('text', '') if item.get('quotedContent') else ''
            reposted = item.get('repostedContent', {}).get('text', '') if item.get('repostedContent') else ''
            total_length += len(text) + len(quoted) + len(reposted)
            
        avg_length = total_length / len(items)
        
        # Estimate tokens (1 token ~4 chars)
        # Target max 2048 tokens for prompt (half of 4096 max)
        # Leave room for system prompt and response
        max_tokens = 2048
        tokens_per_item = max(1, avg_length / 4)  # Ensure at least 1 token per item
        
        # Calculate how many items we can fit
        optimal_size = int(max_tokens / tokens_per_item)
        
        # Clamp between 2 and 5 items
        return max(2, min(5, optimal_size))
        
    def __init__(self, config):
        self.config = config
        self.data_dir = Path('data')
        self.input_dir = self.data_dir / 'filtered' / 'alpha_filtered'  # Input from alpha filter
        self.output_dir = self.data_dir / 'filtered' / 'content_filtered'  # Output directory
        self.state_file = self.output_dir / 'state.json'  # Track processing state
        
        # Initialize API clients
        self.deepseek_api_key = config['deepseek_api_key']
        self.openai_api_key = config['openai_api_key']
        
        self.deepseek_client = AsyncOpenAI(
            api_key=self.deepseek_api_key,
            base_url="https://api.deepseek.com"
        )
        self.openai_client = AsyncOpenAI(api_key=self.openai_api_key)
        
        self.circuit_breaker = CircuitBreaker()
        self.is_shutting_down = False
        self.force_exit = False
        
        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _load_state(self):
        """Load processing state from file"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading state file: {str(e)}")
        return {
            'last_run_date': None,
            'last_processed_date': None
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

    def _should_run_content_filter(self):
        """Check if content filter should run based on last run date"""
        try:
            state = self._load_state()
            last_run = state.get('last_run_date')
            
            if not last_run:
                return True
                
            last_run_date = datetime.strptime(last_run, '%Y%m%d')
            current_date = datetime.now(zoneinfo.ZoneInfo("UTC"))
            days_since_last_run = (current_date - last_run_date).days
            
            return days_since_last_run >= 3
            
        except Exception as e:
            logger.error(f"Error checking run state: {str(e)}")
            return False

    async def cleanup(self):
        """Cleanup before shutdown"""
        if not self.is_shutting_down:
            self.is_shutting_down = True
            logger.info("Cleaning up before shutdown...")
            # Add any cleanup tasks here
            await asyncio.sleep(0.5)  # Brief pause for cleanup

    async def _try_deepseek_request(self, prompt):
        """Attempt to get a response from Deepseek"""
        try:
            response = await asyncio.wait_for(
                self.deepseek_client.chat.completions.create(
                    model="deepseek-chat",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=1.0,
                    response_format={"type": "json_object"},
                    max_tokens=500
                ),
                timeout=3
            )
            
            if not response.choices:
                return None
                
            content = response.choices[0].message.content
            
            # Validate JSON structure
            try:
                result = json.loads(content)
                required_fields = ['are_duplicates', 'keep_item_ids', 'reason', 'confidence']
                if not all(field in result for field in required_fields):
                    return None
                return content
            except json.JSONDecodeError:
                return None
            
        except (asyncio.TimeoutError, Exception):
            return None

    async def _try_openai_request(self, prompt):
        """Attempt to get a response from OpenAI"""
        try:
            response = await asyncio.wait_for(
                self.openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=1.0,
                    response_format={"type": "json_object"},
                    max_tokens=500
                ),
                timeout=10
            )
            
            content = response.choices[0].message.content
            
            # Validate JSON structure
            try:
                result = json.loads(content)
                required_fields = ['are_duplicates', 'keep_item_ids', 'reason', 'confidence']
                if not all(field in result for field in required_fields):
                    return None
                return content
            except json.JSONDecodeError:
                return None
            
        except (asyncio.TimeoutError, Exception):
            return None

    async def _extract_summary(self, tweet_text, reposted_text='', quoted_text=''):
        """Extract relevant parts of the tweet"""
        try:
            prompt = f"""
            Extract the most relevant part of this tweet. Keep it word-for-word, no paraphrasing.

            Tweet Content: {tweet_text}
            Reposted Content: {reposted_text}
            Quoted Content: {quoted_text}

            Return ONLY the extracted text, no other text or explanation.
            """
            
            response = await self._try_deepseek_request(prompt)
            if not response:
                response = await self._try_openai_request(prompt)
                
            if response:
                # Clean up response - remove quotes and extra whitespace
                extracted = response.strip().strip('"').strip()
                
                # Verify the extracted text is actually in any of the sources
                all_text = [tweet_text, reposted_text, quoted_text]
                all_text = ' '.join([t for t in all_text if t])
                
                if extracted.lower() not in all_text.lower():
                    return tweet_text[:100]  # Fallback to first 100 chars
                    
                return extracted
                
            return tweet_text[:100]  # Fallback to truncated original
            
        except Exception as e:
            logger.error(f"Error extracting text: {str(e)}")
            return tweet_text[:100]  # Fallback to truncated original

    async def _check_duplicate_content(self, items):
        """Check for duplicate content among tweets and select the best version"""
        try:
            if len(items) <= 1:
                return items

            prompt = """Analyze these tweets and determine if they contain the same news/information.
If they are duplicates, select the most informative one based on:
1. Specificity (more specific details are better)
2. Completeness (more context is better)
3. Clarity (clearer explanation is better)
4. Numbers and metrics (prefer tweets with specific numbers)

Tweets to analyze:
{}

Return ONLY a JSON object in this exact format:
{{
    "are_duplicates": boolean,
    "keep_item_ids": [integer array of indices to keep],
    "reason": "string explaining the decision",
    "confidence": float between 0 and 1
}}""".format(json.dumps([{
                'id': idx,
                'text': item['text'],
                'date': item.get('created_at', ''),
                'url': item['url']
            } for idx, item in enumerate(items)], indent=2))

            response = await self._try_deepseek_request(prompt)
            if not response:
                response = await self._try_openai_request(prompt)

            if response:
                try:
                    result = json.loads(response.strip())
                    if isinstance(result, dict) and result.get('are_duplicates') is not None and isinstance(result.get('keep_item_ids'), list):
                        # Keep only the selected items
                        kept_items = [items[idx] for idx in result['keep_item_ids'] if idx < len(items)]
                        if kept_items:  # Only log if we actually kept some items
                            logger.info(f"Duplicate detection: Keeping {len(kept_items)} out of {len(items)} items. Reason: {result.get('reason', 'No reason provided')}")
                            return kept_items
                except (json.JSONDecodeError, KeyError, TypeError, IndexError) as e:
                    logger.error(f"Failed to parse duplicate detection response: {str(e)}")

            return items

        except Exception as e:
            logger.error(f"Error in duplicate detection: {str(e)}")
            return items

    async def process_column(self, items, column_id):
        """Process a single column of items"""
        try:
            filtered_items = []
            
            logger.info(f"Processing column {column_id}")
            
            # Process in chunks for rate limiting
            chunk_size = 10
            for i in range(0, len(items), chunk_size):
                chunk = items[i:i+chunk_size]
                chunk_filtered = []
                
                # Process each item in chunk
                for item in chunk:
                    try:
                        tweet_text = item.get('tweet', '')
                        reposted_text = item.get('reposted_content', '')
                        quoted_text = item.get('quoted_content', '')
                        
                        extracted_text = await self._extract_summary(tweet_text, reposted_text, quoted_text)
                        
                        if extracted_text:
                            filtered_item = {
                                'author': item.get('author', ''),
                                'text': extracted_text,
                                'url': item.get('url', ''),
                                'created_at': item.get('created_at', '')
                            }
                            chunk_filtered.append(filtered_item)
                            
                    except Exception as e:
                        logger.error(f"Error processing item: {str(e)}")
                        continue
                
                # Check for duplicates within the chunk
                if chunk_filtered:
                    chunk_filtered = await self._check_duplicate_content(chunk_filtered)
                    filtered_items.extend(chunk_filtered)
                
                # Rate limiting between chunks
                if i + chunk_size < len(items):
                    await asyncio.sleep(2)
            
            # Final duplicate check across all items
            if len(filtered_items) > 1:
                filtered_items = await self._check_duplicate_content(filtered_items)
            
            if not filtered_items:
                logger.warning(f"No tweets found in column {column_id}")
                return []
            
            logger.info(f"Found {len(filtered_items)} unique tweets")
            
            return {
                'tweets': filtered_items
            }
            
        except Exception as e:
            logger.error(f"Error in process_column: {str(e)}")
            return []

    def _get_category_name(self, column_id):
        """Get category name from column id"""
        category_map = {
            '0': '$TRUMP',
            '1': 'Stablecoins',
            '2': 'SEI',
            '3': 'SUI',
            '4': 'Marketing',
            '5': 'Yappers'
        }
        return category_map.get(column_id, f'Category_{column_id}')

    async def filter_content(self, date_str=None):
        """Process and filter content"""
        try:
            if not date_str:
                current_time = datetime.now(zoneinfo.ZoneInfo("UTC"))
                date_str = current_time.strftime('%Y%m%d')
                
            logger.info(f"Starting content filtering for {date_str}")
            
            # Find all column files
            column_files = list(self.input_dir.glob('column_*.json'))
            if not column_files:
                logger.error(f"No column files found in: {self.input_dir}")
                return
                
            logger.info(f"Found {len(column_files)} columns to process")
            
            # Process each column file
            for column_file in column_files:
                column_id = column_file.stem.split('_')[1]
                category_name = self._get_category_name(column_id)
                logger.info(f"Processing column {column_id} ({category_name})")
                
                try:
                    # Load column data
                    with open(column_file, 'r') as f:
                        column_data = json.load(f)
                        items = column_data.get('tweets', [])
                    
                    if not items:
                        continue
                        
                    logger.info(f"Column {column_id}: Found {len(items)} items")
                    
                    # Process column
                    result = await self.process_column(items, column_id)
                    
                    if result and result.get('tweets'):
                        # Create category-based structure
                        categorized_output = {
                            category_name: {
                                'tweets': result['tweets']
                            }
                        }
                        
                        # Save individual column file
                        output_file = self.output_dir / f'column_{column_id}.json'
                        with open(output_file, 'w') as f:
                            json.dump(categorized_output, f, indent=2)
                            
                        logger.info(f"Column {column_id}: Saved {len(result['tweets'])} items for {category_name}")
                    
                except Exception as e:
                    logger.error(f"Error processing column {column_id}: {str(e)}")
                    continue
                
                # Brief pause between columns
                if column_file != column_files[-1]:
                    await asyncio.sleep(5)
            
            logger.info("Content filtering complete")
            
        except Exception as e:
            logger.error(f"Error during content filtering: {str(e)}")
            raise
            
if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )
    
    # Reduce external library logging
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('openai').setLevel(logging.WARNING)
    
    # Get date to process
    import sys
    date_to_process = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y%m%d')
    
    # Load config
    import os
    from dotenv import load_dotenv
    load_dotenv()
    
    config = {
        'deepseek_api_key': os.getenv('DEEPSEEK_API_KEY'),
        'openai_api_key': os.getenv('OPENAI_API_KEY')
    }
    
    content_filter = ContentFilter(config)
    
    # Signal handling
    def signal_handler(signum, frame):
        if content_filter.is_shutting_down:
            logger.warning("Forcing immediate exit...")
            content_filter.force_exit = True
            sys.exit(1)
        else:
            logger.info("Initiating graceful shutdown (Ctrl+C again to force exit)...")
            asyncio.create_task(content_filter.cleanup())
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        asyncio.run(content_filter.filter_content(date_to_process))
    except KeyboardInterrupt:
        if not content_filter.force_exit:
            logger.info("Waiting for cleanup to complete...")
            asyncio.run(content_filter.cleanup())
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1) 