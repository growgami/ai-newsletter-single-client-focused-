"""
Twitter API Client for interacting with twitterapi.io
"""
import os
import time
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any
import random

from core.config import API_KEY, API_SECRET, BEARER_TOKEN, RATE_LIMIT_WAIT, DAYS_TO_SCRAPE

class TwitterAPIClient:
    """Client for making requests to the Twitter API via twitterapi.io"""
    
    def __init__(self):
        # Get API endpoint from environment or use default
        self.base_url = os.environ.get('API_BASE_URL', "https://api.twitterapi.io")
        
        # Check for environment variable overrides
        api_key = os.environ.get('TWITTER_API_KEY')
        
        # Check if credentials are set
        if not api_key:
            raise ValueError("API key not set. Please set TWITTER_API_KEY in your environment variables.")
        
        # Configure headers with optimizations
        self.headers = {
            "X-API-Key": api_key,
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate"  # Enable compression
        }
        
        # Track rate limit attempts
        self.rate_limit_attempts = 0
        self.max_retry_attempts = self._safe_get_env_int('MAX_RETRY_ATTEMPTS', 5)
        
        # Create a session for connection pooling
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # Configure session for better performance
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=self.max_retry_attempts,
            pool_maxsize=self.max_retry_attempts * 2,
            max_retries=1  # We'll handle retries ourselves
        )
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)
        
        print(f"Initialized API client for {self.base_url}")
    
    def _safe_get_env_int(self, env_var: str, default_value: int) -> int:
        """Safely get an integer from an environment variable with robust error handling"""
        try:
            # Get the raw environment variable
            value = os.environ.get(env_var)
            if value is None:
                return default_value
                
            # Try to convert to int, stripping any whitespace or comments
            # This handles malformed .env files where comments are on the same line
            clean_value = value.split('#')[0].strip()
            return int(clean_value)
        except (ValueError, TypeError):
            print(f"WARNING: Invalid value for {env_var}, using default: {default_value}")
            return default_value
    
    def _handle_rate_limit(self, response: requests.Response) -> bool:
        """
        Handle rate limiting from the Twitter API with exponential backoff
        Returns True if we should retry the request, False otherwise
        """
        if response.status_code != 429:  # Not rate limited
            # Reset counter on successful requests
            self.rate_limit_attempts = 0
            return False
            
        # Get rate limit wait time from env or use default
        rate_limit_wait = self._safe_get_env_int('RATE_LIMIT_WAIT', 60)
        
        # Increment attempts counter
        self.rate_limit_attempts += 1
        
        # Check if we've exceeded max retries
        if self.rate_limit_attempts > self.max_retry_attempts:
            print(f"Exceeded maximum retry attempts ({self.max_retry_attempts}). Giving up.")
            return False
            
        # Calculate wait time with exponential backoff and jitter
        wait_time = rate_limit_wait * (2 ** (self.rate_limit_attempts - 1))
        # Add jitter to prevent "thundering herd" problems
        jitter = random.uniform(0.5, 1.5)
        wait_time = wait_time * jitter
        
        print(f"Rate limited. Retry attempt {self.rate_limit_attempts}/{self.max_retry_attempts}. Waiting {wait_time:.1f} seconds...")
        time.sleep(wait_time)
        return True  # Retry the request
    
    def get_list_tweets(self, list_id: str) -> List[Dict[str, Any]]:
        """
        Get all tweets from a list based on the list ID
        
        Implements the exact API pattern shown in the example with optimizations.
        """
        # Ensure list_id is just the numeric id
        list_id = list_id.strip()
        
        print(f"Fetching tweets for list ID: {list_id}")
        
        # Use the endpoint directly as shown in example
        url = f"{self.base_url}/twitter/list/tweets"
        
        # Calculate sinceTime for the past day (in seconds)
        days_to_scrape = self._safe_get_env_int('DAYS_TO_SCRAPE', 1)
        since_time = int((datetime.utcnow() - timedelta(days=days_to_scrape)).timestamp())
        
        # Parameters
        params = {
            "listId": list_id,
            "sinceTime": since_time
        }
        
        print(f"Making request for list {list_id} since {days_to_scrape} days ago")
        
        all_tweets = []
        cursor = None
        page_count = 0
        
        # Handle pagination for all tweets
        while True:
            page_count += 1
            if cursor:
                params["cursor"] = cursor
            
            try:
                # Use session for connection pooling
                response = self.session.get(
                    url, 
                    params=params, 
                    timeout=30,  # Increased timeout for larger responses
                    stream=True   # Stream the response to manage memory better
                )
                
                # Handle rate limiting with retry logic
                if response.status_code == 429 and self._handle_rate_limit(response):
                    continue  # Retry the request
                
                if response.status_code != 200:
                    print(f"Error response ({response.status_code}): {response.text}")
                    break
                
                # Check response size
                content_length = response.headers.get('Content-Length')
                if content_length:
                    print(f"Response size: {int(content_length) / 1024:.1f} KB")
                
                data = response.json()
                
                # Add tweets to our collection
                tweets_in_page = data.get("tweets", [])
                if tweets_in_page:
                    tweets_count = len(tweets_in_page)
                    all_tweets.extend(tweets_in_page)
                    print(f"Page {page_count}: Retrieved {tweets_count} tweets. Total: {len(all_tweets)}")
                else:
                    print("No tweets found in response")
                    break
                
                # Check for next page
                if data.get("has_next_page") and data.get("next_cursor"):
                    cursor = data["next_cursor"]
                    print(f"Getting next page with cursor: {cursor}")
                else:
                    print("No more pages")
                    break
                    
            except requests.exceptions.Timeout:
                print(f"Request timed out. Retrying page {page_count}...")
                continue
            except requests.exceptions.ConnectionError as e:
                print(f"Connection error: {str(e)}. Retrying in 5 seconds...")
                time.sleep(5)
                continue
            except Exception as e:
                print(f"Exception in API request: {str(e)}")
                break
        
        print(f"Completed fetching tweets for list {list_id}. Retrieved {len(all_tweets)} tweets.")
        return all_tweets
        
    def __del__(self):
        """Ensure resources are properly cleaned up"""
        if hasattr(self, 'session'):
            try:
                self.session.close()
            except:
                pass 