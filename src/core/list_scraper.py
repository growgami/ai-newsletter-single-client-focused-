"""
List Scraper module for fetching tweets from Twitter lists
"""
from typing import Dict, List, Any
import concurrent.futures
import os

from core.api_client import TwitterAPIClient
from core.config import MAX_TWEETS_PER_USER

class ListScraper:
    """Handles scraping tweets from Twitter lists"""
    
    def __init__(self):
        self.api_client = TwitterAPIClient()
        # Get the number of workers from environment variable or use default
        self.max_workers = self._safe_get_env_int('MAX_SCRAPER_WORKERS', 5)
    
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
    
    def scrape_list(self, list_id: str) -> List[Dict[str, Any]]:
        """
        Scrape all tweets from a Twitter list using the direct list tweets endpoint
        
        Args:
            list_id: The Twitter list ID (just the numeric ID)
        """
        print(f"Directly scraping tweets from list ID: {list_id}")
        
        # Extract the list ID if it's a URL
        list_id = self._extract_list_id(list_id)
        
        # Skip getting list info and use the list_id directly
        tweets = self.api_client.get_list_tweets(list_id)
        
        # Process tweets to match the required format from instructions.md
        processed_tweets = []
        filtered_count = 0
        
        for tweet in tweets:
            # Check if this is a reply - if so, skip it
            # A reply has an 'in_reply_to_status_id' field or starts with '@'
            is_reply = bool(tweet.get("in_reply_to_status_id")) or bool(tweet.get("in_reply_to_user_id"))
            
            # Additional check: if the tweet text starts with @username it's likely a reply
            tweet_text = tweet.get("text", "").strip()
            if not is_reply and tweet_text and tweet_text.startswith("@"):
                is_reply = True
            
            # Skip replies
            if is_reply:
                filtered_count += 1
                continue
                
            # Extract the required fields according to instructions.md:
            # - Tweet ID
            # - Text
            # - Author Handle
            # - URL
            # - Is Repost (Retweet) + Reposted Content
            # - Is Quote Tweet + Quoted Content
            processed_tweet = {
                "id": tweet.get("id", ""),
                "text": tweet_text,
                "author_handle": tweet.get("author", {}).get("userName", ""),
                "url": tweet.get("url", ""),
                "is_repost": bool(tweet.get("retweeted_tweet")),
                "is_quote_tweet": bool(tweet.get("quoted_tweet"))
            }
            
            # Add reposted content if it exists
            if processed_tweet["is_repost"] and tweet.get("retweeted_tweet"):
                retweet = tweet["retweeted_tweet"]
                processed_tweet["reposted_content"] = {
                    "id": retweet.get("id", ""),
                    "text": retweet.get("text", ""),
                    "author_handle": retweet.get("author", {}).get("userName", ""),
                    "url": retweet.get("url", "")
                }
            
            # Add quoted content if it exists
            if processed_tweet["is_quote_tweet"] and tweet.get("quoted_tweet"):
                quote = tweet["quoted_tweet"]
                processed_tweet["quoted_content"] = {
                    "id": quote.get("id", ""),
                    "text": quote.get("text", ""),
                    "author_handle": quote.get("author", {}).get("userName", ""),
                    "url": quote.get("url", "")
                }
            
            processed_tweets.append(processed_tweet)
        
        print(f"Retrieved {len(tweets)} tweets from list ID: {list_id}")
        print(f"Filtered out {filtered_count} replies")
        print(f"Processed {len(processed_tweets)} non-reply tweets")
        return processed_tweets
    
    def _extract_list_id(self, list_id_or_url: str) -> str:
        """Extract the list ID from a list ID or URL"""
        # If it's already just a numeric ID, return it
        if list_id_or_url.strip().isdigit():
            return list_id_or_url.strip()
            
        # If it's a URL, extract the ID
        if "twitter.com/i/lists/" in list_id_or_url:
            return list_id_or_url.split("/")[-1].split("?")[0].strip()
            
        # Otherwise just return what we got
        return list_id_or_url.strip()
    
    def _scrape_list_worker(self, list_id: str) -> Dict[str, Any]:
        """Worker function for parallel processing of lists"""
        try:
            # Create a new API client for each worker to prevent shared state issues
            client = TwitterAPIClient()
            
            # Create a temporary scraper with this client
            temp_scraper = ListScraper()
            temp_scraper.api_client = client
            
            # Scrape the list
            result = temp_scraper.scrape_list(list_id)
            
            return {
                "list_id": list_id,
                "tweets": result,
                "success": True
            }
        except Exception as e:
            print(f"Error scraping list {list_id}: {str(e)}")
            return {
                "list_id": list_id,
                "tweets": [],
                "success": False,
                "error": str(e)
            }
        
    def scrape_category_lists(self, category: str, list_urls: List[str]) -> List[Dict[str, Any]]:
        """Scrape tweets from all lists in a category in parallel"""
        all_category_tweets = []
        
        print(f"Starting parallel scraping for category: {category}")
        print(f"Lists to scrape: {len(list_urls)}")
        print(f"Using {self.max_workers} workers for parallel processing")
        
        # Use ThreadPoolExecutor for parallel processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all scraping jobs
            future_to_list = {executor.submit(self._scrape_list_worker, list_id): list_id for list_id in list_urls}
            
            # Process results as they come in
            for future in concurrent.futures.as_completed(future_to_list):
                list_id = future_to_list[future]
                try:
                    result = future.result()
                    if result["success"]:
                        all_category_tweets.extend(result["tweets"])
                        print(f"Successfully processed list {list_id}. Got {len(result['tweets'])} tweets.")
                    else:
                        print(f"Failed to process list {list_id}: {result.get('error', 'Unknown error')}")
                except Exception as e:
                    print(f"Exception processing result for list {list_id}: {str(e)}")
        
        print(f"Completed scraping for category: {category}")
        print(f"Total tweets collected: {len(all_category_tweets)}")
        
        return all_category_tweets 