"""
File Handler module for managing data input and output
"""
import os
import json
import time
from typing import Dict, List, Any, Optional
from pathlib import Path
from datetime import datetime, timedelta

from core.config import OUTPUT_DIR

class FileHandler:
    """Handles reading and writing files for the scraper"""
    
    def __init__(self):
        """Initialize the file handler and create output directory if it doesn't exist"""
        # Check for environment variable override
        self.output_dir = os.environ.get('OUTPUT_DIR', OUTPUT_DIR)
        
        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir, exist_ok=True)
            print(f"Created output directory: {self.output_dir}")
            
        # Track time for performance metrics
        self.start_time = time.time()
        
        # Create a cache for category configuration to avoid repeated disk reads
        self._config_cache = {}
    
    def save_category_tweets(self, category: str, tweets: List[Dict[str, Any]]) -> str:
        """
        Save tweets from a category to a JSON file in a date-based folder
        Returns the path to the saved file
        """
        if not tweets:
            print(f"No tweets to save for category: {category}")
            return ""
        
        # Get the target date for the folder - either from environment variable or yesterday
        target_date = os.environ.get('TARGET_DATE')
        
        if target_date:
            # Use the specified target date
            date_folder = target_date
            try:
                # Validate the date format (YYYYMMDD)
                date_obj = datetime.strptime(target_date, "%Y%m%d")
                date_string = date_obj.strftime("%Y-%m-%d")
            except ValueError:
                print(f"Warning: Invalid TARGET_DATE format: {target_date}. Using yesterday's date instead.")
                yesterday = datetime.now() - timedelta(days=1)
                date_folder = yesterday.strftime("%Y%m%d")
                date_string = yesterday.strftime("%Y-%m-%d")
        else:
            # Calculate yesterday's date since we're fetching tweets from the past day
            yesterday = datetime.now() - timedelta(days=1)
            date_folder = yesterday.strftime("%Y%m%d")
            date_string = yesterday.strftime("%Y-%m-%d")
        
        # Create the date-based folder path within the output directory
        date_dir = os.path.join(self.output_dir, date_folder)
        
        # Create the date folder if it doesn't exist
        if not os.path.exists(date_dir):
            os.makedirs(date_dir, exist_ok=True)
            print(f"Created date directory: {date_dir}")
            
        # Create filename based on category
        filename = os.path.join(date_dir, f"{category}_Tweets.json")
        
        # Track time for performance metrics
        start_save_time = time.time()
        
        # Prepare data structure with metadata
        metadata = {
            "category": category,
            "tweet_count": len(tweets),
            "date": date_string,
            "timestamp": datetime.now().isoformat(),
            "processing_time_seconds": time.time() - self.start_time
        }
        
        # Set output file name (always JSON)
        output_file = filename
        
        print(f"Saving {len(tweets)} tweets for category {category}")
        print(f"Saving to path: {date_dir}")
        
        try:
            # Save to file with streaming write to minimize memory usage
            with open(output_file, 'w', encoding='utf-8') as f:
                # Write metadata first
                f.write('{\n')
                f.write(f'  "metadata": {json.dumps(metadata, ensure_ascii=False)},\n')
                f.write('  "tweets": [\n')
                
                # Write tweets one by one
                for i, tweet in enumerate(tweets):
                    tweet_json = json.dumps(tweet, ensure_ascii=False, indent=2)
                    if i < len(tweets) - 1:
                        f.write('    ' + tweet_json + ',\n')
                    else:
                        f.write('    ' + tweet_json + '\n')
                
                # Close the JSON structure
                f.write('  ]\n}')
                
            save_time = time.time() - start_save_time
            file_size = os.path.getsize(output_file) / 1024  # KB
            print(f"Saved {len(tweets)} tweets to {output_file}")
            print(f"File size: {file_size:.1f} KB, Save time: {save_time:.2f} seconds")
            
            return output_file
        except Exception as e:
            print(f"Error saving tweets: {str(e)}")
            return ""
    
    def _estimate_size(self, tweets: List[Dict[str, Any]]) -> int:
        """Estimate the size in bytes of the tweets data"""
        # Sample a few tweets to estimate average size
        sample_size = min(10, len(tweets))
        if sample_size == 0:
            return 0
            
        total_size = 0
        for i in range(sample_size):
            tweet_json = json.dumps(tweets[i], ensure_ascii=False)
            total_size += len(tweet_json.encode('utf-8'))
            
        avg_size = total_size / sample_size
        return int(avg_size * len(tweets))
    
    def load_category_configuration(self, config_file: str = "categories.json") -> Dict[str, List[str]]:
        """Load category configuration from a JSON file"""
        # Check cache first
        if config_file in self._config_cache:
            print(f"Using cached configuration for {config_file}")
            return self._config_cache[config_file]
            
        if not os.path.exists(config_file):
            print(f"Configuration file not found: {config_file}")
            return {}
            
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
                
            # Cache the configuration
            self._config_cache[config_file] = config
            
            # Validate the configuration
            valid = self._validate_config(config)
            if not valid:
                print("WARNING: Configuration format may be invalid")
                
            return config
        except json.JSONDecodeError:
            print(f"Error decoding JSON from {config_file}")
            return {}
        except Exception as e:
            print(f"Error loading configuration: {str(e)}")
            return {}
    
    def _validate_config(self, config: Dict[str, Any]) -> bool:
        """Validate the configuration format"""
        if not isinstance(config, dict):
            return False
            
        # Check that each value is a list of strings
        for category, lists in config.items():
            if not isinstance(lists, list):
                print(f"WARNING: Category {category} should have a list of list IDs")
                return False
                
            for list_id in lists:
                if not isinstance(list_id, str):
                    print(f"WARNING: List ID {list_id} in category {category} should be a string")
                    return False
                    
        return True 