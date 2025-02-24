import logging
import asyncio
import os
import gc
import psutil
from pathlib import Path

logger = logging.getLogger(__name__)

class GarbageCollector:
    def __init__(self, config):
        self.config = config
        self.process = psutil.Process(os.getpid())
        self.is_running = True
        self.check_interval = config.get('check_interval', 3600)  # Default 1 hour
        
        # Setup data directories
        self.data_dir = Path('data')
        self.raw_dir = self.data_dir / 'raw'
        self.processed_dir = self.data_dir / 'processed'
        self.summaries_dir = self.data_dir / 'summaries'
        self.logs_dir = Path('logs')
        
        # Ensure directories exist
        for dir_path in [self.raw_dir, self.processed_dir, self.summaries_dir, self.logs_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
            
    async def start(self):
        """Start the garbage collection service"""
        logger.info("Starting garbage collection service")
        while self.is_running:
            try:
                await self.run_cleanup()
                await asyncio.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Error in garbage collection: {str(e)}")
                await asyncio.sleep(60)  # Wait a minute before retrying
                
    async def run_cleanup(self):
        """Run memory cleanup tasks"""
        try:
            # Force garbage collection
            gc.collect()
            
            # Log memory usage
            memory_info = self.process.memory_info()
            memory_percent = self.process.memory_percent()
            
            logger.info(
                f"Memory Status: Process using {memory_percent:.1f}% "
                f"({memory_info.rss / 1024 / 1024:.1f} MB)"
            )
            
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
            
    async def cleanup_memory(self):
        """Monitor and cleanup memory usage"""
        try:
            # Get current memory usage
            memory_info = self.process.memory_info()
            memory_percent = self.process.memory_percent()
            
            # Get system memory info
            system_memory = psutil.virtual_memory()
            swap_memory = psutil.swap_memory()
            
            logger.info(
                f"Memory Status:\n"
                f"Process: {memory_percent:.1f}% ({memory_info.rss / 1024 / 1024:.1f} MB)\n"
                f"System: {system_memory.percent:.1f}% used, {system_memory.available / 1024 / 1024:.1f}MB available\n"
                f"Swap: {swap_memory.percent:.1f}% used, {swap_memory.free / 1024 / 1024:.1f}MB free"
            )
            
            # If memory usage is high (>80%), take action
            if memory_percent > 80 or system_memory.percent > 90:
                logger.warning(f"High memory usage detected: Process={memory_percent:.1f}%, System={system_memory.percent:.1f}%")
                
                # Force garbage collection
                gc.collect()
                
                # Clear any internal caches
                self.clear_caches()
                
                # Drop system caches if we're still high
                if system_memory.percent > 90:
                    self.drop_system_caches()
                
                # Log new memory usage
                new_memory = psutil.virtual_memory()
                logger.info(f"Memory usage after cleanup: {new_memory.percent:.1f}%")
                
        except Exception as e:
            logger.error(f"Error cleaning up memory: {str(e)}")
            
    def clear_caches(self):
        """Clear internal caches and temporary data"""
        try:
            # Clear Python's internal caches
            gc.collect()
            gc.collect()  # Second pass to collect cyclic references
            
            # Clear file system caches
            if hasattr(os, 'sync'):
                os.sync()
                
        except Exception as e:
            logger.error(f"Error clearing caches: {str(e)}")
            
    def drop_system_caches(self):
        """Drop system caches on Linux systems"""
        try:
            if os.path.exists('/proc/sys/vm/drop_caches'):
                # Only attempt if we have sudo privileges
                if os.geteuid() == 0:  # Check if running as root
                    try:
                        # Sync filesystem to avoid data loss
                        os.system('sync')
                        # Drop page cache, dentries and inodes
                        os.system('echo 3 > /proc/sys/vm/drop_caches')
                        logger.info("Successfully dropped system caches")
                    except Exception as e:
                        logger.error(f"Failed to drop system caches: {str(e)}")
                else:
                    logger.debug("Skipping system cache drop - not running as root")
                    
        except Exception as e:
            logger.error(f"Error accessing system cache controls: {str(e)}")
            
    def stop(self):
        """Stop the garbage collection service"""
        self.is_running = False
        logger.info("Stopped garbage collection service") 