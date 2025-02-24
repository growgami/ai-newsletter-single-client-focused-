"""Error handling and retry utilities"""

import logging
import asyncio
import functools
import traceback
from datetime import datetime
from typing import Type, Tuple, Optional, Callable, TypeVar, ParamSpec

# Type variables for generic function signatures
T = TypeVar('T')  # Return type
P = ParamSpec('P')  # Parameters

logger = logging.getLogger(__name__)

class RetryConfig:
    def __init__(
        self, 
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 30.0,
        backoff_factor: float = 2.0,
        retry_on: Optional[Tuple[Type[Exception], ...]] = None
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.retry_on = retry_on or (Exception,)
        self.attempt_count = 0
        self.last_attempt = None

    def reset(self):
        """Reset retry counter"""
        self.attempt_count = 0
        self.last_attempt = None

    def get_next_delay(self) -> float:
        """Calculate next retry delay using exponential backoff"""
        self.attempt_count += 1
        delay = min(
            self.base_delay * (self.backoff_factor ** (self.attempt_count - 1)),
            self.max_delay
        )
        self.last_attempt = datetime.now()
        return delay

class APIError(Exception):
    """Custom exception for API-related errors"""
    pass

def log_error(logger: logging.Logger, error: Exception, context: str = None):
    """Centralized error logging with context and stack trace
    
    Args:
        logger: Logger instance to use
        error: Exception that occurred
        context: Additional context about where/when the error occurred
    """
    error_type = type(error).__name__
    error_msg = str(error)
    stack_trace = traceback.format_exc()
    
    log_message = f"Error Type: {error_type}\n"
    if context:
        log_message += f"Context: {context}\n"
    log_message += f"Message: {error_msg}\n"
    log_message += f"Stack Trace:\n{stack_trace}"
    
    logger.error(log_message)
    if hasattr(error, '__traceback__'):
        logger.debug("Detailed traceback:", exc_info=error)

def with_retry(config: RetryConfig):
    """Retry decorator with exponential backoff
    
    Args:
        config: RetryConfig instance with retry settings
    """
    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            config.reset()
            last_error = None

            for attempt in range(config.max_retries):
                try:
                    if attempt > 0:
                        delay = config.get_next_delay()
                        logger.info(f"Retry attempt {attempt + 1}/{config.max_retries}, waiting {delay:.1f}s")
                        await asyncio.sleep(delay)
                        
                    return await func(*args, **kwargs)
                    
                except config.retry_on as e:
                    last_error = e
                    logger.warning(
                        f"Attempt {attempt + 1} failed for {func.__name__}. "
                        f"Error: {str(e)}"
                    )
                    # Log detailed error info for debugging
                    logger.debug(f"Detailed error:\n{traceback.format_exc()}")
                    continue
                    
            logger.error(f"All {config.max_retries} retry attempts failed")
            raise last_error

        return wrapper
    return decorator

class NetworkError(Exception):
    """Exception for network-related errors"""
    pass

class BrowserError(Exception):
    """Exception for browser automation errors"""
    pass

class DataProcessingError(Exception):
    """Exception for data processing errors"""
    pass

class TelegramError(Exception):
    """Exception for Telegram-related errors"""
    pass 