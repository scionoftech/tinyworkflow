"""
Retry logic with exponential backoff and jitter.
"""

import asyncio
import random
from dataclasses import dataclass
from typing import Optional, Callable, Any, TypeVar
from datetime import timedelta

T = TypeVar("T")


@dataclass
class RetryPolicy:
    """
    Retry policy configuration.

    Args:
        max_retries: Maximum number of retry attempts (default: 3)
        initial_delay: Initial delay in seconds before first retry (default: 1.0)
        max_delay: Maximum delay in seconds between retries (default: 60.0)
        backoff_multiplier: Multiplier for exponential backoff (default: 2.0)
        jitter: Whether to add random jitter to delays (default: True)
        jitter_factor: Factor for jitter randomization, 0.0-1.0 (default: 0.1)
    """

    max_retries: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    backoff_multiplier: float = 2.0
    jitter: bool = True
    jitter_factor: float = 0.1

    def calculate_delay(self, retry_count: int) -> float:
        """
        Calculate delay for the given retry attempt.

        Args:
            retry_count: Current retry attempt (0-indexed)

        Returns:
            Delay in seconds
        """
        # Exponential backoff
        delay = min(
            self.initial_delay * (self.backoff_multiplier**retry_count), self.max_delay
        )

        # Add jitter
        if self.jitter and retry_count > 0:
            jitter_amount = delay * self.jitter_factor
            delay += random.uniform(-jitter_amount, jitter_amount)

        return max(0, delay)


class RetryExecutor:
    """Executes functions with retry logic."""

    @staticmethod
    async def execute_with_retry(
        func: Callable[..., Any],
        *args,
        retry_policy: Optional[RetryPolicy] = None,
        on_retry: Optional[Callable[[int, Exception], None]] = None,
        **kwargs,
    ) -> Any:
        """
        Execute an async function with retry logic.

        Args:
            func: Async function to execute
            *args: Positional arguments for the function
            retry_policy: Retry policy configuration
            on_retry: Optional callback called on each retry (retry_count, exception)
            **kwargs: Keyword arguments for the function

        Returns:
            Function result

        Raises:
            Last exception if all retries fail
        """
        if retry_policy is None:
            retry_policy = RetryPolicy()

        last_exception = None

        for attempt in range(retry_policy.max_retries + 1):
            try:
                if asyncio.iscoroutinefunction(func):
                    return await func(*args, **kwargs)
                else:
                    # Support sync functions
                    return func(*args, **kwargs)

            except Exception as e:
                last_exception = e

                # If we've exhausted retries, raise
                if attempt >= retry_policy.max_retries:
                    raise

                # Calculate delay and wait
                delay = retry_policy.calculate_delay(attempt)

                # Call retry callback if provided
                if on_retry:
                    on_retry(attempt + 1, e)

                await asyncio.sleep(delay)

        # Should never reach here, but just in case
        if last_exception:
            raise last_exception

    @staticmethod
    def execute_with_retry_sync(
        func: Callable[..., T],
        *args,
        retry_policy: Optional[RetryPolicy] = None,
        on_retry: Optional[Callable[[int, Exception], None]] = None,
        **kwargs,
    ) -> T:
        """
        Execute a sync function with retry logic.

        Args:
            func: Function to execute
            *args: Positional arguments for the function
            retry_policy: Retry policy configuration
            on_retry: Optional callback called on each retry (retry_count, exception)
            **kwargs: Keyword arguments for the function

        Returns:
            Function result

        Raises:
            Last exception if all retries fail
        """
        if retry_policy is None:
            retry_policy = RetryPolicy()

        last_exception = None

        for attempt in range(retry_policy.max_retries + 1):
            try:
                return func(*args, **kwargs)

            except Exception as e:
                last_exception = e

                # If we've exhausted retries, raise
                if attempt >= retry_policy.max_retries:
                    raise

                # Calculate delay and wait
                delay = retry_policy.calculate_delay(attempt)

                # Call retry callback if provided
                if on_retry:
                    on_retry(attempt + 1, e)

                import time

                time.sleep(delay)

        # Should never reach here, but just in case
        if last_exception:
            raise last_exception


def with_retry(retry_policy: Optional[RetryPolicy] = None):
    """
    Decorator to add retry logic to async functions.

    Args:
        retry_policy: Retry policy configuration

    Example:
        @with_retry(RetryPolicy(max_retries=5))
        async def flaky_api_call():
            # May fail and will be retried
            pass
    """

    def decorator(func: Callable):
        async def wrapper(*args, **kwargs):
            return await RetryExecutor.execute_with_retry(func, *args, retry_policy=retry_policy, **kwargs)

        return wrapper

    return decorator
