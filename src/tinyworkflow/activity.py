"""
Activity decorator for defining reusable workflow tasks.
"""

import asyncio
import inspect
from functools import wraps
from typing import Callable, Optional, Any, Dict
from dataclasses import dataclass

from tinyworkflow.retry import RetryPolicy


@dataclass
class ActivityMetadata:
    """Metadata for an activity."""

    name: str
    retry_policy: Optional[RetryPolicy]
    timeout: Optional[float]
    is_async: bool


# Registry of all activities
_activity_registry: Dict[str, Callable] = {}
_activity_metadata: Dict[str, ActivityMetadata] = {}


def activity(
    name: Optional[str] = None,
    retry_policy: Optional[RetryPolicy] = None,
    timeout: Optional[float] = None,
):
    """
    Decorator to define a workflow activity.

    Activities are reusable tasks that can be called within workflows.
    They support automatic retries, timeouts, and state persistence.

    Args:
        name: Activity name (defaults to function name)
        retry_policy: Retry policy for this activity
        timeout: Timeout in seconds for activity execution

    Example:
        @activity(name="fetch_data", retry_policy=RetryPolicy(max_retries=5))
        async def fetch_from_api(url: str):
            # Fetch data from API
            return data
    """

    def decorator(func: Callable) -> Callable:
        activity_name = name or func.__name__
        is_async = asyncio.iscoroutinefunction(func)

        # Store metadata
        _activity_metadata[activity_name] = ActivityMetadata(
            name=activity_name,
            retry_policy=retry_policy or RetryPolicy(),
            timeout=timeout,
            is_async=is_async,
        )

        # Store in registry
        _activity_registry[activity_name] = func

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            """Async wrapper for activities."""
            if timeout:
                try:
                    return await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)
                except asyncio.TimeoutError:
                    raise TimeoutError(
                        f"Activity '{activity_name}' timed out after {timeout} seconds"
                    )
            else:
                return await func(*args, **kwargs)

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            """Sync wrapper for activities."""
            return func(*args, **kwargs)

        # Mark the function as an activity
        wrapper = async_wrapper if is_async else sync_wrapper
        wrapper.__activity_name__ = activity_name
        wrapper.__is_activity__ = True
        wrapper.__activity_metadata__ = _activity_metadata[activity_name]

        return wrapper

    return decorator


def get_activity(name: str) -> Optional[Callable]:
    """Get an activity by name from the registry."""
    return _activity_registry.get(name)


def get_activity_metadata(name: str) -> Optional[ActivityMetadata]:
    """Get activity metadata by name."""
    return _activity_metadata.get(name)


def list_activities() -> list[str]:
    """List all registered activity names."""
    return list(_activity_registry.keys())
