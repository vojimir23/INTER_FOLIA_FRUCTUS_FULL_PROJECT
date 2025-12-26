
import threading
from typing import Dict, Any

class Store:
    """
    A thread-safe application store for caches and readiness state.
    """
    def __init__(self):
        self.cache: Dict[str, Any] = {}
        self.lock = threading.Lock()
        self.is_ready: bool = False

    def swap_cache(self, new_cache: Dict[str, Any]) -> None:
        """
        Atomically clears and updates the cache under a lock and sets the
        application state to ready.
        """
        with self.lock:
            self.cache.clear()
            self.cache.update(new_cache)
            self.is_ready = True

    def mark_loading(self) -> None:
        """
        Sets the application state to not ready (loading).
        """
        with self.lock:
            self.is_ready = False

# Export a singleton instance for global use.
store = Store()