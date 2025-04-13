from dataclasses import dataclass, field
from decimal import Decimal
from typing import List, Optional
import bisect
import logging

logger = logging.getLogger(__name__)

@dataclass
class Reward:
    amount: float
    timestamp: int
    amount_usd: Decimal = field(default_factory=Decimal)
    
    @staticmethod
    def find_by_timestamp(prices: List[tuple], target_timestamp: int) -> Optional[tuple]:
        """
        Find a reward with the closest timestamp using binary search.
        
        Args:
            prices: List of prices sorted by timestamp
            target_timestamp: The timestamp to search for
            
        Returns:
            The reward with the closest timestamp, or None if no rewards exist
        """
        if not prices:
            return None
        logger.info(f"Finding reward by timestamp: {target_timestamp}")
        logger.info(f"Prices: {prices}")

        # Sort prices by timestamp if not already sorted
        sorted_prices = sorted(prices, key=lambda p: p[0])
        
        # Find the index where the target timestamp would be inserted
        idx = bisect.bisect_left([p[0] for p in sorted_prices], target_timestamp)
        
        # Handle edge cases
        if idx == 0:
            return sorted_prices[0]
        if idx == len(sorted_prices):
            return sorted_prices[-1]
            
        # Find the closest timestamp
        left = sorted_prices[idx-1]
        right = sorted_prices[idx]
        
        if abs(left[0] - target_timestamp) <= abs(right[0] - target_timestamp):
            return left
        return right
