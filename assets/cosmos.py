from typing import List, Dict, Any
from datetime import datetime, timezone
from decimal import Decimal

from reward import Reward


def compute_staking_rewards(rewards: List[Dict[str, Any]], year: int) -> List[Reward]:
    """
    Computes the staking rewards for the given address using MintScan's csv.
    """
    # Filter rewards to only include the given year.
    rewards = [r for r in rewards if datetime.strptime(r['timestamp'], '%Y-%m-%d %H:%M:%S').year == year]

    # Filter for claimed rewards.
    rewards = [r for r in rewards if r['type'] == 'GetReward']

    # Filter only for uatom rewards.
    rewards = [r for r in rewards if r['denom'] == 'uatom']

    # Convert to Reward objects.
    return [Reward(amount=float(r['amount']), timestamp=int(datetime.strptime(r['timestamp'], '%Y-%m-%d %H:%M:%S').timestamp()), amount_usd=Decimal(r['totalPrice'])) for r in rewards]


