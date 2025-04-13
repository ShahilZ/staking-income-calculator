from datetime import datetime, timezone, timedelta
from decimal import Decimal
import logging
import requests
from typing import List, Dict, Any

from assets.cosmos import compute_staking_rewards as compute_cosmos_staking_rewards
from assets.solana import Client as SolanaClient, compute_staking_rewards as compute_solana_staking_rewards
from protocol import Protocol
from reward import Reward
from csv_loader import load_csv

logger = logging.getLogger(__name__)

def fetch_usd_protocol_price(protocol: Protocol, year: int):
    """
    Fetches the USD price of the protocol for the given year.
    """
    start_date = datetime(year, 1, 1, tzinfo=timezone.utc).timestamp()
    end_date = datetime(year, 12, 31, tzinfo=timezone.utc).timestamp()
    
    # Calculate the maximum allowed start date (365 days ago from now)
    current_date = datetime.now(timezone.utc)
    max_allowed_start_date = (current_date - timedelta(days=365)).timestamp()
    
    # Use the later of the two dates
    start_date = max(start_date, max_allowed_start_date)
    
    # If the start date is after the end date, we can't fetch data for this period
    if start_date > end_date:
        logger.info(f"Start date is after end date, skipping {protocol.value} for {year}")
        return None

    url = f"https://api.coingecko.com/api/v3/coins/{protocol.name}/market_chart/range?vs_currency=usd&from={start_date}&to={end_date}"
    response = requests.get(url)
    logger.info(f"Received response for historical prices: {response.status_code}")
    data = response.json()
   # Return sorted by timestamp.
    return sorted(data["prices"], key=lambda x: x[0])


async def fetch_staking_income_with_client(protocol: Protocol, year: int, address: str):
    """
    Fetches the staking income for the given protocol and year.
    """
    match (protocol):
        case Protocol.SOLANA:
            client = SolanaClient()
            return await client.fetch_staking_rewards(address, year)
        case _:
            raise ValueError(f"Unsupported protocol: {protocol}")


def fetch_staking_income_from_file(protocol: Protocol, year: int, reward_file: str) -> List[Dict[str, Any]]:
    """
    Computes the staking income for the given protocol and year.
    """
    logger.info(f"Computing staking income for {protocol.value} in {year}")
    rewards = load_csv(file_path=reward_file, has_header=True) 
    logger.info(f"Loaded {len(rewards)} rewards")
    match (protocol):
        case Protocol.SOLANA:
            return compute_solana_staking_rewards(rewards, year)
        case Protocol.COSMOS:
            return compute_cosmos_staking_rewards(rewards, year)
        case _:
            raise ValueError(f"Unsupported protocol: {protocol}")
    



async def calculate_staking_income(protocol: Protocol, year: int, address: str, reward_file: str) -> List[Reward]:
    """
    Calculates the staking income for the given protocol and year.
    """
    logger.info(f"Calculating staking income for {protocol.value} in {year}")
    logger.info(f"Fetching USD prices for {protocol.value}")
    prices = fetch_usd_protocol_price(protocol, year)

    logger.info(f"Fetching staking income for {protocol.value}")
    if reward_file:
        staking_income = fetch_staking_income_from_file(protocol, year, reward_file)
    else:
        assert address, "Address is required if reward_file is not provided"
        staking_income = await fetch_staking_income_with_client(protocol, year, address)

    # Compute USD value for each reward.
    for reward in staking_income:
        # Search for USD price closest to the reward timestamp.
        price = Reward.find_by_timestamp(prices, reward.timestamp)
        if price:
            reward.amount_usd = Decimal(price[1]) * Decimal(f"{reward.amount:.2f}")
        else:
          raise ValueError(f"No price found for {reward.timestamp}")

    return staking_income


def calculate_staking_income_without_daily_prices(protocol: Protocol, year: int, address: str, reward_file: str) -> List[Reward]:
    """
    Calculates the staking income for the given protocol and year. This function should be used if we don't need daily USD prices.
    """
    logger.info(f"Calculating staking income for {protocol.value} in {year}")
    logger.info(f"Fetching staking income for {protocol.value}")
    if not reward_file:
        raise ValueError("reward_file is required")
    staking_income = fetch_staking_income_from_file(protocol, year, reward_file)

    return staking_income

    

