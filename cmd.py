import click
import asyncio
import logging

from protocol import Protocol
from calculator import calculate_staking_income, calculate_staking_income_without_daily_prices

# Configure logging with a custom format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

@click.command()
@click.option('--year', type=int, required=True, help='Tax year for which to calculate the staking income.')
@click.option('--protocol', type=str, required=True, help='Staking protocol.')
@click.option('--address', type=str, required=False, help='Address of the staking wallet.')
@click.option('--reward-file', type=str, required=False, help='File containing the reward data.')
@click.option('--no-prices', is_flag=True, required=False, help='Calculate staking income without fetching daily prices.')
def main(year, protocol, address, reward_file, no_prices):
    """A simple command-line tool using Click."""
    protocol = Protocol.from_name(protocol)
    if not address and not reward_file:
        raise ValueError("Either address or reward-file must be provided.")
    logger.info(f"Calculating {protocol} staking income for {address} for {year}.")
    
    if no_prices:
        staking_income = calculate_staking_income_without_daily_prices(protocol, year, address, reward_file)
    else:
        staking_income = calculate_staking_income(protocol, year, address, reward_file)

    # Sum the total tokens and usd amount.
    total_tokens = sum(reward.amount for reward in staking_income)
    total_usd = sum(reward.amount_usd for reward in staking_income)
    logger.info(f"Total tokens: {total_tokens} {protocol.ticker}")
    logger.info(f"Total USD: {total_usd:.2f}")

if __name__ == '__main__':
    main() 