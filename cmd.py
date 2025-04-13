import click
import asyncio
import logging

from protocol import Protocol
from calculator import calculate_staking_income

# Configure logging with a custom format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

@click.command()
@click.option('--year', '-y', type=int, required=True, help='Tax year for which to calculate the staking income.')
@click.option('--protocol', '-p', type=Protocol, required=True, help='Staking protocol.')
@click.option('--address', '-a', type=str, required=False, help='Address of the staking wallet.')
@click.option('--reward-file', '-r', type=str, required=False, help='File containing the reward data.')
def main(year, protocol, address, reward_file):
    """A simple command-line tool using Click."""
    if not address and not reward_file:
        raise ValueError("Either address or reward-file must be provided.")
    logger.info(f"Calculating {protocol} staking income for {address} for {year}.")
    
    # Run the async function from synchronous code
    staking_income = asyncio.run(calculate_staking_income(protocol, year, address, reward_file))
    logger.info(f"Staking income: {staking_income}")

    # Sum the total tokens and usd amount.
    total_tokens = sum(reward.amount for reward in staking_income)
    total_usd = sum(reward.amount_usd for reward in staking_income)
    logger.info(f"Total tokens: {total_tokens}")
    logger.info(f"Total usd: {total_usd:.2f}")

if __name__ == '__main__':
    main() 