# staking-income-calculator
Basic Repo to Calculate Staking Income

## Overview
Simple calculator that attempts to return total staking income in the provided tax year along with the USD-denominated amount.

### USD Prices
This code uses Coin Gecko's free public api to determine USD prices. The api returns the price of the token at 00:00 UTC time.
Note: The api call uses the free version, so there is only historical data for the last 365 days.


## Supported Assets

### Solana
There are 2 approaches. Either query the blockchain rpc node (public) to fetch inflation rewards (rate-limited), or download a csv of the reward events from SolScan.
- SolScan csv:
  - Looks for only staking rewards and matches the unix timestamp to the closes from the prices data.
- JSON-RPC Node (incomplete; needs ability to go from epoch to timestamps):
  - Directly fetches the inflation rewards for the delegator for each epoch. Since the public node is rate-limited, this approach takes longer.

- Usage:
```
python cmd.py --year 2024 --protocol solana --reward-file data\solana_delegator_rewards_2024.csv 
```
