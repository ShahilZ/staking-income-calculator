from datetime import datetime, timezone
from typing import Any, Dict, List
import aiohttp
import asyncio
import logging

from batch_requestor import BatchRequestor, with_batching
from reward import Reward

logger = logging.getLogger(__name__)

# SOL Mainnet epoch 100's first block: 2020-10-21 15:42:21
SOL_EPOCH_100_START_TIME = 1603254141
EPOCH_SECONDS = 2.5 * 24 * 3600

class Client:
  def __init__(self, base_url: str = "https://api.mainnet-beta.solana.com"):
    self.base_url = base_url
    self.batch_requestor = BatchRequestor(batch_size=38, cooldown=10)

  async def _make_request(self, method: str, params: List[Any]) -> Dict[str, Any]:
      """
      Make a request to the Solana RPC API with rate limiting.
      
      @param method: The RPC method to call
      @param params: The parameters for the RPC method
      @return: Dict[str, Any] The response from the RPC API
      """
      payload = {
          "jsonrpc": "2.0",
          "id": 1,
          "method": method,
          "params": params 
      }
      
      async def _request():
          async with aiohttp.ClientSession() as session:
              async with session.post(self.base_url, json=payload) as response:
                  return await response.json()
      
      return await with_batching(self.batch_requestor, _request)


  async def get_inflation_reward(self, address: str, epoch: int) -> Dict[str, Any]:
      """
      Get the inflation reward for a stake account in a specific epoch.
      
      @param (string) address: The stake account address
      @param (int) epoch: The epoch number
      @return: Dict[str, Any] The inflation reward data
      """
      return await self._make_request(method="getInflationReward", params=[[address], {"epoch": epoch}])
  
  async def get_inflation_rewards_for_epoch_range(self, address: str, start_epoch: int, end_epoch: int) -> List[Dict[str, Any]]:
      """
      Get inflation rewards for multiple stake accounts in multiple epochs.
      
      @param address: The stake account address
      @param start_epoch: The start epoch number
      @param end_epoch: The end epoch number
      @return: List[Dict[str, Any]] The list of inflation reward data
      """
      tasks = []
      for epoch in range(start_epoch, end_epoch + 1):      
          tasks.append(self.get_inflation_reward(address, epoch))
      return await asyncio.gather(*tasks)



  async def fetch_epoch_info(self, year: int):
      
      response = await self._make_request(method="getEpochInfo", params=[])
      logger.info(f"Current epoch info: {response}")
      current_epoch = response.get("result").get("epoch")

      # Estimate starting epoch based on start_time.
      start_time = datetime(year, 1, 1, tzinfo=timezone.utc).timestamp()
      end_time = datetime(year, 12, 31, tzinfo=timezone.utc).timestamp()

      seconds_since_baseline = start_time - SOL_EPOCH_100_START_TIME
      start_epoch = (seconds_since_baseline // EPOCH_SECONDS) + 100
      
      # Add some buffer in case estimates are off.
      start_epoch -= 14 # 12 epochs ~ 30 days.
      
      logger.info(f"Start epoch: {start_epoch}")
      logger.info(f"End epoch: {current_epoch}")

      return int(start_epoch), int(current_epoch)

  async def fetch_staking_rewards(self, address: str, year: int) -> List[Reward]:
      """
      Fetches the staking rewards for the given address using SolScan.
      """
      staking_rewards = []
      # Determine range of epochs to fetch.
      start_epoch, end_epoch = await self.fetch_epoch_info(year)
      resps = await self.get_inflation_rewards_for_epoch_range(address, start_epoch, end_epoch)
      for resp in resps:
        res = resp.get("result")
        if res.get("err"):
          logger.error(f"Error fetching staking rewards for {address}: {res.get('err')}")
        if res[0] == None:
          continue
        else:
          staking_rewards.extend(res[0])
      logger.info(f"Staking rewards for {address}: {staking_rewards}")
      # TODO: Sanity check the responses for errors.
      return staking_rewards


def compute_staking_rewards(rewards: List[Dict[str, Any]], year: int) -> List[Reward]:
  """
  Computes the staking rewards for the given address using SolScan's csv.
  """
  # Filter rewards to only include the given year.
  rewards = [r for r in rewards if datetime.fromtimestamp(int(r['Effective Time Unix']), tz=timezone.utc).year == year]

  # Filter for staking rewards (yes, the typo is intentional).
  rewards = [r for r in rewards if r['Rewad Type'] == 'Staking']

  # Convert to Reward objects.
  return [Reward(amount=float(r['Reward Amount']), timestamp=int(r['Effective Time Unix'])) for r in rewards]


