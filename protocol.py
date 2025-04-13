from enum import Enum

class Protocol(Enum):

  SOLANA = "solana"
  COSMOS = "cosmos"

  def __str__(self):
      return self.value
  
    