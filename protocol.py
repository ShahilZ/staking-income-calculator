from enum import Enum

class Protocol(Enum):
    SOLANA = ("solana", "SOL")
    ETHEREUM = ("ethereum", "ETH")
    BITCOIN = ("bitcoin", "BTC")
    POLKADOT = ("polkadot", "DOT")
    CARDANO = ("cardano", "ADA")
    COSMOS = ("cosmos", "ATOM")
    
    def __init__(self, name: str, ticker: str):
        self._name = name
        self._ticker = ticker
    
    @property
    def name(self) -> str:
        return self._name
    
    @property
    def ticker(self) -> str:
        return self._ticker
    
    def __str__(self) -> str:
        return f"{self.name} ({self.ticker})"

    @classmethod
    def from_name(cls, name: str) -> 'Protocol':
        """Create a Protocol from its name."""
        name = name.lower()
        for protocol in cls:
            if protocol.name == name:
                return protocol
        raise ValueError(f"Invalid protocol name: {name}")

