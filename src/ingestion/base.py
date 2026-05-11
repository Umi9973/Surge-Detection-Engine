from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Iterator


class DataIngestor(ABC):
    @abstractmethod
    def stream(self) -> Iterator[Dict]:
        raise NotImplementedError
