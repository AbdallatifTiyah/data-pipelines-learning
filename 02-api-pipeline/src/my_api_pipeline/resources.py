from __future__ import annotations
import os
import requests
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass
class HttpClient:
    timeout: int = int(os.getenv("REQUEST_TIMEOUT", "30"))

    def get_json(self, url: str):
        r = requests.get(url, timeout=self.timeout)
        r.raise_for_status()
        return r.json()
