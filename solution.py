from dataclasses import dataclass


@dataclass
class Solution:
    nonce_hex: str
    hash_hex: str
    tries: int
