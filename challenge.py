from dataclasses import dataclass
from datetime import datetime, timedelta

from utils import parse_iso8601_to_utc_naive


@dataclass
class Challenge:
    challenge_id: str  # "**D21C10" など
    day: int
    challange_number: int
    difficulty: str  # 4 バイト hex マスク (例 "0000FFFF")
    no_pre_mine: str  # 長い hex 文字列
    no_pre_mine_hour: str  # 10 桁くらいの数字文字列
    latest_submission: str  # ISO8601, "2025-10-30T23:59:59Z" など

    def __init__(self, ch: dict):
        self.challenge_id = ch['challenge_id']
        self.day = ch.get('day')
        self.challange_number = ch.get("challenge_number")
        self.difficulty = ch['difficulty']
        self.no_pre_mine = ch['no_pre_mine']
        self.no_pre_mine_hour = ch['no_pre_mine_hour']
        self.latest_submission = ch['latest_submission']
    # enddef

    def is_valid(self) -> bool:
        latest_submission_dt = parse_iso8601_to_utc_naive(self.latest_submission)

        return latest_submission_dt >= datetime.utcnow() + timedelta(seconds=10)
    # enddef

    def __repr__(self) -> str:
        return '\n'.join([f'challenge_id:       {self.challenge_id}',
                          f'day/ch#:            {self.day}/{self.challange_number}',
                          f'difficulty (hex):   {self.difficulty}',
                          f'no_pre_mine:        {self.no_pre_mine}',
                          f'no_pre_mine_hour:   {self.no_pre_mine_hour}',
                          f'latest_submission:  {self.latest_submission}',
                          ])
    # enddef
