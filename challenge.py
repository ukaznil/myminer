from dataclasses import dataclass, field, fields
from datetime import datetime, timedelta

from utils import parse_iso8601_to_utc_naive


@dataclass
class Challenge:
    challenge_id: str  # "**D21C10" など
    day: int
    challenge_number: int
    difficulty: str  # 4 バイト hex マスク (例 "0000FFFF")
    no_pre_mine: str  # 長い hex 文字列
    no_pre_mine_hour: str  # 10 桁くらいの数字文字列
    latest_submission: str  # ISO8601, "2025-10-30T23:59:59Z" など
    latest_submission_dt: datetime = field(init=False, repr=False)

    def __post_init__(self):
        self.latest_submission_dt = parse_iso8601_to_utc_naive(self.latest_submission)
    # enddef

    def is_valid(self) -> bool:
        return self.is_valid_dt(self.latest_submission_dt)
    # enddef

    @staticmethod
    def is_valid_dt(latest_submission_dt: datetime) -> bool:
        return latest_submission_dt >= datetime.utcnow() + timedelta(seconds=10)
    # enddef

    @classmethod
    def from_dict(cls, data: dict) -> 'Challenge':
        valid_names = {f.name for f in fields(cls)}
        filtered = {k: v for k, v in data.items() if k in valid_names}

        return cls(**filtered)
    # enddef

    @classmethod
    def from_challenge_model(cls, challenge_model) -> 'Challenge':
        return cls(
            challenge_id=challenge_model.challenge_id,
            day=challenge_model.day,
            challenge_number=challenge_model.challenge_number,
            difficulty=challenge_model.difficulty,
            no_pre_mine=challenge_model.no_pre_mine,
            no_pre_mine_hour=challenge_model.no_pre_mine_hour,
            latest_submission=challenge_model.latest_submission,
            )
    # enddef

    def __repr__(self) -> str:
        return '\n'.join([f'challenge_id:       {self.challenge_id}',
                          f'day/ch#:            {self.day}/{self.challenge_number}',
                          f'difficulty (hex):   {self.difficulty}',
                          f'no_pre_mine:        {self.no_pre_mine}',
                          f'no_pre_mine_hour:   {self.no_pre_mine_hour}',
                          f'latest_submission:  {self.latest_submission}',
                          ])
    # enddef
