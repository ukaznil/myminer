import os.path
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Optional

from peewee import CompositeKey, DateTimeField, IntegerField, Model, SqliteDatabase, TextField

from challenge import Challenge
from project import Project
from solution import Solution
from utils import parse_iso8601_to_utc_naive

db = SqliteDatabase(None)


class BaseModel(Model):
    class Meta:
        database = db


class WalletModel(BaseModel):
    address: str = TextField(primary_key=True)


class ChallengeModel(BaseModel):
    challenge_id: str = TextField(primary_key=True)
    day: int = IntegerField()
    challenge_number: int = IntegerField()
    difficulty: str = TextField()
    no_pre_mine: str = TextField()
    no_pre_mine_hour: str = TextField()
    latest_submission: str = TextField()
    latest_submission_dt: datetime = DateTimeField()


class Status(Enum):
    Open = auto()
    Working = auto()
    Solved = auto()
    Invalid = auto()


class WorkModel(BaseModel):
    address: str = TextField()
    challenge_id: str = TextField()
    status: Status = TextField(choices=[(s.value, s.name) for s in Status])

    class Meta:
        primary_key = CompositeKey('address', 'challenge_id')


class SolutionModel(BaseModel):
    address: str = TextField()
    challenge_id: str = TextField()
    nonce_hex: str = TextField()
    hash_hex: str = TextField()
    tries: int = IntegerField()


class Tracker:
    def __init__(self, project: Project):
        db_name = os.path.join('db', f'{project.name.lower()}.sqlite3')
        db.init(db_name)
        db.connect()
        db.create_tables([WalletModel, ChallengeModel, WorkModel, SolutionModel])
    # enddef

    def add_wallet(self, address: str) -> bool:
        if self.wallet_exists(address):
            return False
        else:
            WalletModel.create(address=address)
            return True
        # endif
    # enddef

    def wallet_exists(self, addresss: str) -> bool:
        return WalletModel.select().where(
            WalletModel.address == addresss
            ).exists()
    # enddef

    def get_wallets(self, num: int) -> list[str]:
        wallets = WalletModel.select()
        if num > 0:
            wallets = wallets.limit(num)
        # endif

        return [wallet.address for wallet in wallets]
    # enddef

    def add_challenge(self, challenge: Challenge) -> bool:
        if self.challenge_exists(challenge):
            return False
        else:
            ChallengeModel.create(challenge_id=challenge.challenge_id,
                                  day=challenge.day,
                                  challenge_number=challenge.challange_number,
                                  difficulty=challenge.difficulty,
                                  no_pre_mine=challenge.no_pre_mine,
                                  no_pre_mine_hour=challenge.no_pre_mine_hour,
                                  latest_submission=challenge.latest_submission,
                                  latest_submission_dt=parse_iso8601_to_utc_naive(challenge.latest_submission)
                                  )
            return True
        # endif
    # enddef

    def challenge_exists(self, challenge: Challenge) -> bool:
        return ChallengeModel.select().where(
            ChallengeModel.challenge_id == challenge.challenge_id
            ).exists()
    # enddef

    def get_open_challenges(self, address: str) -> list[Challenge]:
        solved_challenge_id = (
            WorkModel
            .select(WorkModel.challenge_id)
            .where(
                (WorkModel.address == address) &
                (WorkModel.status == Status.Solved.value)
                )
        )

        query = (
            ChallengeModel
            .select()
            .where(
                (ChallengeModel.challenge_id.not_in(solved_challenge_id)) &
                (ChallengeModel.latest_submission_dt >= datetime.utcnow() + timedelta(seconds=10))
                )
            .order_by(ChallengeModel.latest_submission_dt.asc())
        )

        list__challenge = []
        for challenge in query:  # type: ChallengeModel
            list__challenge.append(Challenge({
                'challenge_id': challenge.challenge_id,
                'day': challenge.day,
                'challenge_number': challenge.challenge_number,
                'difficulty': challenge.difficulty,
                'no_pre_mine': challenge.no_pre_mine,
                'no_pre_mine_hour': challenge.no_pre_mine_hour,
                'latest_submission': challenge.latest_submission,
                }))
        # endfor

        return list__challenge
    # enddef

    def get_oldtest_open_challenge(self, address: str) -> Optional[Challenge]:
        solved_challenge_id = (
            WorkModel
            .select(WorkModel.challenge_id)
            .where(
                (WorkModel.address == address) &
                (WorkModel.status == Status.Solved.value)
                )
        )

        query = (
            ChallengeModel
            .select()
            .where(
                (ChallengeModel.challenge_id.not_in(solved_challenge_id)) &
                (ChallengeModel.latest_submission_dt >= datetime.utcnow() + timedelta(seconds=10))
                )
            .order_by(ChallengeModel.latest_submission_dt.asc())
        )
        challenge = query.first()
        if challenge is None:
            return None
        else:
            return Challenge({
                'challenge_id': challenge.challenge_id,
                'day': challenge.day,
                'challenge_number': challenge.challenge_number,
                'difficulty': challenge.difficulty,
                'no_pre_mine': challenge.no_pre_mine,
                'no_pre_mine_hour': challenge.no_pre_mine_hour,
                'latest_submission': challenge.latest_submission,
                })
        # endif
    # enddef

    def work_exists(self, address: str, challenge: Challenge) -> bool:
        return WorkModel.select().where(
            (WorkModel.address == address) & (WorkModel.challenge_id == challenge.challenge_id)
            ).exists()

    def add_work(self, address: str, challenge: Challenge):
        WorkModel.create(address=address, challenge_id=challenge.challenge_id, status=Status.Open.value)
    # enddef

    def update_work(self, address: str, challenge: Challenge, status: Status):
        work = WorkModel.get((WorkModel.address == address) & (WorkModel.challenge_id == challenge.challenge_id))  # type: WorkModel
        if work:
            work.status = status.value
            work.save()
        # endif
    # enddef

    def add_solution(self, address: str, challenge: Challenge, solution: Solution):
        SolutionModel.create(address=address, challenge_id=challenge.challenge_id, nonce_hex=solution.nonce_hex, hash_hex=solution.hash_hex, tries=solution.tries)
    # enddef
