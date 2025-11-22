import os.path
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Optional

from peewee import CompositeKey, DateTimeField, IntegerField, Model, SqliteDatabase, TextField

from challenge import Challenge
from project import Project
from solution import Solution
from utils import parse_iso8601_to_utc_naive

db = SqliteDatabase(
    None,
    pragmas={
        'journal_mode': 'wal',
        'busy_timeout': 5000,
        },
    timeout=5.0,
    )


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


class WorkStatus(Enum):
    Open = auto()
    Working = auto()
    Solved = auto()
    Invalid = auto()


class WorkModel(BaseModel):
    address: str = TextField()
    challenge_id: str = TextField()
    status: WorkStatus = TextField(choices=[(s.value, s.name) for s in WorkStatus])

    class Meta:
        primary_key = CompositeKey('address', 'challenge_id')


class SolutionStatus(Enum):
    Found = auto()
    Verified = auto()
    Invalid = auto()


class SolutionModel(BaseModel):
    address: str = TextField()
    challenge_id: str = TextField()
    nonce_hex: str = TextField()
    hash_hex: str = TextField()
    tries: int = IntegerField()
    status: SolutionStatus = TextField(choices=[(s.value, s.name) for s in SolutionStatus])


class Tracker:
    def __init__(self, project: Project):
        db_name = os.path.join('db', f'{project.name.lower()}.sqlite3')
        db.init(db_name)
        db.connect()
        db.create_tables([WalletModel, ChallengeModel, WorkModel, SolutionModel])

        self.db = db
    # enddef

    # -------------------------
    # wallet
    # -------------------------
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

    # -------------------------
    # challenge
    # -------------------------
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
                (WorkModel.status == WorkStatus.Solved.value)
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
                (WorkModel.status == WorkStatus.Solved.value)
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

    # -------------------------
    # work
    # -------------------------
    def work_exists(self, address: str, challenge: Challenge) -> bool:
        return WorkModel.select().where(
            (WorkModel.address == address) &
            (WorkModel.challenge_id == challenge.challenge_id)
            ).exists()

    def add_work(self, address: str, challenge: Challenge):
        WorkModel.create(address=address, challenge_id=challenge.challenge_id, status=WorkStatus.Open.value)
    # enddef

    def update_work(self, address: str, challenge: Challenge, status: WorkStatus):
        work = WorkModel.get(
            (WorkModel.address == address) &
            (WorkModel.challenge_id == challenge.challenge_id)
            )  # type: WorkModel
        if work:
            work.status = status.value
            work.save()
        # endif
    # enddef

    def get_num_work(self, address: str, status: WorkStatus) -> int:
        num = (
            WorkModel
            .select()
            .where(
                (WorkModel.address == address) &
                (WorkModel.status == status.value)
                )
            .count()
        )

        return num
    # enddef

    # -------------------------
    # solution
    # -------------------------
    def add_solution_found(self, address: str, challenge: Challenge, solution: Solution):
        SolutionModel.create(address=address, challenge_id=challenge.challenge_id, nonce_hex=solution.nonce_hex, hash_hex=solution.hash_hex, tries=solution.tries,
                             status=SolutionStatus.Found.value)
    # enddef

    def update_solution(self, address: str, challenge: Challenge, solution: Solution, status: SolutionStatus):
        solution = SolutionModel.get(
            (SolutionModel.address == address) &
            (SolutionModel.challenge_id == challenge.challenge_id) &
            (SolutionModel.nonce_hex == solution.nonce_hex)
            )  # type: SolutionModel
        if solution:
            solution.status = status.value
            solution.save()
        # endif
    # enddef

    def get_found_solution(self, address: str, challenge: Challenge) -> Optional[Solution]:
        list__solution = (
            SolutionModel
            .select()
            .where(
                (SolutionModel.address == address) &
                (SolutionModel.challenge_id == challenge.challenge_id) &
                (SolutionModel.status == SolutionStatus.Found.value)
                ))

        return list__solution.first()
