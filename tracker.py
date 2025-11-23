import os.path
import threading
from datetime import datetime
from enum import Enum, auto
from typing import Iterable, Optional

from peewee import CompositeKey, DateTimeField, IntegerField, Model, SqliteDatabase, TextField
from playhouse.sqliteq import SqliteQueueDatabase

from challenge import Challenge
from project import Project
from solution import Solution
from utils import parse_iso8601_to_utc_naive

# db = SqliteQueueDatabase(
db = SqliteDatabase(
    None,
    pragmas={
        'journal_mode': 'wal',
        'busy_timeout': 30_000,
        },
    timeout=30.0,
    # autostart=False,
    # queue_max_size=64,
    # results_timeout=10.0,
    )

db_lock = threading.Lock()


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
    Solving = auto()
    Validated = auto()
    Invalid = auto()


class WorkModel(BaseModel):
    address: str = TextField()
    challenge_id: str = TextField()
    status: WorkStatus = TextField(choices=[(s.value, s.name) for s in WorkStatus])

    class Meta:
        primary_key = CompositeKey('address', 'challenge_id')


class SolutionStatus(Enum):
    Found = auto()
    Validated = auto()
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
        # db.start()
        db.connect(reuse_if_open=True)
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
            with db_lock:
                WalletModel.create(address=address)
            # endwith

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
            with db_lock:
                ChallengeModel.create(challenge_id=challenge.challenge_id,
                                      day=challenge.day,
                                      challenge_number=challenge.challange_number,
                                      difficulty=challenge.difficulty,
                                      no_pre_mine=challenge.no_pre_mine,
                                      no_pre_mine_hour=challenge.no_pre_mine_hour,
                                      latest_submission=challenge.latest_submission,
                                      latest_submission_dt=parse_iso8601_to_utc_naive(challenge.latest_submission)
                                      )
            # endwith

            return True
        # endif
    # enddef

    def get_challenge_model(self, challenge_id: str) -> Optional[ChallengeModel]:
        return ChallengeModel.select().where(
            ChallengeModel.challenge_id == challenge_id
            ).first()
    # enddef

    def challenge_exists(self, challenge: Challenge) -> bool:
        return ChallengeModel.select().where(
            ChallengeModel.challenge_id == challenge.challenge_id
            ).exists()
    # enddef

    def _query_challenge_models(self, address: str, list__status: list[WorkStatus]) -> Iterable[ChallengeModel]:
        ignore_challenge_id = (
            WorkModel
            .select(WorkModel.challenge_id)
            .where(
                (WorkModel.address == address) &
                (WorkModel.status.in_([ws.value for ws in WorkStatus if ws not in list__status]))
                )
        )

        query = (
            ChallengeModel
            .select()
            .where(
                (ChallengeModel.challenge_id.not_in(ignore_challenge_id)) &
                (Challenge.is_valid_dt(ChallengeModel.latest_submission_dt))
                )
            .order_by(ChallengeModel.latest_submission_dt.asc())
        )

        return query
    # enddef

    def get_challenges(self, address: str, list__status: list[WorkStatus]) -> list[Challenge]:
        list__challenge_models = self._query_challenge_models(address=address, list__status=list__status)

        list__challenge = []
        for cm in list__challenge_models:
            list__challenge.append(Challenge.from_challenge_model(challenge_model=cm))
        # endfor

        return list__challenge
    # enddef

    def get_oldest_unsolved_challenge(self, address: str) -> Optional[Challenge]:
        cm = self._query_challenge_models(address=address, list__status=[status for status in WorkStatus if status != WorkStatus.Validated]).first()

        if cm is None:
            return None
        else:
            return Challenge.from_challenge_model(cm)
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
        with db_lock:
            WorkModel.create(address=address, challenge_id=challenge.challenge_id, status=WorkStatus.Open.value)
        # endwith
    # enddef

    def update_work(self, address: str, challenge: Challenge, status: WorkStatus):
        with db_lock:
            (WorkModel
             .update(status=status.value)
             .where(
                (WorkModel.address == address) &
                (WorkModel.challenge_id == challenge.challenge_id)
                )
             .execute())
        # endwith
    # enddef

    def get_solving_challenge(self, address: str) -> Optional[Challenge]:
        work_solving = (
            WorkModel
            .select()
            .where(
                (WorkModel.address == address) &
                (WorkModel.status == WorkStatus.Solving.value)
                )
            .first()
        )  # type: WorkModel

        if work_solving:
            challenge_id = work_solving.challenge_id
            cm = self.get_challenge_model(challenge_id=challenge_id)

            return Challenge.from_challenge_model(challenge_model=cm)
        else:
            return None
        # endif
    # enddef

    # -------------------------
    # solution
    # -------------------------
    def add_solution_found(self, address: str, challenge: Challenge, solution: Solution):
        with db_lock:
            SolutionModel.create(address=address, challenge_id=challenge.challenge_id, nonce_hex=solution.nonce_hex, hash_hex=solution.hash_hex, tries=solution.tries,
                                 status=SolutionStatus.Found.value)
        # endwith
    # enddef

    def update_solution(self, address: str, challenge: Challenge, solution: Solution, status: SolutionStatus):
        with db_lock:
            (SolutionModel
             .update(status=status.value)
             .where(
                (SolutionModel.address == address) &
                (SolutionModel.challenge_id == challenge.challenge_id) &
                (SolutionModel.nonce_hex == solution.nonce_hex)
                )
             .execute())
        # endwith
    # enddef

    def get_found_solution(self, address: str, challenge: Challenge) -> Optional[Solution]:
        sm = (SolutionModel
              .select()
              .where(
            (SolutionModel.address == address) &
            (SolutionModel.challenge_id == challenge.challenge_id) &
            (SolutionModel.status == SolutionStatus.Found.value)
            )
              .first())  # type: SolutionModel

        if sm:
            return Solution(nonce_hex=sm.nonce_hex, hash_hex=sm.hash_hex, tries=sm.tries)
        else:
            return None
        # endif
    # enddef
