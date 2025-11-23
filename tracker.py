import os.path
import threading
from datetime import datetime
from enum import Enum, auto
from typing import Iterable, Optional

from peewee import CompositeKey, DateTimeField, IntegerField, JOIN, Model, SqliteDatabase, TextField

from challenge import Challenge
from logger import Logger, measure_time
from project import Project
from solution import Solution
from utils import assert_type, parse_iso8601_to_utc_naive

# db = SqliteQueueDatabase(
db = SqliteDatabase(
    None,
    pragmas={
        'journal_mode': 'wal',
        'synchronous': 'normal',
        'busy_timeout': 30_000,  # ms
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

    class Meta:
        indexes = (
            (('day', 'challenge_number'), False),  # 必要なら
            (('latest_submission_dt',), False),  # order_by で頻繁に使うなら
            )
    # endclass


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
        indexes = (
            (('address', 'status'), False),
            )


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

    class Meta:
        indexes = (
            # update_solution / delete_solution などで使用
            (('address', 'challenge_id', 'nonce_hex'), False),
            # get_found_solution 用
            (('address', 'challenge_id', 'status'), False),
            )
    # endclass


class Tracker:
    @measure_time
    def __init__(self, project: Project, logger: Logger):
        assert_type(project, Project)

        db_name = os.path.join('db', f'{project.name.lower()}.sqlite3')
        db.init(db_name)
        # db.start()
        db.connect(reuse_if_open=True)
        db.create_tables([WalletModel, ChallengeModel, WorkModel, SolutionModel])

        self.db = db
        self.logger = logger

        self._ensure_indexes()
    # enddef

    def _ensure_indexes(self):
        # SolutionModel
        self.db.execute_sql("""
        CREATE INDEX IF NOT EXISTS idx_solution_address_challenge_nonce
        ON solutionmodel (address, challenge_id, nonce_hex);
        """)
        self.db.execute_sql("""
        CREATE INDEX IF NOT EXISTS idx_solution_address_challenge_status
        ON solutionmodel (address, challenge_id, status);
        """)

        # WorkModel
        self.db.execute_sql("""
        CREATE INDEX IF NOT EXISTS idx_work_address_status
        ON workmodel (address, status);
        """)

        # ChallengeModel
        self.db.execute_sql("""
        CREATE INDEX IF NOT EXISTS idx_challenge_latest_submission_dt
        ON challengemodel (latest_submission_dt);
        """)
    # enddef

    @measure_time
    def close(self):
        if not self.db.is_closed():
            self.db.close()
        # endif
    # enddef

    # -------------------------
    # wallet
    # -------------------------
    @measure_time
    def add_wallet(self, address: str) -> bool:
        assert_type(address, str)

        q = (
            WalletModel
            .insert(
                address=address
                )
            .on_conflict_ignore()
        )

        with db_lock:
            inserted = q.execute()
        # endwith

        return bool(inserted)
    # enddef

    @measure_time
    def get_wallets(self, num: Optional[int]) -> list[str]:
        assert_type(num, int, allow_none=True)

        wallets = WalletModel.select()
        if num and num >= 0:
            wallets = wallets.limit(num)
        # endif

        return [wallet.address for wallet in wallets]
    # enddef

    # -------------------------
    # challenge
    # -------------------------
    @measure_time
    def add_challenge(self, challenge: Challenge) -> bool:
        assert_type(challenge, Challenge)

        q = (
            ChallengeModel
            .insert(
                challenge_id=challenge.challenge_id,
                day=challenge.day,
                challenge_number=challenge.challenge_number,
                difficulty=challenge.difficulty,
                no_pre_mine=challenge.no_pre_mine,
                no_pre_mine_hour=challenge.no_pre_mine_hour,
                latest_submission=challenge.latest_submission,
                latest_submission_dt=parse_iso8601_to_utc_naive(challenge.latest_submission)
                )
            .on_conflict_ignore()
        )

        with db_lock:
            inserted = q.execute()
        # endwith

        return bool(inserted)
    # enddef

    @measure_time
    def get_challenge_model(self, challenge_id: str) -> Optional[ChallengeModel]:
        assert_type(challenge_id, str)

        return ChallengeModel.select().where(
            ChallengeModel.challenge_id == challenge_id
            ).first()
    # enddef

    @measure_time
    def _query_challenge_models(self, address: str, list__status: list[WorkStatus]) -> Iterable[ChallengeModel]:
        assert_type(address, str)
        assert_type(list__status, list, WorkStatus)

        allowed_status_values = [ws.value for ws in list__status]
        WorkAlias = WorkModel.alias()

        query = (
            ChallengeModel
            .select(ChallengeModel)
            .join(
                WorkAlias,
                JOIN.LEFT_OUTER,
                on=(
                        (WorkAlias.challenge_id == ChallengeModel.challenge_id) &
                        (WorkAlias.address == address)
                ),
                )
            .where(
                # Work が無い（まだ一度も着手していない）か、
                (WorkAlias.challenge_id.is_null(True)) |
                # or status が許可リストに入っている
                (WorkAlias.status.in_(allowed_status_values))
                )
            .where(Challenge.is_valid_dt(ChallengeModel.latest_submission_dt))
            .order_by(ChallengeModel.latest_submission_dt.asc())
        )

        return query
    # enddef

    @measure_time
    def get_challenges(self, address: str, list__status: list[WorkStatus]) -> list[Challenge]:
        assert_type(address, str)
        assert_type(list__status, list, WorkStatus)

        list__challenge_models = self._query_challenge_models(address=address, list__status=list__status)

        list__challenge = []
        for cm in list__challenge_models:
            list__challenge.append(Challenge.from_challenge_model(challenge_model=cm))
        # endfor

        return list__challenge
    # enddef

    @measure_time
    def get_oldest_unsolved_challenge(self, address: str) -> Optional[Challenge]:
        assert_type(address, str)

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
    @measure_time
    def add_work(self, address: str, challenge: Challenge, status: WorkStatus) -> bool:
        assert_type(address, str)
        assert_type(challenge, Challenge)

        q = (
            WorkModel
            .insert(
                address=address,
                challenge_id=challenge.challenge_id,
                status=status.value,
                )
            .on_conflict_replace()
        )

        with db_lock:
            inserted = q.execute()
        # endwith

        return bool(inserted)
    # enddef

    @measure_time
    def update_work(self, address: str, challenge: Challenge, status: WorkStatus):
        assert_type(address, str)
        assert_type(challenge, Challenge)
        assert_type(status, WorkStatus)

        q = (
            WorkModel
            .update(status=status.value)
            .where(
                (WorkModel.address == address) &
                (WorkModel.challenge_id == challenge.challenge_id)
                )
        )

        with db_lock:
            q.execute()
        # endwith
    # enddef

    @measure_time
    def get_solving_challenge(self, address: str) -> Optional[Challenge]:
        assert_type(address, str)

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
    @measure_time
    def add_solution_found(self, address: str, challenge: Challenge, solution: Solution):
        assert_type(address, str)
        assert_type(challenge, Challenge)
        assert_type(solution, Solution)

        data = dict(
            address=address,
            challenge_id=challenge.challenge_id,
            nonce_hex=solution.nonce_hex,
            hash_hex=solution.hash_hex,
            tries=solution.tries,
            status=SolutionStatus.Found.value,
            )

        with db_lock:
            SolutionModel.create(**data)
        # endwith
    # enddef

    @measure_time
    def update_solution(self, address: str, challenge: Challenge, solution: Solution, status: SolutionStatus):
        assert_type(address, str)
        assert_type(challenge, Challenge)
        assert_type(solution, Solution)
        assert_type(status, SolutionStatus)

        q = (
            SolutionModel
            .update(status=status.value)
            .where(
                (SolutionModel.address == address) &
                (SolutionModel.challenge_id == challenge.challenge_id) &
                (SolutionModel.nonce_hex == solution.nonce_hex)
                )
        )

        with db_lock:
            q.execute()
        # endwith
    # enddef

    @measure_time
    def get_found_solution(self, address: str, challenge: Challenge) -> Optional[Solution]:
        assert_type(address, str)
        assert_type(challenge, Challenge)

        sm = (
            SolutionModel
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

    # -------------------------
    # work & solution
    # -------------------------
    @measure_time
    def update_solution_submission_result(self, address: str, challenge: Challenge, solution: Solution, validated: bool):
        assert_type(address, str)
        assert_type(challenge, Challenge)
        assert_type(validated, bool)

        q1 = (
            WorkModel
            .update(status=(WorkStatus.Validated if validated else WorkStatus.Invalid).value)
            .where(
                (WorkModel.address == address) &
                (WorkModel.challenge_id == challenge.challenge_id)
                )
        )

        q2 = (
            SolutionModel
            .update(status=(SolutionStatus.Validated if validated else SolutionStatus.Invalid).value)
            .where(
                (SolutionModel.address == address) &
                (SolutionModel.challenge_id == challenge.challenge_id) &
                (SolutionModel.nonce_hex == solution.nonce_hex)
                )
        )

        with db_lock:
            q1.execute()
            q2.execute()
        # endwith
    # enddef
