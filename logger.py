import os
import time
from enum import Enum
from functools import wraps

import constants
from project import Project
from utils import assert_type, msg_with_time


class LogType(Enum):
    # system
    System = ('00_system')

    # work
    Worklist = ('10_worklist')
    Statistics = ('11_statistics')
    System_Metrics = ('12_system_metrics')
    ROM_Cache_Status = ('13_rom_cache_status')
    ROM_Cache_Management = ('14_rom_cache_all_cleared')

    # main loop
    Fetch_New_Challenge = ('20_fetch_new_challenge')
    Fetch_New_Challenge_Error = ('21_fetch_new_challenge_error')

    # mining loop
    Active_Addresses = ('30_active_addresses')
    Start_New_Challenge = ('31_start_new_challenge')
    Batch_Size_Search = ('32_batch_size_search')
    Solution_Found = ('33_solution_found')
    Solution_Submission = ('34_solution_submission')
    Solution_Submission_Error = ('35_solution_submission_error')
    Challenge_Expired = ('36_challenge_expired')

    # wallet
    Wallet_List = ('80_wallet_list')
    Donate_To = ('81_donate_to')
    Donate_To_Error = ('82_donate_to_error')

    # misc
    Func_Time_Measure = ('99_func_time_measure')

    def __init__(self, shortname: str):
        assert_type(shortname, str)

        self.shortname = shortname
    # enddef


class Logger:
    def __init__(self, project: Project):
        assert_type(project, Project)

        self.log_dirname = os.path.join('logs', project.name.lower())
        os.makedirs(self.log_dirname, exist_ok=True)
    # enddef

    def log(self, msg: str, log_type: LogType, sufix: str = None, now: float = None):
        assert_type(log_type, LogType)
        assert_type(msg, str)
        assert_type(now, float, allow_none=True)

        msg = msg_with_time(msg, now=now)

        filepath = os.path.join(self.log_dirname,
                                f'{log_type.shortname}' + (f'_{sufix}' if sufix else '') + '.log')
        with open(filepath, 'wt') as f:
            f.write(msg)
            f.flush()
        # endwith

        print(msg, flush=True)
    # enddef


def measure_time(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if constants.DEBUG:
            start = time.time()
        # endif

        try:
            return func(self, *args, **kwargs)
        finally:
            if constants.DEBUG:
                end = time.time()
                elapsed = end - start
                funcname = func.__qualname__
                msg = f'[{funcname}] took {elapsed:.1f} sec'

                logger = self.logger  # type: Logger
                logger.log(msg, log_type=LogType.Func_Time_Measure, sufix=funcname, now=end)
            # endif
        # endtry
    return wrapper
