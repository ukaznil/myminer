import os
from enum import Enum

from project import Project
from utils import assert_type, msg_with_time


class LogType(Enum):
    System = ('system')
    Worklist = ('worklist')
    Hashrate = ('hashrate')
    Statistics = ('statistics')
    Start_New_Challenge = ('start_new_challenge')
    Solution_Found = ('solution_found')
    Solution_Submission = ('solution_submission')
    Solution_Submission_Error = ('solution_submission_error')
    Fetch_New_Challenge = ('fetch_new_challenge')
    Fetch_New_Challenge_Error = ('fetch_new_challenge_error')
    Cache_Status = ('cache_status')
    Memory = ('memory')

    def __init__(self, shortname: str):
        assert_type(shortname, str)

        self.shortname = shortname
    # enddef


class Logger:
    def __init__(self, project: Project):
        assert_type(project, Project)

        self._log_dirname = os.path.join('logs')
    # enddef

    def log(self, msg: str, log_type: LogType, sufix: str = None):
        assert_type(log_type, LogType)
        assert_type(msg, str)

        msg = msg_with_time(msg)

        filepath = os.path.join(self._log_dirname,
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
        do_measure_time = True

        if do_measure_time:
            start = time.time()
        # endif

        try:
            return func(self, *args, **kwargs)
        finally:
            if do_measure_time:
                end = time.time()
                elapsed = end - start
                funcname = func.__qualname__
                msg = f'[{funcname}] took {elapsed:.1f} sec'

                logger = self.logger  # type: Logger
                logger.log(msg, log_type=LogType.Func_Time_Measure, sufix=funcname)
            # endif
        # endtry
    return wrapper
