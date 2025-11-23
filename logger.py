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
