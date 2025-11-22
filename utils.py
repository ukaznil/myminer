from datetime import datetime, timezone
from typing import Any

import pytz


def parse_iso8601_to_utc_naive(s: str) -> datetime:
    # "2025-10-30T23:59:59Z" → aware datetime(…, tzinfo=UTC)
    if s.endswith("Z"):
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    else:
        dt = datetime.fromisoformat(s)
    # endif

    # DB には「UTCのnaive」として保存する例
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def print_with_time(msg: str):
    jst = pytz.timezone("Asia/Tokyo")
    now_jst = datetime.now(jst)
    formatted = now_jst.strftime("%Y/%m/%d %H:%M")

    print(f'[{formatted}]\n{msg}\n', flush=True)


def safefstr(v: Any, fmt: str) -> str:
    if v is None:
        return 'N/A'
    # endif

    return f'{v:{fmt}}'
