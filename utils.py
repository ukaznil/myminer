from datetime import datetime, timezone
from typing import *

import pytz


def parse_iso8601_to_utc_naive(s: str) -> datetime:
    assert_type(s, str)

    # "2025-10-30T23:59:59Z" → aware datetime(…, tzinfo=UTC)
    if s.endswith("Z"):
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    else:
        dt = datetime.fromisoformat(s)
    # endif

    # DB には「UTCのnaive」として保存する例
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def msg_with_time(msg: str, now: Optional[float]) -> str:
    assert_type(msg, str)

    jst = pytz.timezone("Asia/Tokyo")
    if now is None:
        now_jst = datetime.now(jst)
    else:
        now_jst = datetime.fromtimestamp(now, jst)
    # enif
    formatted = now_jst.strftime("%Y/%m/%d %H:%M:%S")

    return f'[{formatted}]\n{msg}\n'


def print_with_time(msg: str):
    assert_type(msg, str)

    msg = msg_with_time(msg)

    print(msg, flush=True)


def safefstr(v: Any, fmt: str) -> str:
    assert_type(fmt, str)

    if v is None:
        return 'N/A'
    # endif

    return f'{v:{fmt}}'


def assert_type(v: Any, _type: type, item_type: Optional[type] = None, allow_none: bool = False):
    is_debug = False

    if is_debug:
        _assert_type(v=v, _type=_type, item_type=item_type, allow_none=allow_none)
    # endif


def _assert_type(v: Any, _type: type, item_type: Optional[type] = None, allow_none: bool = False):
    # None 許容
    if v is None:
        if allow_none:
            return
        raise TypeError(
            f"Type assertion failed: "
            f"expected {_type.__name__}, got None"
            )

    # 本体の型チェック
    if not isinstance(v, _type):
        raise TypeError(
            f"Type assertion failed: "
            f"expected {_type.__name__}, "
            f"got {type(v).__name__} (value={v!r})"
            )

    # list / tuple の中身チェック
    if isinstance(v, (list, tuple)) and item_type is not None:
        for i, item in enumerate(v):
            if item is None and allow_none:
                continue
            if not isinstance(item, item_type):
                raise TypeError(
                    f"Type assertion failed at index {i}: "
                    f"expected {item_type.__name__}, "
                    f"got {type(item).__name__} (value={item!r})"
                    )
