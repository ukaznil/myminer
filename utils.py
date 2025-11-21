from datetime import datetime, timezone


def parse_iso8601_to_utc_naive(s: str) -> datetime:
    # "2025-10-30T23:59:59Z" → aware datetime(…, tzinfo=UTC)
    if s.endswith("Z"):
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    else:
        dt = datetime.fromisoformat(s)
    # endif

    # DB には「UTCのnaive」として保存する例
    return dt.astimezone(timezone.utc).replace(tzinfo=None)
