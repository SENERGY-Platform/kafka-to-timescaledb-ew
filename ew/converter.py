"""
   Copyright 2022 InfAI (CC SES)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import datetime
import logging
import re
import typing

UNIX_FORMAT = "unix"
ISO_FORMAT = "iso8601"

# Tried in order when the configured time format does not fit the value, most
# common first. The configured format for an export is often unknown or wrong,
# so guessing is preferable to dropping the message.
FALLBACK_TIME_FORMATS = (
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    UNIX_FORMAT,
    "%Y-%m-%d %H:%M:%S.%f%z",
    "%Y-%m-%d %H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%d.%m.%Y %H:%M:%S",
    "%d.%m.%Y %H:%M",
    "%d.%m.%Y",
    "%d/%m/%Y %H:%M:%S",  # ambiguous with the line below, day first wins
    "%m/%d/%Y %H:%M:%S",
    "%a, %d %b %Y %H:%M:%S %z",
    "%a, %d %b %Y %H:%M:%S %Z",
    "%A, %d-%b-%y %H:%M:%S %Z",
    "%Y%m%dT%H%M%S%z",
    "%Y%m%dT%H%M%S",
    "%Y%m%d%H%M%S",
    "%Y%m%d",
    ISO_FORMAT,
)

# Range an epoch value has to fall into after scaling to seconds (1990 - 2100)
# to be guessed as such, so that a compact date like 20220301154501 or 20220301
# is not mistaken for one. Only applied when guessing, a format configured as
# unix is trusted.
UNIX_GUESS_MIN_SECONDS = 631152000.0
UNIX_GUESS_MAX_SECONDS = 4102444800.0

# Format that last worked for an export. Tried first, so the fallback search
# runs once per export and not per row. Keyed by export and not by configured
# format, because exports sharing a configured format can carry different
# values and would otherwise evict each other on every row.
_learned_formats: typing.Dict[typing.Optional[str], str] = dict()

# Fallbacks already reported, so that an export carrying more than one format
# does not log on every row.
_logged_formats: typing.Set[typing.Tuple[typing.Optional[str], str]] = set()


_iso_fraction_pattern = re.compile(r"\.\d{7,}")


class TimeParseError(ValueError):
    def __init__(self, val, fmt):
        super().__init__(f"could not parse time '{val}' with format '{fmt}' or any known format")


def to_float(val, *args):
    if val is None:
        return val
    return float(val)


def to_int(val, *args):
    if val is None:
        return val
    return int(val)


def to_bool(val, *args):
    return bool(val)


def to_str(val, *args):
    if val is None:
        return val
    return str(val)


def to_datetime(val, fmt: str = None, export_id=None):
    if val is None:
        return val
    for candidate in _gen_format_candidates(fmt, export_id):
        try:
            time_obj = _parse_time(val, candidate, configured=candidate == fmt)
        except (ValueError, TypeError, AttributeError, OverflowError, OSError):
            continue
        if candidate != fmt:
            _learn_format(fmt, candidate, val, export_id)
        return time_obj
    raise TimeParseError(val, fmt)


def _gen_format_candidates(fmt: str, export_id):
    learned = _learned_formats.get(export_id)
    seen = set()
    for candidate in (learned, fmt) + FALLBACK_TIME_FORMATS:
        if candidate is None or candidate in seen:
            continue
        seen.add(candidate)
        yield candidate


def _learn_format(fmt: str, candidate: str, val, export_id):
    _learned_formats[export_id] = candidate
    if (export_id, candidate) in _logged_formats:
        return
    _logged_formats.add((export_id, candidate))
    # the logger is provided by the service and only looked up on demand, to
    # keep this module free of import order constraints
    logging.getLogger("ew").warning(
        "parsing time with fallback format",
        {"export_id": export_id, "configured_format": fmt, "used_format": candidate, "value": str(val)}
    )



def _parse_time(val, fmt: str, configured: bool):
    if fmt == UNIX_FORMAT:
        return _from_unix(val, guessed=not configured)
    if fmt == ISO_FORMAT:
        return _from_iso(val)
    time_obj = datetime.datetime.strptime(val, fmt)
    return _to_naive_utc(time_obj)


def _from_iso(val):
    # catches iso 8601 variants the explicit formats miss, like arbitrary utc
    # offsets or a resolution below microseconds
    val = _iso_fraction_pattern.sub(lambda match: match.group(0)[:7], val.strip())
    return _to_naive_utc(datetime.datetime.fromisoformat(val))


def _from_unix(val, guessed: bool):
    seconds = float(val)
    if seconds != seconds:
        raise ValueError(f"'{val}' is not a timestamp")
    if not guessed:
        return _to_naive_utc(datetime.datetime.fromtimestamp(seconds, datetime.timezone.utc))
    # milli, micro and nano second epochs are common, pick by magnitude
    for divisor in (1, 1e3, 1e6, 1e9):
        scaled = seconds / divisor
        if UNIX_GUESS_MIN_SECONDS <= scaled <= UNIX_GUESS_MAX_SECONDS:
            return _to_naive_utc(datetime.datetime.fromtimestamp(scaled, datetime.timezone.utc))
    raise ValueError(f"'{val}' is not within the epoch range accepted for guessing")


def _to_naive_utc(time_obj: datetime.datetime):
    if time_obj.tzinfo is None:
        return time_obj
    return time_obj.astimezone(datetime.timezone.utc).replace(tzinfo=None)


type_map = {
    "TIMESTAMP": to_datetime,
    "TIMESTAMPTZ": to_datetime,
    "bool": to_bool,
    "real": to_float,
    "double": to_float,
    "smallint": to_int,
    "integer": to_int,
    "bigint": to_int,
    "varchar": to_str,
    "text": to_str
}
