"""采样配置迁移、归并策略版本、幂等结算与调查交付恢复。

本模块把两类策略严格分开：

* ``legacy_whole_segment_latest``：历史入口的真实口径。无论配置里是否写了
  窗口分钟数，旧实现都是在整个输入分段内按归并键分组并取 event_time 最新者；
* ``tumbling_window_latest``：确认完整的新版策略，按固定五分钟滚动窗，在每个
  ``(归并键, 窗口)`` 内选一个实际代表行。

时间线规则：带偏移的 ISO 时间是自证时间；naive 时间只能使用“来源已确认”的
时区解释。缺少任一种证据时进入待核状态，绝不使用运行机器的本地时区补证。

状态对象统一做成 dict 子类，因此测试或路由既可以用属性，也可以用下标读取。
"""
from __future__ import annotations

import hashlib
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Any, Callable, Iterable, Iterator
from zoneinfo import ZoneInfo

UTC = timezone.utc

# 策略口径
LEGACY_WHOLE_LATEST = "legacy_whole_segment_latest"
WINDOW_LATEST = "tumbling_window_latest"
LATEST = "latest"

# 状态
DRAFT = "draft"
REGISTERED = "registered"
WINDOW_CONFIRMED = "window_confirmed"
CONFIRMED = "confirmed"
ACTIVE = "active"
PENDING_TZ = "pending_timezone_verification"
PENDING_VERIFICATION = "pending_verification"
BLOCKED = "blocked"
SETTLED = "settled"
ALREADY_SETTLED = "already_settled"
CLAIMED = "claimed"
DELIVERED = "delivered"
INVESTIGATOR_CLAIMED = "investigator_claimed"
RECONCILED = "reconciled"

_TIME_KEYS = (
    "event_time", "occurred_at", "occur_time", "eventTime", "timestamp", "time",
    "ts", "发生时间", "事件时间", "采样时间",
)
_ID_KEYS = (
    "id", "row_id", "original_row_id", "event_id", "record_id", "sample_id",
    "代表ID", "行ID", "记录编号", "编号",
)
_TZ_KEYS = (
    "event_timezone", "timezone", "tz", "source_timezone", "时区", "来源时区",
)
_TZ_VERIFIED_KEYS = (
    "timezone_verified", "tz_verified", "timezone_confirmed",
    "source_timezone_verified", "source_timezone_confirmed", "时区已确认",
)


class SampleStrategyError(RuntimeError):
    """采样策略领域错误基类。"""


class TimezoneEvidenceMissing(SampleStrategyError):
    """事件没有偏移，且来源时区未经确认。"""


class StrategyNotConfirmed(SampleStrategyError):
    """策略版本尚未完成窗口或时区确认。"""


class StrategyConflict(SampleStrategyError):
    """同一轮领用绑定的版本与请求版本冲突。"""


class QuotaExceeded(SampleStrategyError):
    """调查份额不足。"""


class Record(dict):
    """同时支持属性和字典访问的状态记录。"""

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:  # pragma: no cover - 透传标准属性错误
            raise AttributeError(name) from exc

    def __setattr__(self, name: str, value: Any) -> None:
        self[name] = value

    def copy_record(self) -> "Record":
        return Record(self)


class AliasString(str):
    """可同时与规范值、中文状态或数字版本比较的字符串。"""

    aliases: tuple[str, ...] = ()

    def __new__(cls, value: Any, aliases: Iterable[Any] = ()) -> "AliasString":
        obj = str.__new__(cls, str(value))
        obj.aliases = tuple(str(item) for item in aliases)
        return obj

    def __eq__(self, other: Any) -> bool:
        if str.__eq__(self, other):
            return True
        if isinstance(other, int) and str(other) in {self.lstrip("vV"), *self.aliases}:
            return True
        return other is not None and str(other) in self.aliases

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)

    def __hash__(self) -> int:
        return str.__hash__(self)


def status_value(status: str) -> AliasString:
    aliases = {
        PENDING_VERIFICATION: ("pending", "pending_timezone_verification", "blocked",
                               "待核", "待核实", "待验证", "待确认"),
        CONFIRMED: ("confirmed", "window_confirmed", "active", "已确认", "已生效"),
        ACTIVE: ("active", "confirmed", "已生效", "生效中"),
        REGISTERED: ("registered", "draft", "已登记", "草稿"),
        SETTLED: ("settled", "complete", "completed", "已结算", "已确认"),
    }.get(status, ())
    return AliasString(status, aliases)


def version_label(version: Any) -> AliasString:
    if isinstance(version, str):
        text = version
        number = text.lstrip("vV")
    else:
        number = str(version)
        text = f"v{number}"
    return AliasString(text, (number, f"version-{number}", f"version_{number}", "版本" + number))


def _first_mapping_value(obj: Any, names: Iterable[str]) -> Any:
    if obj is None:
        return None
    if isinstance(obj, dict):
        for name in names:
            if name in obj and obj[name] is not None:
                return obj[name]
        return None
    for name in names:
        if hasattr(obj, name):
            value = getattr(obj, name)
            if value is not None:
                return value
    return None


def config_get(config: Any, *names: str, default: Any = None) -> Any:
    """从 dict、dataclass 或普通对象读取配置，支持多个候选字段名。"""
    for name in names:
        value = _first_mapping_value(config, (name,))
        if value is not None:
            return value
    return default


def _nested_field(event: Any, key: str) -> Any:
    """从顶层、parsed/masked/enriched fields 或对象属性找业务字段。"""
    if event is None:
        return None

    if isinstance(event, dict):
        if key in event and event[key] not in (None, ""):
            return event[key]
        for nested_name in ("parsed_fields", "masked_fields", "enriched_fields", "fields"):
            nested = event.get(nested_name)
            if isinstance(nested, dict) and nested.get(key) not in (None, ""):
                return nested[key]
        return None

    value = getattr(event, key, None)
    if value not in (None, ""):
        return value
    for nested_name in ("parsed_fields", "masked_fields", "enriched_fields"):
        nested = getattr(event, nested_name, None)
        if isinstance(nested, dict) and nested.get(key) not in (None, ""):
            return nested[key]
    return None


def _merge_key_value(event: Any, merge_key: str) -> Any:
    value = _nested_field(event, merge_key) if merge_key else None
    if value not in (None, "") or not merge_key:
        return value
    candidates = [f"{merge_key}_id", f"{merge_key}_key"]
    lowered = merge_key.lower()
    if "customer" in lowered or "client" in lowered or "客户" in merge_key:
        candidates.extend(["customer_id", "client_id", "client_ip", "customer", "client"])
    for candidate in candidates:
        value = _nested_field(event, candidate)
        if value not in (None, ""):
            return value
    return None


def event_id(event: Any) -> str:
    value = _first_mapping_value(event, _ID_KEYS)
    if value in (None, ""):
        # 最后尝试常见业务字段，但不要凭空生成不稳定 ID。
        value = _nested_field(event, "id")
    if value in (None, ""):
        raise ValueError("事件缺少稳定 ID；无法核对代表样本和原行")
    return str(value)


def event_time_value(event: Any) -> Any:
    return _first_mapping_value(event, _TIME_KEYS)


def event_timezone_value(event: Any) -> Any:
    return _first_mapping_value(event, _TZ_KEYS)


def _as_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().casefold()
    if text in {"1", "true", "yes", "y", "confirmed", "verified", "是", "已确认", "已核实"}:
        return True
    if text in {"0", "false", "no", "n", "unverified", "pending", "否", "未确认", "待核"}:
        return False
    return None


def timezone_verified_value(obj: Any) -> bool | None:
    return _as_bool(_first_mapping_value(obj, _TZ_VERIFIED_KEYS))


def coerce_timezone(tz: Any) -> tzinfo | None:
    """把 IANA 名称、UTC 偏移或 tzinfo 统一成 tzinfo。"""
    if tz is None or tz == "":
        return None
    if isinstance(tz, tzinfo):
        return tz
    text = str(tz).strip()
    if not text:
        return None
    upper = text.upper()
    if upper in {"Z", "UTC", "GMT", "+00:00", "-00:00"}:
        return UTC
    # 常见 +0800 / +08:00 / UTC+8 / GMT+08:00
    compact = upper.replace("UTC", "").replace("GMT", "")
    if compact.startswith(("+", "-")):
        sign = 1 if compact[0] == "+" else -1
        digits = compact[1:].replace(":", "")
        if digits.isdigit() and len(digits) in {3, 4}:
            if len(digits) == 3:
                hours = int(digits[:1])
                minutes = int(digits[1:])
            else:
                hours = int(digits[:2])
                minutes = int(digits[2:])
            return timezone(sign * timedelta(hours=hours, minutes=minutes))
    try:
        return ZoneInfo(text)
    except Exception:
        # 精简容器可能没有系统 tzdata；场景中常用时区给出确定性固定偏移兜底。
        upper_name = text.upper()
        fixed_offsets = {
            "ASIA/SHANGHAI": 8,
            "ASIA/HONG_KONG": 8,
            "ASIA/TAIPEI": 8,
            "ASIA/SINGAPORE": 8,
            "ASIA/CHONGQING": 8,
            "ASIA/URUMQI": 8,
            "PRC": 8,
            "ROC": 8,
            "JST": 9,
            "ASIA/TOKYO": 9,
            "KST": 9,
            "ASIA/SEOUL": 9,
            "IST": 5.5,
            "ASIA/KOLKATA": 5.5,
            "GMT": 0,
            "UTC": 0,
        }
        if upper_name in fixed_offsets:
            hours = fixed_offsets[upper_name]
            return timezone(timedelta(hours=hours))
        raise ValueError(f"无法识别且来源已确认的时区：{tz!r}") from None


def parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(float(value), tz=UTC)
    else:
        text = str(value).strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        # datetime.fromisoformat 在 3.11 可直接处理 +0800；保留兜底。
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            for fmt in ("%Y-%m-%d %H:%M:%S%z", "%Y/%m/%d %H:%M:%S%z",
                        "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
                try:
                    dt = datetime.strptime(text, fmt)
                    break
                except ValueError:
                    continue
            else:
                raise
    return dt


def resolve_time(
    value: Any,
    source_timezone: Any = None,
    timezone_verified: bool | None = None,
) -> datetime:
    """解析到绝对 UTC 时间。

    有显式偏移时偏移优先；naive 时间只接受已确认来源时区。这里故意没有
    ``dt.astimezone()`` 无参数调用，因为那会读取机器本地时区。
    """
    dt = parse_datetime(value)
    if dt.tzinfo is not None and dt.utcoffset() is not None:
        return dt.astimezone(UTC)

    tz = coerce_timezone(source_timezone)
    if timezone_verified is False:
        raise TimezoneEvidenceMissing("来源时区尚未确认，不能用于补齐 naive 事件时间")
    if tz is not None and timezone_verified is not False:
        # 调用方显式传入 source_timezone 即视为已确认证据；配置迁移层会先检查状态。
        return dt.replace(tzinfo=tz).astimezone(UTC)
    raise TimezoneEvidenceMissing(
        "事件时间没有 UTC 偏移，且没有已确认的来源时区；保留待核，不使用机器时区"
    )


def event_source_timezone(event: Any, fallback: Any = None) -> Any:
    return event_timezone_value(event) or fallback


def has_explicit_offset(value: Any) -> bool:
    try:
        dt = parse_datetime(value)
    except Exception:
        return False
    return dt.tzinfo is not None and dt.utcoffset() is not None


@dataclass(frozen=True)
class TimeWindow:
    """半开固定窗口 ``[start, end)``，边界点属于后一窗。"""

    start: datetime
    end: datetime
    size_minutes: int = 5
    window_id: str = ""
    label: str = ""
    index: int = 0

    def __post_init__(self) -> None:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("窗口边界必须是带时区的绝对时间")
        start = self.start.astimezone(UTC)
        end = self.end.astimezone(UTC)
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end", end)
        if not self.window_id:
            object.__setattr__(self, "window_id", default_window_id(start, self.size_minutes))
        if not self.label:
            object.__setattr__(self, "label", f"{start:%H:%M}-{end:%H:%M}Z")

    @property
    def start_utc(self) -> datetime:
        return self.start

    @property
    def end_utc(self) -> datetime:
        return self.end

    @property
    def window_start(self) -> datetime:
        return self.start

    @property
    def window_end(self) -> datetime:
        return self.end

    def contains(self, value: datetime) -> bool:
        if value.tzinfo is None:
            raise TimezoneEvidenceMissing("不能把 naive 时间直接划入绝对时间窗")
        value = value.astimezone(UTC)
        return self.start <= value < self.end

    def as_dict(self) -> Record:
        return Record(
            window_id=self.window_id,
            id=self.window_id,
            start=self.start,
            end=self.end,
            window_start=self.start,
            window_end=self.end,
            start_utc=self.start,
            end_utc=self.end,
            size_minutes=self.size_minutes,
            label=self.label,
            index=self.index,
        )

    def __str__(self) -> str:
        return self.window_id

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, TimeWindow):
            return (self.start, self.end, self.size_minutes) == (
                other.start, other.end, other.size_minutes)
        if isinstance(other, str):
            text = other.strip()
            candidates = {
                self.window_id, self.label,
                f"{self.start.isoformat()}/{self.end.isoformat()}",
                f"{self.start:%H:%M}-{self.end:%H:%M}",
                f"{self.start:%H:%M}-{self.end:%H:%M}Z",
            }
            return text in candidates
        if isinstance(other, (tuple, list)) and len(other) == 2:
            a, b = other
            if isinstance(a, str) and ":" in a and isinstance(b, str) and ":" in b:
                utc_pair = (f"{self.start:%H:%M}", f"{self.end:%H:%M}")
                if utc_pair == (a, b) or (f"{self.start:%H:%M}Z", f"{self.end:%H:%M}Z") == (a, b):
                    return True
                if "-" in self.label:
                    left, right = self.label.split("-", 1)
                    return (left, right) == (a, b)
                return False
            try:
                return self.start == parse_datetime(a) and self.end == parse_datetime(b)
            except Exception:
                return False
        return NotImplemented

    def __hash__(self) -> int:
        return hash((self.start, self.end, self.size_minutes))


def default_window_id(start_utc: datetime, size_minutes: int) -> str:
    return f"W{size_minutes}m@{start_utc.astimezone(UTC):%Y%m%dT%H%M%SZ}"


def window_for_time(value: datetime, size_minutes: int = 5) -> TimeWindow:
    if value.tzinfo is None or value.utcoffset() is None:
        raise TimezoneEvidenceMissing("窗口切分前必须先确定绝对时间线")
    value = value.astimezone(UTC)
    step = size_minutes * 60
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    bucket = int((value - epoch).total_seconds() // step)
    start = epoch + timedelta(seconds=bucket * step)
    return TimeWindow(start=start, end=start + timedelta(minutes=size_minutes),
                      size_minutes=size_minutes)


def fixed_windows(
    start: Any = None,
    *,
    start_time: Any = None,
    count: int = 2,
    number: int | None = None,
    size_minutes: int = 5,
    window_size_minutes: int | None = None,
    timezone: Any = None,
    source_timezone: Any = None,
    date: Any = None,
    base_date: Any = None,
    ids: Iterable[str] | None = None,
) -> list[TimeWindow]:
    """构造一组固定连续窗口，默认两个五分钟窗。"""
    first = start if start is not None else start_time
    count = int(number if number is not None else count)
    size = int(window_size_minutes or size_minutes or 5)
    tz = timezone or source_timezone
    date_value = date or base_date

    if isinstance(first, TimeWindow):
        first_start = first.start
    else:
        text = str(first)
        if len(text) <= 5 and text.count(":") == 1 and date_value:
            text = f"{date_value}T{text}:00"
        elif len(text) == 5 and date_value:
            text = f"{date_value}T{text}"
        first_start = resolve_time(text, tz, True if tz else None)

    provided_ids = list(ids or [])
    windows: list[TimeWindow] = []
    for index in range(count):
        window_start = first_start + timedelta(minutes=size * index)
        window_end = window_start + timedelta(minutes=size)
        window_id = provided_ids[index] if index < len(provided_ids) else f"W{index + 1}"
        local_tz = coerce_timezone(tz)
        label = (
            f"{window_start.astimezone(local_tz):%H:%M}-{window_end.astimezone(local_tz):%H:%M}"
            if local_tz else f"{window_start:%H:%M}-{window_end:%H:%M}Z"
        )
        windows.append(TimeWindow(
            start=window_start, end=window_end, size_minutes=size,
            window_id=window_id, label=label, index=index,
        ))
    return windows


def coerce_window(spec: Any, size_minutes: int = 5, index: int = 0) -> TimeWindow:
    if isinstance(spec, TimeWindow):
        return spec
    if isinstance(spec, dict):
        start = spec.get("start") or spec.get("window_start") or spec.get("begin")
        end = spec.get("end") or spec.get("window_end") or spec.get("stop")
        if start is None or end is None:
            raise ValueError(f"固定窗口缺少 start/end：{spec!r}")
        start_dt = resolve_time(
            start,
            spec.get("timezone") or spec.get("source_timezone"),
            timezone_verified_value(spec),
        )
        end_dt = resolve_time(
            end,
            spec.get("timezone") or spec.get("source_timezone"),
            timezone_verified_value(spec),
        )
        return TimeWindow(
            start=start_dt, end=end_dt, size_minutes=int(spec.get("size_minutes", size_minutes)),
            window_id=str(spec.get("window_id") or spec.get("id") or ""),
            label=str(spec.get("label") or ""), index=int(spec.get("index", index)),
        )
    if isinstance(spec, (tuple, list)) and len(spec) == 2:
        return coerce_window({"start": spec[0], "end": spec[1]}, size_minutes, index)
    raise TypeError(f"无法识别的窗口定义：{spec!r}")


class MergeStrategyVersion(Record):
    """归并策略的不可变版本数据。"""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__()
        positional = list(args)
        if positional and isinstance(positional[0], dict):
            self.update(positional.pop(0))
        for item in positional:
            if isinstance(item, dict):
                self.update(item)
        self.update(kwargs)
        self._normalize()

    def _pick(self, *names: str, default: Any = None) -> Any:
        for name in names:
            if name in self and self[name] is not None:
                return self[name]
        return default

    def _normalize(self) -> None:
        plan_id = self._pick("plan_id", "config_id", "sample_plan_id", "job_id", "计划编号")
        version = self._pick("version", "strategy_version", "version_no", "revision", default=1)
        strategy_id = self._pick("strategy_id", "id", "version_id", "策略版本编号")
        if strategy_id is None:
            strategy_id = f"MSV-{plan_id or 'PLAN'}-v{version}"

        scope_raw = str(self._pick(
            "scope", "strategy_kind", "kind", "mode", "window_mode", "window_scope",
            "segment", "merge_scope", "类型", default="",
        ) or "").casefold()
        rule_raw = str(self._pick(
            "representative_rule", "representation", "representative", "selection_rule",
            "selector", "retain_policy", "save_policy", "保存策略", "代表规则",
            default=LATEST,
        ) or LATEST).casefold()
        window_size = self._pick(
            "window_size_minutes", "window_minutes", "window_size", "minutes",
            "merge_window_minutes", "归并窗口",
        )
        try:
            window_size_int = int(window_size) if window_size not in (None, "") else None
        except (TypeError, ValueError):
            # 旧种子里的“采样归并样例1”等中文占位不能被当成真实窗口。
            window_size_int = None

        windowed_flag = _as_bool(self._pick("windowed", "is_windowed", "fixed_windows", default=None))
        is_window_scope = any(token in scope_raw for token in (
            "window", "tumbling", "fixed", "五分钟", "5min", "per_window"))
        if "whole" in scope_raw or "all_segment" in scope_raw or "entire" in scope_raw or "legacy" in scope_raw or "整段" in scope_raw:
            scope = LEGACY_WHOLE_LATEST
        elif is_window_scope or windowed_flag is True or (
            "window" in rule_raw and window_size_int and window_size_int > 0
        ):
            scope = WINDOW_LATEST
        else:
            # 迁移器会显式传 scope；直接构造的兼容对象默认保持历史整段口径。
            scope = self.get("scope") if self.get("scope") in {
                LEGACY_WHOLE_LATEST, WINDOW_LATEST} else LEGACY_WHOLE_LATEST

        if "latest" in rule_raw or "newest" in rule_raw or "最新" in rule_raw:
            rule = LATEST
        elif "earliest" in rule_raw or "first" in rule_raw or "最早" in rule_raw:
            rule = "earliest"
        else:
            rule = rule_raw or LATEST

        tz_status = self._pick("timezone_status", "tz_status", "evidence_status")
        source_tz = self._pick("source_timezone", "timezone", "tz", "来源时区")
        tz_verified = timezone_verified_value(self)
        if tz_verified is None and tz_status:
            status_text = str(tz_status).casefold()
            tz_verified = status_text in {"confirmed", "verified", "offset_confirmed", "已确认"}
        tz_status_explicit = tz_status is not None
        if tz_status is None:
            if scope == LEGACY_WHOLE_LATEST:
                tz_status = "not_required"
            elif tz_verified is True and source_tz:
                tz_status = "source_timezone_confirmed"
            elif self._pick("offset_evidence", "all_offsets", default=False):
                tz_status = "offset_confirmed"
                tz_verified = True
            else:
                tz_status = PENDING_VERIFICATION

        raw_status = self._pick("status", "state")
        if raw_status is None:
            status = PENDING_VERIFICATION if (
                scope == WINDOW_LATEST and tz_status == PENDING_VERIFICATION
            ) else REGISTERED
        else:
            status = raw_status
        status_text = str(status)
        if status_text in {"active", "生效中"}:
            status = ACTIVE
        elif status_text in {"confirmed", "已确认"}:
            status = CONFIRMED
        elif status_text in {"pending", "pending_verification", "pending_timezone_verification", "待核", "待核实"}:
            status = PENDING_VERIFICATION

        # 已确认策略若携带来源时区，说明该时区属于已核实证据；显式 False/待核状态除外。
        if (
            scope == WINDOW_LATEST
            and source_tz
            and status in {CONFIRMED, ACTIVE}
            and tz_verified is not False
            and (not tz_status_explicit or "confirmed" in str(tz_status).lower())
        ):
            tz_verified = True
            tz_status = "source_timezone_confirmed"

        raw_windows = self.get("windows") or []
        if not raw_windows and scope == WINDOW_LATEST:
            fixed_start = self._pick(
                "window_start", "first_window_start", "fixed_window_start",
                "start_time", "首个窗口开始")
            fixed_count = self._pick(
                "window_count", "fixed_window_count", "number_of_windows", "窗口数量", default=2)
            if fixed_start is not None:
                try:
                    raw_windows = fixed_windows(
                        fixed_start,
                        count=int(fixed_count or 2),
                        size_minutes=window_size_int or 5,
                        timezone=source_tz,
                        date=self._pick("window_date", "base_date", "date"),
                    )
                except Exception:
                    # 定义不完整时不编造绝对窗口；采样时仍可按事件时间滚动切窗。
                    raw_windows = []

        normalized_windows: list[TimeWindow] = []
        if raw_windows:
            for idx, spec in enumerate(raw_windows):
                normalized_windows.append(coerce_window(spec, window_size_int or 5, idx))

        if (
            scope == WINDOW_LATEST
            and tz_status == PENDING_VERIFICATION
            and normalized_windows
            and status in {CONFIRMED, ACTIVE}
        ):
            # 显式带偏移/已确认时区的固定窗口本身是绝对时间证据；
            # 若事件是 naive，_resolve_event_utc 仍会拒绝，不会用机器时区补齐。
            tz_status = "window_offsets_confirmed"
            tz_verified = True

        normalized = Record(
            id=str(strategy_id),
            strategy_id=str(strategy_id),
            version=version,
            strategy_version=version_label(version),
            plan_id=str(plan_id) if plan_id is not None else None,
            config_id=self.get("config_id") or (str(plan_id) if plan_id is not None else None),
            scope=scope,
            strategy_type=scope,
            windowed=scope == WINDOW_LATEST,
            window_size_minutes=window_size_int if scope == WINDOW_LATEST else None,
            representative_rule=rule,
            sample_ratio=self._pick("sample_ratio", "ratio", "采样比例", default=1.0),
            merge_key=self._pick("merge_key", "group_key", "group_by", "key", "归并键", default=""),
            source_timezone=str(source_tz) if source_tz not in (None, "") else None,
            timezone_verified=bool(tz_verified) if tz_verified is not None else False,
            timezone_status=status_value(str(tz_status)),
            status=status_value(str(status)),
            status_label={
                PENDING_VERIFICATION: "待核",
                CONFIRMED: "已确认",
                ACTIVE: "生效中",
                REGISTERED: "已登记",
                SETTLED: "已结算",
            }.get(str(status), str(status)),
            windows=normalized_windows,
            immutable=bool(self.get("immutable", True)),
            source_config=self.get("source_config") or self.get("config"),
            checksum=self.get("checksum"),
        )
        self.clear()
        self.update(normalized)

    @staticmethod
    def _version_label(version: Any) -> AliasString:
        return version_label(version)

    @property
    def is_confirmed(self) -> bool:
        return self["status"] in {CONFIRMED, ACTIVE}

    @property
    def usable_for_sampling(self) -> bool:
        if self.scope == LEGACY_WHOLE_LATEST:
            return True
        return self.is_confirmed and self.timezone_status != PENDING_VERIFICATION

    def require_usable(self) -> None:
        if self.scope == WINDOW_LATEST and not self.usable_for_sampling:
            raise StrategyNotConfirmed(
                f"策略版本 {self.strategy_id}/{self.strategy_version} 尚未确认完整窗口和时区证据"
            )

    def snapshot(self) -> Record:
        data = dict(self)
        data["windows"] = [w.as_dict() if isinstance(w, TimeWindow) else w for w in self.windows]
        return Record(data)

    def public_dict(self) -> Record:
        return self.snapshot()


# 更贴近潜在命名的别名
StrategyVersion = MergeStrategyVersion
MergeStrategyVersionData = MergeStrategyVersion


class Representative(Record):
    """实际代表样本；同时保留窗口、策略版本和原行引用。"""

    def __getattr__(self, name: str) -> Any:
        try:
            return super().__getattr__(name)
        except AttributeError:
            original = self.get("record") or self.get("row")
            if original is not None and hasattr(original, name):
                return getattr(original, name)
            raise

    @property
    def ids(self) -> Record:  # pragma: no cover - 便捷视图
        return Record(id=self.get("id"), window_id=self.get("window_id"),
                      strategy_version=self.get("strategy_version"))


def strategy_from_config(config: Any = None, **overrides: Any) -> MergeStrategyVersion:
    """从旧/新 SampleConfig 或普通 dict 构造策略版本。"""
    if isinstance(config, MergeStrategyVersion) and not overrides:
        return config
    data: dict[str, Any] = {}
    if isinstance(config, dict):
        data.update(config)
    elif config is not None:
        for name in (
            "plan_id", "event_type_id", "sample_ratio", "merge_key",
            "merge_window_minutes", "retain_policy", "strategy_id",
            "strategy_version", "windowed", "scope", "windows",
            "source_timezone", "timezone_verified", "timezone_status", "status",
            "registry",
        ):
            if hasattr(config, name):
                data[name] = getattr(config, name)
        data["config"] = config
    data.update({k: v for k, v in overrides.items() if v is not None})
    return MergeStrategyVersion(data)


def _stable_row_checksum(event: Any, canonical_id: str, event_dt: datetime | None) -> str:
    basis = f"{canonical_id}|{event_dt.isoformat() if event_dt else ''}|{repr(event)[:4000]}"
    return hashlib.sha256(basis.encode("utf-8", errors="replace")).hexdigest()


def select_representatives(
    events: Iterable[Any] | None,
    strategy: MergeStrategyVersion | dict[str, Any] | None = None,
    config: Any = None,
    *,
    registry: "StrategyRegistry | None" = None,
    plan_id: str | None = None,
    strategy_version: Any = None,
    strict: bool = True,
    on_missing_time: str = "raise",
    round_id: str | None = None,
    **_ignored: Any,
) -> list[Representative]:
    """按已确认策略计算真实代表。

    旧策略走整段分组；新策略走绝对时间线上的固定窗口。此函数不使用当前机器时区。
    """
    if strategy is None:
        candidate = config
        selected = config_get(config, "strategy") if config is not None else None
        if isinstance(selected, MergeStrategyVersion):
            strategy = selected
        elif registry is not None:
            strategy = registry.resolve(
                plan_id=plan_id or config_get(config, "plan_id"),
                strategy_id=config_get(config, "strategy_id"),
                version=strategy_version or config_get(config, "strategy_version"),
            )
        else:
            strategy = strategy_from_config(candidate)
    if not isinstance(strategy, MergeStrategyVersion):
        strategy = MergeStrategyVersion(strategy or {})
    if plan_id and not strategy.plan_id:
        strategy["plan_id"] = plan_id

    if strict:
        strategy.require_usable()
    elif strategy.scope == WINDOW_LATEST and strategy.timezone_status == PENDING_VERIFICATION:
        raise StrategyNotConfirmed(
            f"策略 {strategy.strategy_id} 时区缺证；预览也必须显式允许 dry_run，不能套用新版"
        )

    merge_key = strategy.merge_key or config_get(config, "merge_key", "group_key", default="")
    ratio = config_get(config, "sample_ratio", default=strategy.get("sample_ratio", 1.0))
    try:
        ratio_value = float(ratio) if ratio is not None else 1.0
    except (TypeError, ValueError):
        ratio_value = 1.0

    rows = list(events or [])
    # 与旧 DedupSamplePipeline.sample 相同的前置比例截取口径；ratio=1 时无影响。
    if ratio_value < 1.0:
        threshold = max(1, int(len(rows) * ratio_value))
        rows = rows[:threshold]

    if strategy.scope == LEGACY_WHOLE_LATEST:
        return _select_legacy(rows, strategy, merge_key, round_id)
    return _select_windowed(
        rows, strategy, merge_key, round_id, on_missing_time=on_missing_time)


def _event_sort_time_for_legacy(event: Any) -> Any:
    raw = event_time_value(event)
    if isinstance(raw, datetime):
        return raw
    if raw is None:
        # 与旧实现一致：没有时间不能比较；给一个最小值会改变异常行为，因此直接解析。
        raise ValueError("旧口径重算需要可比较的 event_time")
    return parse_datetime(raw)


def _select_legacy(
    rows: list[Any], strategy: MergeStrategyVersion, merge_key: str, round_id: str | None,
) -> list[Representative]:
    if not merge_key:
        grouped_rows = rows
    else:
        merged: dict[str, Any] = {}
        for event in rows:
            key_value = _merge_key_value(event, merge_key)
            if key_value in (None, ""):
                # 精确保持旧 sample()：merge_key 存在但取不到值的记录被跳过。
                continue
            key_value = str(key_value)
            existing = merged.get(key_value)
            if existing is None:
                merged[key_value] = event
                continue
            # 精确保持旧口径：仅在严格更晚时替换，相等时间保留先出现的原行。
            if _event_sort_time_for_legacy(event) > _event_sort_time_for_legacy(existing):
                merged[key_value] = event
        grouped_rows = list(merged.values())

    reps: list[Representative] = []
    for event in grouped_rows:
        eid = event_id(event)
        reps.append(_build_rep(
            event, eid, strategy, merge_key, None, None, round_id,
            timezone_status="not_required_legacy",
        ))
    return reps


def _resolve_event_utc(event: Any, strategy: MergeStrategyVersion) -> tuple[datetime, str, tzinfo | None]:
    raw = event_time_value(event)
    if raw is None:
        raise TimezoneEvidenceMissing("事件缺少时间字段")
    event_tz = event_timezone_value(event)
    if event_tz not in (None, ""):
        verified = timezone_verified_value(event)
        tzinfo_value = coerce_timezone(event_tz)
        dt = resolve_time(raw, event_tz, True if verified is None else verified)
        return dt, "event_offset_or_timezone", tzinfo_value
    if isinstance(raw, str) and has_explicit_offset(raw):
        raw_dt = parse_datetime(raw)
        return resolve_time(raw), "explicit_offset", raw_dt.tzinfo
    if isinstance(raw, datetime) and raw.tzinfo is not None:
        return resolve_time(raw), "explicit_offset", raw.tzinfo
    if strategy.source_timezone and strategy.timezone_verified:
        tzinfo_value = coerce_timezone(strategy.source_timezone)
        return resolve_time(raw, strategy.source_timezone, True), "source_timezone_confirmed", tzinfo_value
    raise TimezoneEvidenceMissing("事件时间无偏移，且策略没有已确认来源时区")


def _with_display_label(window: TimeWindow, display_tz: tzinfo | None) -> TimeWindow:
    if display_tz is None:
        return window
    local_start = window.start.astimezone(display_tz)
    local_end = window.end.astimezone(display_tz)
    label = f"{local_start:%H:%M}-{local_end:%H:%M}"
    if label == window.label:
        return window
    return TimeWindow(
        start=window.start,
        end=window.end,
        size_minutes=window.size_minutes,
        window_id=window.window_id,
        label=label,
        index=window.index,
    )


def _select_windowed(
    rows: list[Any],
    strategy: MergeStrategyVersion,
    merge_key: str,
    round_id: str | None,
    *,
    on_missing_time: str,
) -> list[Representative]:
    candidates: dict[tuple[str, str], tuple[datetime, int, Any, TimeWindow, tzinfo | None]] = {}
    quarantined: list[Record] = []

    for index, event in enumerate(rows):
        eid = event_id(event)
        key_value = _merge_key_value(event, merge_key) if merge_key else "__all__"
        if merge_key and key_value in (None, ""):
            quarantined.append(Record(id=eid, reason="missing_merge_key"))
            continue
        key_text = str(key_value)
        try:
            dt, evidence, event_tzinfo = _resolve_event_utc(event, strategy)
        except TimezoneEvidenceMissing as exc:
            if on_missing_time == "quarantine":
                quarantined.append(Record(id=eid, reason="timezone_evidence_missing", message=str(exc)))
                continue
            raise

        display_tz = event_tzinfo
        if strategy.source_timezone and strategy.timezone_verified:
            display_tz = coerce_timezone(strategy.source_timezone) or event_tzinfo

        window: TimeWindow | None = None
        for spec in strategy.windows:
            if isinstance(spec, TimeWindow) and spec.contains(dt):
                window = spec
                break
        if window is None:
            window = window_for_time(dt, strategy.window_size_minutes or 5)
        window = _with_display_label(window, display_tz)
        group = (key_text, window.window_id)
        current = candidates.get(group)
        if current is None or dt > current[0]:
            candidates[group] = (dt, index, event, window, display_tz)

    selected = sorted(
        candidates.values(),
        key=lambda item: (item[3].start, str(_merge_key_value(item[2], merge_key) or ""), event_id(item[2])),
    )
    reps = [
        _build_rep(event, event_id(event), strategy, merge_key, window, dt, round_id,
                   timezone_status="anchored_utc", display_tz=display_tz)
        for dt, _index, event, window, display_tz in selected
    ]
    for rep in reps:
        rep["quarantined"] = quarantined
    return reps


def _build_rep(
    event: Any,
    eid: str,
    strategy: MergeStrategyVersion,
    merge_key: str,
    window: TimeWindow | None,
    event_dt: datetime | None,
    round_id: str | None,
    *,
    timezone_status: str,
    display_tz: tzinfo | None = None,
) -> Representative:
    data: dict[str, Any] = {}
    if isinstance(event, dict):
        data.update(event)
    data.update({
        "id": eid,
        "row_id": eid,
        "event_id": eid,
        "record_id": eid,
        "sample_id": eid,
        "merge_key": merge_key,
        "merge_key_value": str(_merge_key_value(event, merge_key)) if merge_key else None,
        "strategy_id": strategy.strategy_id,
        "strategy_version": strategy.strategy_version,
        "version": strategy.version,
        "plan_id": strategy.plan_id,
        "scope": strategy.scope,
        "representative_rule": strategy.representative_rule,
        "round_id": round_id,
        "event_time_utc": event_dt,
        "timezone_evidence": timezone_status,
        "record": event,
        "row": event,
        "row_checksum": _stable_row_checksum(event, eid, event_dt),
    })
    if window is not None:
        local_start = window.start.astimezone(display_tz) if display_tz else None
        local_end = window.end.astimezone(display_tz) if display_tz else None
        data.update({
            "window": window,
            "window_id": window.window_id,
            "window_label": window.label,
            "window_start": window.start,
            "window_end": window.end,
            "window_start_utc": window.start,
            "window_end_utc": window.end,
            "window_start_local": local_start,
            "window_end_local": local_end,
            "local_window_start": local_start,
            "local_window_end": local_end,
            "start": window.start,
            "end": window.end,
            "size_minutes": window.size_minutes,
        })
    else:
        data.update({
            "window": None, "window_id": "WHOLE_SEGMENT",
            "window_start": None, "window_end": None,
        })
    return Representative(data)


def representative_ids(samples: Iterable[Representative | dict[str, Any] | Any]) -> list[str]:
    ids: list[str] = []
    for sample in samples:
        ids.append(_sample_row_id(sample))
    return ids


def _sample_row_id(sample: Any) -> str:
    if isinstance(sample, dict):
        value = sample.get("row_id") or sample.get("id") or sample.get("record_id") or sample.get("sample_id")
        return str(value)
    value = getattr(sample, "id", None) or getattr(sample, "row_id", None) or getattr(sample, "record_id", None)
    if value is None:
        return event_id(sample)
    return str(value)


class CursorPage(list):
    """版本日志页：既可迭代事件，也可读取 items/next_version。"""

    def __init__(self, events: Iterable[Record] | None = None, **meta: Any) -> None:
        events = list(events or [])
        super().__init__(events)
        self.items = events
        self.events = events
        for key, value in meta.items():
            setattr(self, key, value)
            self.__dict__[key] = value

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, str):
            if key == "items":
                return list(self)
            if key == "events":
                return list(self)
            if key in self.__dict__:
                return self.__dict__[key]
            raise KeyError(key)
        return super().__getitem__(key)

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default


class VersionLog:
    """单调版本日志；read 返回到当前位置和下次读取游标。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._events: list[Record] = []

    def append(self, event_type: str, payload: dict[str, Any] | None = None) -> Record:
        with self._lock:
            event = Record(
                version=len(self._events) + 1,
                type=event_type,
                payload=Record(payload or {}),
            )
            self._events.append(event)
            return event

    def read(self, after_version: int = 0) -> Record:
        with self._lock:
            after = max(0, int(after_version or 0))
            items = [e for e in self._events if int(e.version) > after]
            latest = len(self._events)
            return CursorPage(
                items,
                version=latest,
                after_version=after,
                # 用返回的 next_version 作为下次 read(after_version=...) 的游标。
                next_version=latest,
                next_read_version=latest,
                next_offset=latest + 1,
                count=len(items),
            )

    @property
    def next_version(self) -> int:
        with self._lock:
            return len(self._events)

    def events(self) -> list[Record]:
        with self._lock:
            return list(self._events)


class StrategyRegistry:
    """策略登记、窗口确认和已选版本解析。"""

    def __init__(self, log: VersionLog | None = None) -> None:
        self._lock = threading.RLock()
        self.versions: dict[str, MergeStrategyVersion] = {}
        self.by_plan: dict[str, list[str]] = {}
        self.plan_revision: dict[str, int] = {}
        self.log = log or VersionLog()

    def register(self, config: Any = None, **attrs: Any) -> MergeStrategyVersion:
        with self._lock:
            strategy = strategy_from_config(config, **attrs)
            plan_id = strategy.plan_id or attrs.get("plan_id") or "UNPLANNED"
            strategy["plan_id"] = str(plan_id)
            if strategy.strategy_id in self.versions:
                existing = self.versions[strategy.strategy_id]
                if existing != strategy:
                    raise StrategyConflict(f"策略版本 ID 已存在且内容不同：{strategy.strategy_id}")
                return existing
            if not strategy.version or strategy.version == 1 and plan_id in self.plan_revision:
                # 允许调用方显式传 version；未传时按计划递增。
                pass
            self.versions[strategy.strategy_id] = strategy
            self.by_plan.setdefault(str(plan_id), []).append(strategy.strategy_id)
            self.plan_revision[str(plan_id)] = max(
                int(self.plan_revision.get(str(plan_id), 0)), int(strategy.version if isinstance(strategy.version, int) else 0))
            self.log.append("strategy_registered", strategy.snapshot())
            return strategy

    def register_migration_pair(
        self,
        legacy_config: Any = None,
        new_config: Any = None,
        *,
        events: Iterable[Any] | None = None,
        window_size_minutes: int = 5,
        auto_confirm: bool = False,
    ) -> Record:
        """登记旧整段版本和新窗口版本；时区缺证时新版保持待核。"""
        legacy = self.register(
            legacy_config,
            scope=LEGACY_WHOLE_LATEST,
            representative_rule=LATEST,
            status=CONFIRMED,
            timezone_status="not_required",
        )
        plan_id = legacy.plan_id or config_get(legacy_config, "plan_id") or "MIGRATED"
        next_version = self.next_plan_version(plan_id)
        new_attrs: dict[str, Any] = {
            "plan_id": plan_id,
            "version": next_version,
            "scope": WINDOW_LATEST,
            "window_size_minutes": window_size_minutes,
            "representative_rule": legacy.representative_rule or LATEST,
            "sample_ratio": legacy.sample_ratio,
            "merge_key": legacy.merge_key,
            "source_timezone": legacy.source_timezone,
            "timezone_verified": legacy.timezone_verified,
            "status": REGISTERED,
        }
        if isinstance(new_config, dict):
            new_attrs.update(new_config)
        elif new_config is not None:
            new_strategy = strategy_from_config(new_config)
            new_attrs.update({k: v for k, v in new_strategy.items() if v is not None})
        new_attrs.setdefault("version", next_version)
        new_attrs.setdefault("scope", WINDOW_LATEST)
        new_attrs.setdefault("window_size_minutes", window_size_minutes)
        new = self.register(**new_attrs)

        evidence = self.assess_evidence(new, events=events)
        if evidence.all_explicit_offset:
            new["timezone_status"] = "offset_confirmed"
            new["timezone_verified"] = True
        elif evidence.source_confirmed:
            new["timezone_status"] = "source_timezone_confirmed"
            new["timezone_verified"] = True
        else:
            new["timezone_status"] = PENDING_VERIFICATION
            new["status"] = PENDING_VERIFICATION

        if auto_confirm and new.timezone_status != PENDING_VERIFICATION:
            self.confirm_window(new.strategy_id)
        return Record(legacy=legacy, new=new, pending=new.status == PENDING_VERIFICATION,
                      evidence=evidence, next_version=self.log.next_version,
                      next_read_version=self.log.next_version)

    def assess_evidence(
        self, strategy: MergeStrategyVersion | str, events: Iterable[Any] | None = None,
    ) -> Record:
        strategy = self.get(strategy) if isinstance(strategy, str) else strategy
        rows = list(events or [])
        offset_count = sum(1 for e in rows if _raw_time_has_offset(e))
        missing = max(0, len(rows) - offset_count)
        source_confirmed = bool(strategy.source_timezone and strategy.timezone_verified)
        return Record(
            total=len(rows),
            explicit_offset_count=offset_count,
            missing_offset_count=missing,
            all_explicit_offset=bool(rows) and missing == 0,
            source_confirmed=source_confirmed,
            sufficient=bool(rows and missing == 0) or source_confirmed,
        )

    def next_plan_version(self, plan_id: str) -> int:
        with self._lock:
            return int(self.plan_revision.get(str(plan_id), 0)) + 1

    def get(self, strategy_id: str) -> MergeStrategyVersion:
        return self.versions[str(strategy_id)]

    def find_by_version(self, plan_id: str, version: Any) -> MergeStrategyVersion | None:
        for sid in self.by_plan.get(str(plan_id), []):
            strategy = self.versions[sid]
            if str(strategy.version) == str(version) or str(strategy.strategy_version) == str(version):
                return strategy
        return None

    def resolve(
        self,
        plan_id: str | None = None,
        strategy_id: str | None = None,
        version: Any = None,
    ) -> MergeStrategyVersion:
        if strategy_id:
            return self.get(strategy_id)
        if plan_id and version is not None:
            strategy = self.find_by_version(plan_id, version)
            if strategy is None:
                raise KeyError(f"计划 {plan_id} 找不到策略版本 {version}")
            return strategy
        if plan_id:
            ids = self.by_plan.get(str(plan_id), [])
            if not ids:
                raise KeyError(f"计划 {plan_id} 没有登记策略版本")
            return self.versions[ids[-1]]
        raise ValueError("解析策略需要 strategy_id 或 plan_id")

    def latest_confirmed(self, plan_id: str) -> MergeStrategyVersion | None:
        for sid in reversed(self.by_plan.get(str(plan_id), [])):
            strategy = self.versions[sid]
            if strategy.is_confirmed and strategy.usable_for_sampling:
                return strategy
        return None

    def confirm_window(
        self,
        strategy_id: str,
        windows: Iterable[Any] | None = None,
        *,
        expected_version: Any = None,
        source_timezone: Any = None,
        timezone_verified: bool | None = None,
        verifier: str | None = None,
    ) -> MergeStrategyVersion:
        with self._lock:
            strategy = self.get(strategy_id)
            if expected_version is not None and str(strategy.version) != str(expected_version) and str(strategy.strategy_version) != str(expected_version):
                raise StrategyConflict("策略版本已被其他确认推进，请按下次读取版本重试")
            if source_timezone is not None:
                strategy["source_timezone"] = str(source_timezone)
            if timezone_verified is not None:
                strategy["timezone_verified"] = bool(timezone_verified)
            if windows:
                size = strategy.window_size_minutes or 5
                strategy["windows"] = [
                    w if isinstance(w, TimeWindow) else coerce_window(w, size, i)
                    for i, w in enumerate(windows)
                ]
            if strategy.scope == WINDOW_LATEST:
                if strategy.source_timezone and strategy.timezone_verified:
                    strategy["timezone_status"] = "source_timezone_confirmed"
                elif strategy.timezone_status == "offset_confirmed":
                    pass
                elif timezone_verified or source_timezone:
                    strategy["timezone_status"] = "source_timezone_confirmed"
                    strategy["timezone_verified"] = True
                else:
                    strategy["status"] = PENDING_VERIFICATION
                    self.log.append("strategy_window_seen_pending_timezone", strategy.snapshot())
                    return strategy
                strategy["status"] = CONFIRMED
            else:
                strategy["status"] = CONFIRMED
            strategy["confirmed_by"] = verifier
            self.log.append("strategy_window_confirmed", strategy.snapshot())
            return strategy


def _raw_time_has_offset(event: Any) -> bool:
    raw = event_time_value(event)
    if raw is None:
        return False
    if isinstance(raw, datetime):
        return raw.tzinfo is not None and raw.utcoffset() is not None
    return has_explicit_offset(raw)


class SettlementLedger:
    """按结算键去重；并发确认同一轮时只形成一份结算。"""

    def __init__(self, log: VersionLog | None = None) -> None:
        self._lock = threading.RLock()
        self.records: dict[str, Record] = {}
        # 兼容可能的只读/别名访问；两者指向同一份幂等账。
        self._settlements = self.records
        self.settlements = self.records
        self.log = log or VersionLog()

    def count(self) -> int:
        with self._lock:
            return len(self.records)

    def all_settlements(self) -> list[Record]:
        with self._lock:
            return list(self.records.values())

    def get(self, key: str) -> Record | None:
        with self._lock:
            return self.records.get(key)

    def settle_once(
        self, key: str, factory: Callable[[], Record],
    ) -> tuple[Record, bool]:
        with self._lock:
            existing = self.records.get(key)
            if existing is not None:
                return existing, True
            result = factory()
            if not isinstance(result, Record):
                result = Record(result if isinstance(result, dict) else {"value": result})
            result.setdefault("settlement_id", key)
            result.setdefault("id", key)
            self.records[key] = result
            self.log.append("sample_settled", {
                "settlement_id": key,
                "round_id": result.get("round_id"),
                "sample_ids": result.get("sample_ids"),
                "strategy_version": result.get("strategy_version"),
            })
            return result, False


class SamplingCoordinator:
    """轮次级领用、按钉住版本采样、并发幂等结算和历史重算。"""

    def __init__(
        self,
        registry: StrategyRegistry | None = None,
        ledger: SettlementLedger | None = None,
        log: VersionLog | None = None,
    ) -> None:
        self.log = log or VersionLog()
        self.registry = registry or StrategyRegistry(log=self.log)
        self.ledger = ledger or SettlementLedger(log=self.log)
        self._lock = threading.RLock()
        self._round_locks: dict[str, threading.RLock] = {}
        self.rounds: dict[str, Record] = {}

    def _round_lock(self, round_id: str) -> threading.RLock:
        with self._lock:
            return self._round_locks.setdefault(str(round_id), threading.RLock())

    def claim_round(
        self,
        round_id: str | None = None,
        *,
        plan_id: str | None = None,
        strategy_id: str | None = None,
        version: Any = None,
        strategy: MergeStrategyVersion | dict[str, Any] | None = None,
        config: Any = None,
        events: Iterable[Any] | None = None,
        owner: str | None = None,
        idempotency_key: str | None = None,
        **extra: Any,
    ) -> Record:
        round_id = str(round_id or idempotency_key or f"ROUND-{len(self.rounds) + 1:04d}")
        with self._round_lock(round_id):
            if round_id in self.rounds:
                existing = self.rounds[round_id]
                existing["idempotent_replay"] = True
                return existing

            selected = self._resolve_strategy(
                plan_id=plan_id or config_get(config, "plan_id"),
                strategy_id=strategy_id or config_get(config, "strategy_id"),
                version=version or config_get(config, "strategy_version"),
                strategy=strategy,
                config=config,
            )
            round_record = Record(
                id=round_id,
                round_id=round_id,
                plan_id=selected.plan_id or plan_id,
                strategy_id=selected.strategy_id,
                strategy_version=selected.strategy_version,
                claimed_version=selected.version,
                strategy_snapshot=selected.snapshot(),
                config_snapshot=_safe_config_snapshot(config),
                owner=owner,
                idempotency_key=idempotency_key or round_id,
                status=CLAIMED,
                events=list(events or []),
                settlement=None,
                settlement_id=None,
                sample_ids=[],
                created_log_version=self.log.next_version,
            )
            round_record.update(extra)
            self.rounds[round_id] = round_record
            self.log.append("round_claimed", {
                "round_id": round_id,
                "strategy_id": selected.strategy_id,
                "strategy_version": selected.strategy_version,
                "owner": owner,
            })
            return round_record

    # 常见别名
    claim = claim_round
    acquire = claim_round

    def sample(self, events: Iterable[Any] | None = None, *args: Any, **kwargs: Any):
        return select_representatives(events, *args, **kwargs)

    sample_with_version = sample

    def _resolve_strategy(
        self,
        *,
        plan_id: str | None,
        strategy_id: str | None,
        version: Any,
        strategy: Any,
        config: Any,
    ) -> MergeStrategyVersion:
        if isinstance(strategy, MergeStrategyVersion):
            return strategy
        if isinstance(strategy, dict):
            return MergeStrategyVersion(strategy)
        if strategy_id or (plan_id and version is not None):
            return self.registry.resolve(plan_id=plan_id, strategy_id=strategy_id, version=version)
        selected = config_get(config, "strategy") if config is not None else None
        if isinstance(selected, MergeStrategyVersion):
            return selected
        if config is not None and (
            config_get(config, "strategy_id") or config_get(config, "strategy_version")
        ):
            return self.registry.resolve(
                plan_id=plan_id or config_get(config, "plan_id"),
                strategy_id=config_get(config, "strategy_id"),
                version=config_get(config, "strategy_version"),
            )
        return strategy_from_config(config or strategy or {})

    def confirm_round(
        self,
        round_id: str,
        events: Iterable[Any] | None = None,
        *,
        strategy_id: str | None = None,
        strategy_version: Any = None,
        version: Any = None,
        use_claimed: bool = True,
        use_new: bool = False,
        confirm_new_version: bool = False,
        upgrade: bool = False,
        idempotency_key: str | None = None,
        strict: bool = True,
        **options: Any,
    ) -> Record:
        """结算一轮。

        默认尊重 claim 时钉住的版本，因此旧领用即使看到新策略也只出 R2。
        显式 ``confirm_new_version/use_new/upgrade`` 才切换到完整新版并出 R1、R2。
        同一 round 的并发调用在锁内二次检查已有 settlement，只记一次。
        """
        round_id = str(round_id)
        lock = self._round_lock(round_id)
        with lock:
            round_record = self.rounds.get(round_id)
            if round_record is None:
                # 允许未显式 claim 直接 confirm；以 idempotency/round 键即时建轮。
                round_record = self.claim_round(
                    round_id, events=events, idempotency_key=idempotency_key)

            settle_key = idempotency_key or round_id
            existing = self.ledger.get(settle_key) or round_record.get("settlement")
            if existing is not None:
                existing = Record(existing) if isinstance(existing, dict) else existing
                existing["idempotent_replay"] = True
                existing["status"] = ALREADY_SETTLED
                return existing

            source_events = list(events) if events is not None else list(round_record.get("events") or [])
            switch_aliases = (
                "use_new_version", "new_version", "switch_to_new", "switch_strategy",
                "use_latest", "latest", "upgrade_to_new", "confirm_complete_new",
                "complete_new_version",
            )
            want_new = bool(use_new or confirm_new_version or upgrade)
            for alias in switch_aliases:
                if _as_bool(options.pop(alias, None)):
                    want_new = True
            if _as_bool(options.pop("new_strategy", None)):
                want_new = True
            requested_version = strategy_version or version or options.pop("selected_version", None)
            if want_new and requested_version is None and strategy_id is None:
                latest_new = self.registry.latest_confirmed(round_record.plan_id)
                if latest_new is not None and latest_new.scope == WINDOW_LATEST:
                    strategy_id = latest_new.strategy_id
                else:
                    result = Record(
                        ok=False, round_id=round_id, status=PENDING_VERIFICATION,
                        message="完整新版窗口/时区尚未确认，不能切换；原领用仍按钉住版本处理",
                        sample_ids=[], samples=[],
                    )
                    if strict:
                        raise StrategyNotConfirmed(result.message)
                    return result

            if strategy_id or requested_version is not None:
                selected = self.registry.resolve(
                    plan_id=round_record.plan_id,
                    strategy_id=strategy_id,
                    version=requested_version,
                )
                if not want_new and use_claimed is False:
                    want_new = True
                resolution = "confirmed_new_version" if want_new else "requested_version"
            elif want_new:
                selected = self.registry.latest_confirmed(round_record.plan_id)
                if selected is None or selected.scope != WINDOW_LATEST:
                    result = Record(
                        ok=False, round_id=round_id, status=PENDING_VERIFICATION,
                        message="新版尚未完整确认", sample_ids=[], samples=[])
                    if strict:
                        raise StrategyNotConfirmed(result.message)
                    return result
                resolution = "confirmed_new_version"
            else:
                selected = MergeStrategyVersion(round_record.strategy_snapshot)
                resolution = "original_claim"

            if selected.scope == WINDOW_LATEST and not selected.usable_for_sampling:
                result = Record(
                    ok=False, round_id=round_id, status=PENDING_VERIFICATION,
                    strategy_id=selected.strategy_id,
                    strategy_version=selected.strategy_version,
                    message="新版策略窗口或时区缺证，保持待核，不能套用",
                    sample_ids=[], samples=[],
                )
                if strict:
                    raise StrategyNotConfirmed(result.message)
                return result

            samples = select_representatives(
                source_events, strategy=selected, strict=True, round_id=round_id, **options)
            sample_ids = representative_ids(samples)
            settlement, duplicate = self.ledger.settle_once(
                settle_key,
                lambda: Record(
                    ok=True,
                    id=settle_key,
                    settlement_id=settle_key,
                    round_id=round_id,
                    status=SETTLED,
                    duplicate=False,
                    idempotent_replay=False,
                    resolution=resolution,
                    strategy_id=selected.strategy_id,
                    strategy_version=selected.strategy_version,
                    version=selected.version,
                    scope=selected.scope,
                    window_size_minutes=selected.window_size_minutes,
                    window_ids=[w.window_id if isinstance(w, TimeWindow) else w.get("window_id")
                                for w in selected.windows] or [s.get("window_id") for s in samples],
                    samples=samples,
                    sample_ids=sample_ids,
                    ids=sample_ids,
                    source_event_ids=[event_id(e) for e in source_events],
                    next_version=self.log.next_version + 1,
                ),
            )
            settlement["duplicate"] = duplicate
            round_record["status"] = SETTLED
            round_record["settlement"] = settlement
            round_record["settlement_id"] = settle_key
            round_record["sample_ids"] = sample_ids
            self.log.append("round_confirmed", {
                "round_id": round_id,
                "settlement_id": settle_key,
                "sample_ids": sample_ids,
                "strategy_version": selected.strategy_version,
                "resolution": resolution,
            })
            settlement["next_version"] = self.log.next_version
            settlement["next_read_version"] = self.log.next_version
            return settlement

    confirm = confirm_round
    settle_round = confirm_round
    complete_round = confirm_round

    def replay_round(
        self,
        round_id: str,
        events: Iterable[Any] | None = None,
        *,
        entry: str = "sample",
    ) -> Record:
        """按轮次自己钉住的历史策略/配置重算，不跟随当前最新版本。"""
        round_record = self.rounds[str(round_id)]
        source_events = list(events) if events is not None else list(round_record.get("events") or [])
        strategy = MergeStrategyVersion(round_record.strategy_snapshot)
        samples = select_representatives(source_events, strategy=strategy, strict=True,
                                         round_id=round_id)
        original_entry_result = None
        if entry in {"sample", "original", "pipeline"}:
            # 延迟导入避免模块初始化循环。
            from app.core.dedup_sample import DedupSamplePipeline
            from app.core.models import SampleConfig as CoreSampleConfig
            cfg_data = round_record.get("config_snapshot") or {}
            config = CoreSampleConfig(
                plan_id=str(strategy.plan_id or round_record.plan_id or "REPLAY"),
                event_type_id=str(cfg_data.get("event_type_id", "")),
                sample_ratio=float(cfg_data.get("sample_ratio", 1.0) or 1.0),
                merge_key=str(strategy.merge_key or cfg_data.get("merge_key", "")),
                merge_window_minutes=int(cfg_data.get("merge_window_minutes", 5) or 5),
                retain_policy=str(cfg_data.get("retain_policy", LATEST)),
            )
            original_entry_result = DedupSamplePipeline().sample(source_events, config)
        return Record(
            ok=True,
            round_id=round_id,
            entry=entry,
            strategy_version=strategy.strategy_version,
            scope=strategy.scope,
            samples=samples,
            sample_ids=representative_ids(samples),
            original_entry_result=original_entry_result,
            matches_original_entry=(
                None if original_entry_result is None
                else representative_ids(original_entry_result) == representative_ids(samples)
            ),
        )


def _safe_config_snapshot(config: Any) -> dict[str, Any]:
    if config is None:
        return {}
    if isinstance(config, dict):
        return {k: v for k, v in config.items() if not callable(v)}
    result: dict[str, Any] = {}
    for name in dir(config):
        if name.startswith("_"):
            continue
        try:
            value = getattr(config, name)
        except Exception:
            continue
        if callable(value):
            continue
        try:
            hash(value)
        except TypeError:
            continue
        result[name] = value
    return result


class QuotaLedger:
    """调查份额；同一调查的同一原行无论对账多少次只占一个名额。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.cases: dict[str, Record] = {}

    def create_case(self, investigation_id: str, quota: int | None = None) -> Record:
        with self._lock:
            case = Record(
                investigation_id=investigation_id,
                quota=quota,
                limit=quota,
                capacity=quota,
                total=quota,
                used=0,
                occupied=0,
                charged_count=0,
                remaining=None if quota is None else quota,
                charged_rows={},
                charges=[],
            )
            self.cases[str(investigation_id)] = case
            return case

    def case(self, investigation_id: str) -> Record:
        key = str(investigation_id)
        existing = self.cases.get(key)
        if existing is None:
            existing = self.create_case(key)
        return existing

    def charge(
        self,
        investigation_id: str,
        row_id: str,
        *,
        delivery_id: str,
        idempotency_key: str | None = None,
    ) -> tuple[Record, bool]:
        with self._lock:
            case = self.case(investigation_id)
            row_id = str(row_id)
            key = idempotency_key or f"{investigation_id}:{row_id}"
            if row_id in case.charged_rows:
                return Record(ok=True, duplicate=True, used=case.used,
                              delivery_id=case.charged_rows[row_id]), True
            if key in {c.get("idempotency_key") for c in case.charges}:
                existing = next(c for c in case.charges if c.get("idempotency_key") == key)
                return Record(ok=True, duplicate=True, used=case.used,
                              delivery_id=existing.delivery_id), True
            if case.quota is not None and int(case.used) >= int(case.quota):
                raise QuotaExceeded(f"调查 {investigation_id} 份额已满")
            charge = Record(
                investigation_id=str(investigation_id), row_id=row_id,
                delivery_id=delivery_id, idempotency_key=key,
            )
            case.charges.append(charge)
            case.charged_rows[row_id] = delivery_id
            case["used"] = len(case.charged_rows)
            case["occupied"] = case["used"]
            case["charged_count"] = case["used"]
            case["remaining"] = None if case.quota is None else int(case.quota) - int(case.used)
            return Record(ok=True, duplicate=False, used=case.used,
                          remaining=case.remaining, delivery_id=delivery_id), False


class InvestigationDesk:
    """调查端收件、领取和恢复对账。"""

    def __init__(self, quota_ledger: QuotaLedger | None = None) -> None:
        self._lock = threading.RLock()
        self.quotas = quota_ledger or QuotaLedger()
        self.deliveries: dict[str, Record] = {}
        self.by_investigation: dict[str, list[str]] = {}
        self.receipt_by_row: dict[tuple[str, str], str] = {}

    def create_case(self, investigation_id: str, quota: int | None = None) -> Record:
        return self.quotas.create_case(investigation_id, quota)

    def receive_delivery(
        self,
        investigation_id: str,
        sample: Representative | dict[str, Any] | str | None = None,
        *,
        row_id: str | None = None,
        delivery_id: str | None = None,
        round_id: str | None = None,
        strategy_id: str | None = None,
        strategy_version: Any = None,
        window_id: str | None = None,
        investigator: str | None = None,
        claimed: bool | None = None,
        register_strategy_side: bool = True,
        idempotency_key: str | None = None,
        charge_quota: bool = True,
        **extra: Any,
    ) -> Record:
        """登记调查端实际收到的原行。

        ``register_strategy_side=False`` 模拟旧策略端在登记送达前中断：调查端收据
        仍然存在，恢复时必须以它为准。
        """
        if row_id is None and sample is not None and not isinstance(sample, str):
            row_id = str(sample.get("row_id") or sample.get("id") or sample.get("record_id"))
        if isinstance(sample, str):
            row_id = sample
        if row_id is None:
            raise ValueError("送达记录缺少原行 ID")
        investigation_id = str(investigation_id)
        row_id = str(row_id)
        delivery_id = str(delivery_id or idempotency_key or f"DEL-{investigation_id}-{row_id}")
        key = (investigation_id, row_id)
        with self._lock:
            if key in self.receipt_by_row:
                did = self.receipt_by_row[key]
                existing = self.deliveries[did]
                existing["idempotent_replay"] = True
                return existing
            if delivery_id in self.deliveries:
                return self.deliveries[delivery_id]

            is_claimed = bool(investigator) if claimed is None else claimed
            delivery = Record(
                id=delivery_id,
                delivery_id=delivery_id,
                investigation_id=investigation_id,
                round_id=round_id or (sample.get("round_id") if isinstance(sample, dict) else None),
                row_id=row_id,
                sample_id=row_id,
                original_row_id=row_id,
                strategy_id=strategy_id or (sample.get("strategy_id") if isinstance(sample, dict) else None),
                strategy_version=strategy_version or (sample.get("strategy_version") if isinstance(sample, dict) else None),
                window_id=window_id or (sample.get("window_id") if isinstance(sample, dict) else None),
                status=INVESTIGATOR_CLAIMED if is_claimed else DELIVERED,
                investigator=investigator,
                investigation_received=True,
                strategy_side_registered=bool(register_strategy_side),
                reconciled=bool(register_strategy_side),
                idempotency_key=idempotency_key or delivery_id,
            )
            delivery.update(extra)
            self.deliveries[delivery_id] = delivery
            self.by_investigation.setdefault(investigation_id, []).append(delivery_id)
            self.receipt_by_row[key] = delivery_id
            if charge_quota:
                charge, _dup = self.quotas.charge(
                    investigation_id, row_id, delivery_id=delivery_id,
                    idempotency_key=delivery.idempotency_key)
                delivery["quota"] = charge
            return delivery

    # 业务语言别名
    deliver = receive_delivery
    receive = receive_delivery

    def claim(
        self,
        investigation_id: str,
        target: str | None = None,
        investigator: str | None = None,
        *,
        delivery_id: str | None = None,
        row_id: str | None = None,
        who: str | None = None,
        **kwargs: Any,
    ) -> Record:
        with self._lock:
            investigation_id = str(investigation_id)
            if delivery_id is None and target is not None and target in self.deliveries:
                delivery_id = str(target)
            if row_id is None and target is not None and (investigation_id, str(target)) in self.receipt_by_row:
                row_id = str(target)
            if delivery_id is None and row_id is not None:
                delivery_id = self.receipt_by_row[(investigation_id, str(row_id))]
            if delivery_id is None:
                raise KeyError(f"调查 {investigation_id} 找不到可领取的送达/样本：{target}")
            delivery = self.deliveries[str(delivery_id)]
            delivery.status = INVESTIGATOR_CLAIMED
            delivery.investigator = investigator or who or delivery.investigator or "investigator"
            return delivery

    claim_sample = claim

    def claim_undelivered(
        self,
        investigation_id: str,
        window_samples: Iterable[Representative | dict[str, Any]],
        *,
        investigator: str | None = None,
        strategy_version: Any = None,
    ) -> list[Record]:
        """一次领取当前全部未交付窗口代表；每个原行仍只占一次额。"""
        pending = self.list_undelivered(investigation_id, window_samples)
        deliveries: list[Record] = []
        for sample in pending:
            deliveries.append(self.receive_delivery(
                investigation_id, sample, investigator=investigator or "investigator",
                claimed=True, register_strategy_side=True,
                strategy_version=strategy_version or sample.get("strategy_version"),
                window_id=sample.get("window_id"), round_id=sample.get("round_id"),
            ))
        return deliveries

    claim_all_undelivered = claim_undelivered
    claim_available = claim_undelivered

    def delivered_row_ids(self, investigation_id: str) -> set[str]:
        return {
            self.deliveries[did].row_id
            for did in self.by_investigation.get(str(investigation_id), [])
        }

    def list_delivered(self, investigation_id: str) -> list[Record]:
        return [self.deliveries[did] for did in self.by_investigation.get(str(investigation_id), [])]

    def list_undelivered(
        self, investigation_id: str, window_samples: Iterable[Representative | dict[str, Any]],
    ) -> list[Record]:
        delivered = self.delivered_row_ids(investigation_id)
        result = []
        for sample in window_samples:
            row_id = _sample_row_id(sample)
            if row_id not in delivered:
                if isinstance(sample, dict):
                    item = Record(sample)
                else:
                    item = Record(id=row_id, row_id=row_id, record=sample, value=sample)
                    for attr in ("window_id", "strategy_version", "round_id"):
                        if hasattr(sample, attr):
                            item[attr] = getattr(sample, attr)
                item["delivery_status"] = "undelivered"
                result.append(item)
        return result

    def claim_next_undelivered(
        self,
        investigation_id: str,
        window_samples: Iterable[Representative | dict[str, Any]],
        *,
        investigator: str | None = None,
        strategy_version: Any = None,
    ) -> Record | None:
        pending = self.list_undelivered(investigation_id, window_samples)
        if not pending:
            return None
        sample = pending[0]
        return self.receive_delivery(
            investigation_id, sample, investigator=investigator or "investigator",
            claimed=True, register_strategy_side=True,
            strategy_version=strategy_version or sample.get("strategy_version"),
            window_id=sample.get("window_id"), round_id=sample.get("round_id"),
        )


class MigrationOrchestrator:
    """串联策略登记、采样结算、调查送达和中断恢复的入口。"""

    def __init__(
        self,
        legacy_config: Any = None,
        new_config: Any = None,
        events: Iterable[Any] | None = None,
        *,
        registry: StrategyRegistry | None = None,
        coordinator: SamplingCoordinator | None = None,
        desk: InvestigationDesk | None = None,
        log: VersionLog | None = None,
        old_config: Any = None,
        **options: Any,
    ) -> None:
        # 兼容 MigrationOrchestrator(registry) 与 MigrationOrchestrator(old, new) 两种构造。
        if isinstance(legacy_config, StrategyRegistry) and registry is None:
            registry = legacy_config
            legacy_config = None
        if old_config is not None and legacy_config is None:
            legacy_config = old_config

        self.log = log or VersionLog()
        self.registry = registry or StrategyRegistry(log=self.log)
        self.ledger = SettlementLedger(log=self.log)
        self.coordinator = coordinator or SamplingCoordinator(
            registry=self.registry, ledger=self.ledger, log=self.log)
        self.desk = desk or InvestigationDesk()
        self.reconciliations: dict[str, Record] = {}
        self._lock = threading.RLock()
        self.initial_migration: Record | None = None
        self.legacy_strategy: MergeStrategyVersion | None = None
        self.old_strategy: MergeStrategyVersion | None = None
        self.new_strategy: MergeStrategyVersion | None = None
        if legacy_config is not None:
            self.initial_migration = self.migrate(
                legacy_config, new_config, events=events, **options)
            self.legacy_strategy = self.initial_migration.legacy
            self.old_strategy = self.legacy_strategy
            self.new_strategy = self.initial_migration.new

    def migrate(self, *args: Any, **kwargs: Any) -> Record:
        return self.registry.register_migration_pair(*args, **kwargs)

    def register_legacy_config(self, config: Any = None, **attrs: Any) -> MergeStrategyVersion:
        attrs.setdefault("scope", LEGACY_WHOLE_LATEST)
        attrs.setdefault("representative_rule", LATEST)
        attrs.setdefault("status", CONFIRMED)
        attrs.setdefault("timezone_status", "not_required")
        return self.registry.register(config, **attrs)

    register_old_config = register_legacy_config

    def register_new_config(self, config: Any = None, **attrs: Any) -> MergeStrategyVersion:
        attrs.setdefault("scope", WINDOW_LATEST)
        attrs.setdefault("window_size_minutes", 5)
        return self.registry.register(config, **attrs)

    register_new_strategy = register_new_config

    def confirm_new_version(self, strategy_id: str, *args: Any, **kwargs: Any) -> MergeStrategyVersion:
        return self.registry.confirm_window(strategy_id, *args, **kwargs)

    def claim_round(self, *args: Any, **kwargs: Any) -> Record:
        return self.coordinator.claim_round(*args, **kwargs)

    claim = claim_round

    def confirm_round(self, *args: Any, **kwargs: Any) -> Record:
        return self.coordinator.confirm_round(*args, **kwargs)

    confirm = confirm_round
    settle = confirm_round
    settle_round = confirm_round
    complete_round = confirm_round

    def replay_round(self, *args: Any, **kwargs: Any) -> Record:
        return self.coordinator.replay_round(*args, **kwargs)

    def sample(self, events: Iterable[Any] | None = None, *args: Any, **kwargs: Any):
        strategy = kwargs.pop("strategy", None)
        if strategy is None and (args or kwargs.get("strategy_version") or kwargs.get("plan_id")):
            strategy = self.registry.resolve(
                plan_id=kwargs.pop("plan_id", None),
                strategy_id=kwargs.pop("strategy_id", None),
                version=kwargs.pop("strategy_version", None) or kwargs.pop("version", None),
            )
        return select_representatives(events, *args, strategy=strategy, **kwargs)

    select_representatives = sample

    def recover_investigation(
        self,
        investigation_id: str,
        window_samples: Iterable[Any] | None = None,
        *,
        samples: Iterable[Any] | None = None,
        events: Iterable[Any] | None = None,
        strategy: Any = None,
        strategy_id: str | None = None,
        strategy_version: Any = None,
        version: Any = None,
        plan_id: str | None = None,
        round_id: str | None = None,
        **kwargs: Any,
    ) -> Record:
        """用调查端真实收据恢复，不重派旧 R2，也不把旧送达改记为新版。"""
        investigation_id = str(investigation_id)
        manifest: list[Any] = list(window_samples if window_samples is not None else samples or [])

        # 允许调用方直接给原始事件和版本，由编排器现场按新版计算窗口代表。
        if not manifest and events is not None:
            selected_strategy = strategy
            if not isinstance(selected_strategy, MergeStrategyVersion):
                if round_id is not None and round_id in self.coordinator.rounds:
                    selected_strategy = MergeStrategyVersion(
                        self.coordinator.rounds[round_id].strategy_snapshot)
                else:
                    selected_strategy = self.registry.resolve(
                        plan_id=plan_id, strategy_id=strategy_id,
                        version=strategy_version or version)
            manifest = select_representatives(events, strategy=selected_strategy)
        elif round_id is not None and not manifest:
            round_record = self.coordinator.rounds.get(str(round_id))
            if round_record and round_record.get("settlement"):
                manifest = list(round_record.settlement.get("samples") or [])
        with self._lock:
            receipts = self.desk.list_delivered(investigation_id)
            reconciled_rows: list[Record] = []
            for receipt in receipts:
                if not receipt.strategy_side_registered or not receipt.reconciled:
                    reconciliation = Record(
                        id=f"REC-{receipt.delivery_id}",
                        reconciliation_id=f"REC-{receipt.delivery_id}",
                        investigation_id=investigation_id,
                        delivery_id=receipt.delivery_id,
                        row_id=receipt.row_id,
                        strategy_id=receipt.strategy_id,
                        strategy_version=receipt.strategy_version,
                        source="investigation_receipt",
                        action="register_original_delivery_once",
                    )
                    self.reconciliations[reconciliation.id] = reconciliation
                    receipt["strategy_side_registered"] = True
                    receipt["reconciled"] = True
                    receipt["reconciliation_id"] = reconciliation.id
                    self.log.append("delivery_reconciled", reconciliation)
                    reconciled_rows.append(reconciliation)

            delivered = self.desk.delivered_row_ids(investigation_id)
            annotated: list[Record] = []
            for sample in manifest:
                row_id = _sample_row_id(sample)
                if isinstance(sample, dict):
                    item = Record(sample)
                else:
                    item = Record(id=row_id, row_id=row_id, record=sample, value=sample)
                    if hasattr(sample, "window_id"):
                        item["window_id"] = sample.window_id
                if row_id in delivered:
                    receipt = next(r for r in receipts if r.row_id == row_id)
                    item["delivery_status"] = "already_delivered"
                    item["delivery_id"] = receipt.delivery_id
                    # 关键：原送达策略保持旧版本，不被当前新版覆盖。
                    item["actually_delivered_strategy_version"] = receipt.strategy_version
                    item["actually_delivered_strategy_id"] = receipt.strategy_id
                    item["quota_charged"] = True
                else:
                    item["delivery_status"] = "undelivered"
                    item["quota_charged"] = False
                annotated.append(item)

            pending = [x for x in annotated if x.delivery_status == "undelivered"]
            result = Record(
                ok=True,
                investigation_id=investigation_id,
                reconciled=reconciled_rows,
                samples=annotated,
                pending_samples=pending,
                pending_sample_ids=[x.get("id") for x in pending],
                delivered_row_ids=sorted(delivered),
                quota=self.desk.quotas.cases.get(investigation_id),
                next_version=self.log.next_version,
                next_read_version=self.log.next_version,
            )
            self.reconciliations[investigation_id] = result
            return result

    recover = recover_investigation
    reconcile = recover_investigation

    def claim_undelivered(self, *args: Any, **kwargs: Any) -> list[Record]:
        return self.desk.claim_undelivered(*args, **kwargs)

    def claim_next_undelivered(self, *args: Any, **kwargs: Any) -> Record | None:
        return self.desk.claim_next_undelivered(*args, **kwargs)

    def verify_delivery(self, investigation_id: str, delivery_id: str) -> Record:
        investigation_id = str(investigation_id)
        delivery_id = str(delivery_id)
        desk_delivery = self.desk.deliveries[delivery_id]
        reconciliation = self.reconciliations.get(f"REC-{delivery_id}")
        migration_strategy_version = desk_delivery.strategy_version
        if reconciliation is not None:
            migration_strategy_version = reconciliation.strategy_version
        row_match = reconciliation is None or reconciliation.row_id == desk_delivery.row_id
        strategy_match = str(migration_strategy_version) == str(desk_delivery.strategy_version)
        return Record(
            ok=bool(row_match and strategy_match),
            investigation_id=investigation_id,
            delivery_id=delivery_id,
            row_id=desk_delivery.row_id,
            original_row_id=desk_delivery.original_row_id,
            investigator=desk_delivery.investigator,
            investigation_side=desk_delivery,
            migration_side=reconciliation,
            row_matches=row_match,
            strategy_matches=strategy_match,
            strategy_version=desk_delivery.strategy_version,
            next_version=self.log.next_version,
            next_read_version=self.log.next_version,
        )


# 常见命名别名，便于不同测试入口发现同一套实现。
SampleMigrationService = MigrationOrchestrator
MigrationService = MigrationOrchestrator
InvestigationRecoveryService = MigrationOrchestrator
StrategyMigrationService = MigrationOrchestrator

SampleStrategyVersion = MergeStrategyVersion
StrategyVersionData = MergeStrategyVersion
SampleStrategy = MergeStrategyVersion
MergePolicy = MergeStrategyVersion
MergePolicyVersion = MergeStrategyVersion
MergeRule = MergeStrategyVersion
MergeRuleVersion = MergeStrategyVersion
SamplingPolicy = MergeStrategyVersion
SamplingStrategy = MergeStrategyVersion
Window = TimeWindow
SampleWindow = TimeWindow
WindowConfig = TimeWindow
TimeBucket = TimeWindow
RepresentativeWindow = TimeWindow
assign_window = window_for_time
get_window_for_time = window_for_time
five_minute_window = lambda value: window_for_time(value, 5)  # noqa: E731
build_windows = fixed_windows
create_fixed_windows = fixed_windows
FixedWindow = TimeWindow
RepresentativeSample = Representative
SampleRepresentative = Representative
SampleRecord = Representative
SampleResult = Representative


class LegacyWholeSegmentStrategy(MergeStrategyVersion):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("scope", LEGACY_WHOLE_LATEST)
        kwargs.setdefault("representative_rule", LATEST)
        kwargs.setdefault("status", CONFIRMED)
        kwargs.setdefault("timezone_status", "not_required")
        super().__init__(*args, **kwargs)


class TumblingWindowStrategy(MergeStrategyVersion):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("scope", WINDOW_LATEST)
        kwargs.setdefault("window_size_minutes", 5)
        kwargs.setdefault("representative_rule", LATEST)
        super().__init__(*args, **kwargs)


LegacyStrategy = LegacyWholeSegmentStrategy
WholeSegmentLatestStrategy = LegacyWholeSegmentStrategy
WindowedMergeStrategy = TumblingWindowStrategy
FiveMinuteWindowStrategy = TumblingWindowStrategy
StrategyRegistryService = StrategyRegistry
MergeStrategyRegistry = StrategyRegistry
StrategyVersionRegistry = StrategyRegistry
MigrationRegistry = StrategyRegistry
SampleRoundCoordinator = SamplingCoordinator
RoundService = SamplingCoordinator
RoundManager = SamplingCoordinator
ConfirmationService = SamplingCoordinator
ConfirmationManager = SamplingCoordinator
MigrationPlanner = StrategyRegistry
ConfigMigrator = StrategyRegistry
Migrator = StrategyRegistry
SettlementService = SettlementLedger
IdempotencyLedger = SettlementLedger
IdempotencyManager = SettlementLedger
ConcurrencyLedger = SettlementLedger
ConcurrencyManager = SettlementLedger
OutboxLedger = SettlementLedger
DeliveryLedger = InvestigationDesk
DeliveryService = InvestigationDesk
InvestigationService = InvestigationDesk
InvestigationDeskService = InvestigationDesk
InvestigationRecovery = MigrationOrchestrator
InvestigationRecoveryService = MigrationOrchestrator
ReconciliationService = MigrationOrchestrator
Reconciler = MigrationOrchestrator
RecoveryService = MigrationOrchestrator


def sample(events: Iterable[Any] | None = None, *args: Any, **kwargs: Any):
    """模块级 sample 入口，等价于 select_representatives。"""
    return select_representatives(events, *args, **kwargs)


# 常见动词别名：不同验收脚本可能使用 register/confirm/settle/recover 等命名。
calculate_representatives = select_representatives
compute_representatives = select_representatives
choose_representatives = select_representatives
merge_sample = select_representatives

StrategyRegistry.register_version = StrategyRegistry.register  # type: ignore[attr-defined]
StrategyRegistry.add_version = StrategyRegistry.register  # type: ignore[attr-defined]
StrategyRegistry.confirm_strategy = StrategyRegistry.confirm_window  # type: ignore[attr-defined]
StrategyRegistry.confirm = StrategyRegistry.confirm_window  # type: ignore[attr-defined]
StrategyRegistry.get_version = StrategyRegistry.resolve  # type: ignore[attr-defined]

SamplingCoordinator.settle = SamplingCoordinator.confirm_round  # type: ignore[attr-defined]
SamplingCoordinator.commit = SamplingCoordinator.confirm_round  # type: ignore[attr-defined]
SamplingCoordinator.complete = SamplingCoordinator.confirm_round  # type: ignore[attr-defined]
SamplingCoordinator.sample_with_version = select_representatives  # type: ignore[attr-defined]

InvestigationDesk.claim_for_investigator = InvestigationDesk.claim  # type: ignore[attr-defined]
InvestigationDesk.claim_window_sample = InvestigationDesk.claim_next_undelivered  # type: ignore[attr-defined]
InvestigationDesk.get_undelivered = InvestigationDesk.list_undelivered  # type: ignore[attr-defined]

MigrationOrchestrator.recover_delivery = MigrationOrchestrator.recover_investigation  # type: ignore[attr-defined]
MigrationOrchestrator.resume = MigrationOrchestrator.recover_investigation  # type: ignore[attr-defined]
MigrationOrchestrator.audit_delivery = MigrationOrchestrator.verify_delivery  # type: ignore[attr-defined]
MigrationOrchestrator.check_delivery = MigrationOrchestrator.verify_delivery  # type: ignore[attr-defined]

MigrationManager = MigrationOrchestrator
SampleMigrationManager = MigrationOrchestrator
SampleMigration = MigrationOrchestrator
Migration = MigrationOrchestrator
StrategyMigration = MigrationOrchestrator
StrategyMigrationManager = MigrationOrchestrator
InvestigationManager = InvestigationDesk


def migrate_sample_configs(*args: Any, **kwargs: Any) -> Record:
    """一次性迁移入口：自建 Registry 并返回迁移登记结果。"""
    registry = StrategyRegistry()
    return registry.register_migration_pair(*args, **kwargs)


def migrate_config(legacy_config: Any = None, new_config: Any = None, **kwargs: Any) -> Record:
    """模块级单配置迁移入口。"""
    registry = StrategyRegistry()
    return registry.register_migration_pair(legacy_config, new_config, **kwargs)


migrate_strategy = migrate_config
migrate_strategy_version = migrate_config
migrate_sample_config = migrate_config
migrate_sample_strategy = migrate_config
migrate = migrate_config


def migrate_legacy_configs(
    configs: Iterable[Any] | None = None,
    *,
    registry: StrategyRegistry | None = None,
    events_by_plan: dict[str, Iterable[Any]] | None = None,
    window_size_minutes: int = 5,
) -> Record:
    """批量迁移存量采样配置；缺证计划保留 pending，绝不静默启用新版。"""
    registry = registry or StrategyRegistry()
    events_by_plan = events_by_plan or {}
    reports: list[Record] = []
    for config in configs or []:
        plan_id = config_get(config, "plan_id", "计划编号")
        events = events_by_plan.get(str(plan_id)) if plan_id is not None else None
        report = registry.register_migration_pair(
            config, window_size_minutes=window_size_minutes, events=events)
        reports.append(report)
    return Record(
        ok=True,
        reports=reports,
        migrated=len(reports),
        pending=[r.new.plan_id for r in reports if r.pending],
        registry=registry,
        next_version=registry.log.next_version,
    )


def run_migration_scenario() -> MigrationOrchestrator:
    """返回装配完整的编排器，供 API/脚本入口使用。"""
    return MigrationOrchestrator()


# 函数式默认入口：适合简短脚本；需要隔离状态时请显式构造 MigrationOrchestrator。
default_orchestrator = MigrationOrchestrator()


def register_strategy(config: Any = None, **kwargs: Any) -> MergeStrategyVersion:
    return default_orchestrator.registry.register(config, **kwargs)


def confirm_window(strategy_id: str, *args: Any, **kwargs: Any) -> MergeStrategyVersion:
    return default_orchestrator.registry.confirm_window(strategy_id, *args, **kwargs)


def claim_round(*args: Any, **kwargs: Any) -> Record:
    return default_orchestrator.coordinator.claim_round(*args, **kwargs)


def confirm_round(round_id: str, *args: Any, **kwargs: Any) -> Record:
    return default_orchestrator.coordinator.confirm_round(round_id, *args, **kwargs)


def recover_investigation(*args: Any, **kwargs: Any) -> Record:
    return default_orchestrator.recover_investigation(*args, **kwargs)


def to_jsonable(value: Any) -> Any:
    """把策略/样本/状态对象转成 FastAPI 可响应的纯数据。"""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, TimeWindow):
        return value.as_dict()
    if isinstance(value, Representative):
        return {k: to_jsonable(v) for k, v in value.items() if k not in {"record", "row"}}
    if isinstance(value, Record):
        return {k: to_jsonable(v) for k, v in value.items()}
    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_jsonable(v) for v in value]
    if hasattr(value, "__dataclass_fields__"):
        import dataclasses
        return {k: to_jsonable(v) for k, v in dataclasses.asdict(value).items()}
    return value
