"""权威交易日历：新浪 klc_td_sh.txt 解码器 + 校验（纯标准库）。

akshare `hk_js_decode` 中 R 路径（selector 139）的逐行直译，不引入
akshare/pandas/JS 运行时依赖。抓取与刷新由后续任务接线，本模块只做解码 / 校验。
"""

import contextlib
import json
import logging
import os
import re
import time
from datetime import date, datetime, timedelta
from itertools import pairwise

import requests

logger = logging.getLogger(__name__)

MISSING_KNOWN = [date(1992, 5, 4)]

_B64_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
_B64_INDEX = {ch: i for i, ch in enumerate(_B64_ALPHABET)}

_EPOCH = date(1970, 1, 1)
_EPOCH_OFFSET = 7657


class _BitReader:
    """JS 解码器的位流状态（`i`/`e`/`o`/`n`/`r.d`/`r.l` 直译）。"""

    def __init__(self, payload: str) -> None:
        vals = []
        for ch in payload.strip():
            v = _B64_INDEX.get(ch)
            if v is None:
                raise ValueError(f"illegal base64 char: {ch!r}")
            vals.append(v)
        self._i = vals
        self._n = len(vals)
        self._e = 0
        self._o = 0
        self.r_d = 0
        self.r_l = 0

    def y(self) -> bool:
        """读 1 bit；流耗尽时返回 False（JS：`e >= n ? 0`）。"""
        if self._e >= self._n:
            return False
        t = self._i[self._e] & (1 << self._o)
        self._o += 1
        if self._o >= 6:
            self._o -= 6
            self._e += 1
        return bool(t)

    def w(self, widths: list[int], signed: list[int] | None = None) -> list[int]:
        """按宽逐域 LSB 组装；signed 域做补码还原。"""
        out: list[int] = []
        signed = signed or []
        for s, width in enumerate(widths):
            if not width:
                out.append(0)
                continue
            if self._e >= self._n:
                return out
            if width <= 0:
                out.append(0)
            elif width <= 30:
                total = width
                c = width
                u = 0
                while True:
                    d = 6 - self._o
                    if c < d:
                        d = c
                    u |= ((self._i[self._e] >> self._o) & ((1 << d) - 1)) << (total - c)
                    self._o += d
                    if self._o >= 6:
                        self._o -= 6
                        self._e += 1
                    c -= d
                    if c <= 0:
                        break
                if s < len(signed) and signed[s] and u >= (1 << (total - 1)):
                    u -= 1 << total
                out.append(u)
            else:
                raise ValueError(f"unsupported width: {width}")
        return out

    def read_n(self) -> int:
        """JS `N()`：一元编码读有符号增量。"""
        t = self.y()
        e = 1
        while True:
            if not self.y():
                return e if t else -e
            e += 1

    def step(self, count: int) -> str:
        """JS `x(t)`：向前走 count 个工作日（跳周末），返回 YYYY-MM-DD。"""
        for _ in range(count):
            self.r_d += 1
            m = self.r_d % 7
            if m == 3 or m == 4:
                self.r_d += 5 - m
        return (_EPOCH + timedelta(days=_EPOCH_OFFSET + self.r_d)).isoformat()


def decode_sina_calendar(payload: str) -> list[str]:
    """解码新浪交易日历 payload，返回升序 `YYYY-MM-DD` 列表（含补丁日期）。"""
    r = _BitReader(payload)
    u = r.w([12, 6])
    if len(u) < 2:
        raise ValueError("truncated payload")
    s = 63 ^ u[1]
    if u[0] != 139 or s > 1:
        raise ValueError("unsupported selector")
    head = r.w([18])
    tail = r.w([18])
    if not head or not tail:
        raise ValueError("truncated payload")
    r.r_d = head[0] - 1
    end = tail[0]
    run_left = -1
    out: list[str] = []
    first = True
    while r.r_d < end:
        cur = r.step(1)
        if run_left <= 0:
            if r.y():
                r.r_l += r.read_n()
            nxt = r.w([3 * r.r_l])
            if not nxt:
                raise ValueError("truncated payload")
            run_left = nxt[0] + 1
            if first:
                out.append(cur)
                first = False
                run_left -= 1
        else:
            out.append(cur)
        run_left -= 1
    seen = set(out)
    for m in MISSING_KNOWN:
        iso = m.isoformat()
        if iso not in seen:
            out.append(iso)
            seen.add(iso)
    out.sort()
    return out


def validate_calendar(dates: list[str], today: date | None = None) -> None:
    """校验日历合法性；任一规则不过抛 `ValueError`，合法返回 None。"""
    if not 8000 <= len(dates) <= 9000:
        raise ValueError(f"bad length: {len(dates)}")
    parsed = []
    for d in dates:
        try:
            parsed.append(date.fromisoformat(d))
        except (ValueError, TypeError):
            raise ValueError(f"bad date: {d!r}") from None
    for a, b in pairwise(parsed):
        if b <= a:
            raise ValueError(f"not strictly increasing: {a} then {b}")
    if "1992-05-04" not in dates:
        raise ValueError("missing 1992-05-04")
    if today is None:
        today = date.today()
    last = parsed[-1]
    if (today - last).days > 400:
        raise ValueError(f"stale end: {last}")
    known = set(dates)
    for probe in (f"{last.year}-01-01", f"{last.year}-10-01"):
        if probe in known:
            raise ValueError(f"holiday traded: {probe}")
    return None


# --- 抓取 / 刷新 / 探测 / 对账 / 钉钉周频上限（Task 2） ---

SINA_CAL_URL = "https://finance.sina.com.cn/realstock/company/klc_td_sh.txt"
TENCENT_DAY_URL = "https://ifzq.gtimg.cn/appstock/app/fqkline/get?param="
_SINA_HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                 "Referer": "https://finance.sina.com.cn"}
_TX_HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
               "Referer": "https://gu.qq.com/"}
_HTTP_TIMEOUT = 15.0
_MAX_RETRIES = 3
_BACKOFF = (2.0, 5.0, 10.0)


def fetch_sina_payload() -> str:
    last = None
    for wait in (0, *_BACKOFF[:_MAX_RETRIES - 1]):
        if wait:
            time.sleep(wait)
        try:
            r = requests.get(SINA_CAL_URL, headers=_SINA_HEADERS, timeout=_HTTP_TIMEOUT)
            r.raise_for_status()
            m = re.search(r'var datelist="([^"]+)"', r.text)
            if not m:
                raise ValueError("datelist not found in sina response")
            return m.group(1)
        except Exception as e:
            last = e
    raise ValueError(f"sina calendar fetch failed after retries: {last}")


def calendar_path() -> str:
    env = os.environ.get("TRADE_CALENDAR_PATH")
    if env:
        return env
    from app.quant.config import CONFIG
    return os.path.join(os.path.dirname(CONFIG.db_path), "trade_calendar.json")


def _seed_path() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(os.path.dirname(here), "quant", "trade_calendar_seed.json")


def load_calendar_file(path: str | None = None) -> dict:
    """读日历文件，只做轻校验（可解析、dates 非空 ISO 升序列），不过抛 LookupError。

    完整 validate_calendar 只在刷新流里跑。
    """
    p = path or calendar_path()
    try:
        with open(p, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError) as e:
        raise LookupError(f"trade calendar unreadable: {p}: {e}") from e
    dates = data.get("dates") if isinstance(data, dict) else None
    if not isinstance(dates, list) or not dates:
        raise LookupError(f"trade calendar empty dates: {p}")
    prev = ""
    for d in dates:
        if not isinstance(d, str):
            raise LookupError(f"trade calendar bad entry: {d!r} in {p}")
        try:
            date.fromisoformat(d)
        except ValueError:
            raise LookupError(f"trade calendar bad date: {d!r} in {p}") from None
        if d <= prev:
            raise LookupError(f"trade calendar not increasing at {d!r} in {p}")
        prev = d
    return {
        "dates": dates,
        "fetched_at": data.get("fetched_at", ""),
        "source": data.get("source", "sina"),
        "meta": data.get("meta") or {},
    }


def load_authoritative_dates() -> tuple[list[str], str]:
    """按 文件 → seed 顺序取权威日期，返回 (dates, origin)，都没有抛 LookupError。"""
    try:
        return load_calendar_file(calendar_path())["dates"], "file"
    except LookupError:
        pass
    try:
        return load_calendar_file(_seed_path())["dates"], "seed"
    except LookupError as e:
        raise LookupError(f"no authoritative trade calendar: {e}") from e


def save_calendar_file(path: str | None, dates: list[str], source: str = "sina") -> None:
    """写日历文件；已存在则保留 meta.last_stale_push。"""
    p = path or calendar_path()
    last_push = None
    with contextlib.suppress(OSError, ValueError):
        with open(p, encoding="utf-8") as f:
            old = json.load(f)
        if isinstance(old, dict) and isinstance(old.get("meta"), dict):
            last_push = old["meta"].get("last_stale_push")
    payload = {
        "dates": list(dates),
        "fetched_at": datetime.now().isoformat(timespec="seconds"),
        "source": source,
        "meta": {"last_stale_push": last_push},
    }
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    os.replace(tmp, p)


def _fresh_result(path: str, today: date, max_age_days: int) -> dict | None:
    try:
        data = load_calendar_file(path)
        fetched = datetime.fromisoformat(str(data.get("fetched_at") or ""))
    except (LookupError, ValueError, TypeError):
        return None
    if (today - fetched.date()).days < max_age_days:
        return {"ok": True, "reason": "fresh", "path": path, "count": len(data["dates"])}
    return None


def refresh_calendar(force: bool = False, max_age_days: int = 7) -> dict:
    """刷新日历文件，永不抛异常；返回 {"ok","reason","path","count"}。"""
    path = ""
    try:
        path = calendar_path()
        today = date.today()
        if not force:
            hit = _fresh_result(path, today, max_age_days)
            if hit is not None:
                return hit
        payload = fetch_sina_payload()
        dates = decode_sina_calendar(payload)
        validate_calendar(dates, today=today)
        save_calendar_file(path, dates)
        return {"ok": True, "reason": "refreshed", "path": path, "count": len(dates)}
    except Exception as e:
        logger.warning("交易日历刷新失败: %s", e)
        count = 0
        with contextlib.suppress(Exception):
            count = len(load_calendar_file(path)["dates"])
        return {"ok": False, "reason": str(e), "path": path, "count": count}


def probe_recent_anchor(symbol: str = "sh000001", count: int = 40) -> str | None:
    """腾讯日线探测最近交易日，失败返回 None，永不抛异常。"""
    try:
        r = requests.get(TENCENT_DAY_URL + f"{symbol},day,,,{count},qfq",
                         headers=_TX_HEADERS, timeout=_HTTP_TIMEOUT)
        r.raise_for_status()
        body = r.json()
        blk = body.get("data") if isinstance(body, dict) else None
        if not isinstance(blk, dict) or not blk:
            return None
        first = next(iter(blk.values()))
        rows = first.get("day") if isinstance(first, dict) else None
        if not rows:
            return None
        last = rows[-1]
        day = last[0] if isinstance(last, (list, tuple)) else None
        return day if isinstance(day, str) and day else None
    except Exception as e:
        logger.warning("腾讯交易日历探测失败: %s", e)
        return None


def last_trading_day(dates: list[str], day: str | date) -> str | None:
    """dates 中 <= day 的最大日期，无则 None（dates 升序）。"""
    target = day.isoformat() if isinstance(day, date) else str(day)
    prev = None
    for d in dates:
        if d <= target:
            prev = d
        else:
            break
    return prev


def check_drift(engine_end: str | None, file_end: str | None,
                probe_end: str | None) -> dict:
    """三方对账：引擎末端 vs 文件末端 vs 腾讯探测。"""
    if engine_end is None or file_end is None or probe_end is None:
        return {"ok": False, "level": "error",
                "reason": f"日历对账数据缺失: engine={engine_end} file={file_end} probe={probe_end}"}
    if engine_end != file_end:
        return {"ok": False, "level": "error",
                "reason": f"引擎日历末端{engine_end}落后权威{file_end}"}
    if probe_end and probe_end != file_end:
        return {"ok": True, "level": "warn",
                "reason": f"腾讯探测末端{probe_end}与权威文件{file_end}不一致"}
    return {"ok": True, "level": "ok", "reason": "三方一致"}


def push_stale_alert(text: str) -> bool:
    """过期钉钉告警（同自然周只推一次）；无配置/本周已推返回 False。"""
    try:
        from app.quant import db
        webhook = db.get_quant_setting("dingtalk_webhook_url") or ""
        if not webhook:
            return False
        secret = db.get_quant_setting("dingtalk_secret") or ""
        path = calendar_path()
        data: dict | None = None
        with contextlib.suppress(OSError, ValueError):
            with open(path, encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                data = raw
        today = date.today()
        if data is not None:
            meta = data.get("meta")
            last = meta.get("last_stale_push") if isinstance(meta, dict) else None
            if last:
                with contextlib.suppress(ValueError):
                    if date.fromisoformat(str(last)[:10]).isocalendar()[:2] == today.isocalendar()[:2]:
                        return False
        from app.quant import notify
        if not notify.send_dingtalk(webhook, secret, "交易日历过期提醒", text):
            return False
        if data is not None:
            meta = dict(data["meta"]) if isinstance(data.get("meta"), dict) else {}
            meta["last_stale_push"] = today.isoformat()
            data["meta"] = meta
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
            os.replace(tmp, path)
        return True
    except Exception as e:
        logger.warning("交易日历钉钉告警失败: %s", e)
        return False
