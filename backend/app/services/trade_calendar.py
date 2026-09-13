"""权威交易日历：新浪 klc_td_sh.txt 解码器 + 校验（纯标准库）。

akshare `hk_js_decode` 中 R 路径（selector 139）的逐行直译，不引入
akshare/pandas/JS 运行时依赖。抓取与刷新由后续任务接线，本模块只做解码 / 校验。
"""

from datetime import date, timedelta
from itertools import pairwise

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
