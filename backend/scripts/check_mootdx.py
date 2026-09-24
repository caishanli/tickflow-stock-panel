"""mootdx 连通性诊断脚本：分层验证 TDX 到底能不能拉到数据。

分层（对应 systematic-debugging 的逐层取证）：
  L1 TCP 建连      probe_servers()          —— 控制面
  L2 原始数据请求   pytdx 直连逐台 bars/quotes —— 数据面（绕开 app 包装）
  L3 应用层路径     MootdxSource.get_daily/get_minute
  L4 备用源对照     腾讯 HTTP 快照（只看备用源本身是否正常，不计入结论）

运行（从 backend/ 目录）：
  uv run --extra dev python scripts/check_mootdx.py [--code 600519]

退出码：0 = mootdx 能拉到数据（L2 任一台有数，或 L3 任一接口有数）；
  1 = 拉不到；3 = 脚本自身异常（2 留给 argparse 参数错误）。
"""

from __future__ import annotations

import argparse
import contextlib
import sys
import urllib.request


def check_l1_tcp() -> tuple[int, int, list[dict]]:
    from app.quant.jqengine.datasource.mootdx_src import probe_servers

    probes = probe_servers(timeout=3.0)
    ok = [p for p in probes if p["ok"]]
    return len(probes), len(ok), probes


def check_l2_raw(servers: list[tuple[str, int]], code: str = "600519") -> list[dict]:
    """逐台原始请求。status: ok（有 K 线）/ empty（连上但无数据）/ error（异常）。

    bars/quotes 为 int 行数或 None（无数据）；err 置异常/连接失败说明。
    """
    from pytdx.hq import TdxHq_API

    rows = []
    for ip, port in servers:
        api = TdxHq_API()
        r: dict = {"ip": ip, "bars": None, "quotes": None,
                   "status": "error", "err": ""}
        try:
            if not api.connect(ip, port, time_out=5):
                r["err"] = "connect FAIL"
            else:
                try:
                    df = api.get_security_bars(9, 1, code, 0, 5)
                    r["bars"] = None if df is None else len(df)
                except Exception as e:
                    r["err"] = f"bars EXC {type(e).__name__}: {e}"[:120]
                try:
                    q = api.get_security_quotes([(1, code)])
                    r["quotes"] = None if q is None else len(q)
                except Exception as e:
                    qerr = f"quotes EXC {type(e).__name__}: {e}"[:120]
                    r["err"] = f"{r['err']}; {qerr}" if r["err"] else qerr
                if r["err"]:
                    r["status"] = "error"
                elif (r["bars"] or 0) > 0:
                    r["status"] = "ok"
                else:
                    r["status"] = "empty"
        except Exception as e:
            r["err"] = f"{type(e).__name__}: {e}"[:120]
        finally:
            with contextlib.suppress(Exception):
                api.disconnect()
        rows.append(r)
    return rows


def check_l3_app(code: str) -> dict:
    from app.quant.jqengine.datasource.mootdx_src import MootdxSource

    out: dict = {}
    src = MootdxSource()
    try:
        d = src.get_daily(code, "2026-09-01", "2026-09-22")
        out["daily"] = 0 if d is None else len(d)
    except Exception as e:
        out["daily"] = f"FAIL {type(e).__name__}: {e}"[:150]
    try:
        m = src.get_minute(code, date="2026-09-22")
        out["minute"] = 0 if m is None else len(m)
    except Exception as e:
        out["minute"] = f"FAIL {type(e).__name__}: {e}"[:150]
    return out


def _to_tencent_code(code: str) -> str:
    """600519[.SH] -> sh600519；000001[.SZ] -> sz000001（6/9 开头归沪市）。"""
    pure = code.split(".")[0].strip()
    prefix = "sh" if pure[:1] in ("6", "9") else "sz"
    return f"{prefix}{pure}"


def check_l4_tencent(code: str) -> dict:
    out: dict = {}
    try:
        req = urllib.request.Request(
            f"https://qt.gtimg.cn/q={_to_tencent_code(code)}",
            headers={"User-Agent": "Mozilla/5.0"},
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            body = r.read()
        out["quote"] = f"HTTP {r.status} len={len(body)}"
    except Exception as e:
        out["quote"] = f"FAIL {type(e).__name__}: {e}"[:120]
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="mootdx 连通性分层诊断")
    ap.add_argument("--code", default="600519", help="测试标的（默认 600519）")
    args = ap.parse_args()

    from app.quant.jqengine.datasource.mootdx_src import _TDX_SERVERS

    print("=== L1 TCP 建连 ===")
    total, ok_n, probes = check_l1_tcp()
    print(f"TCP: {ok_n}/{total} 可达")
    for p in probes:
        print(f"  {p['ip']}:{p['port']} ok={p['ok']} latency_ms={p['latency_ms']}")

    print("\n=== L2 原始数据请求（逐台 pytdx） ===")
    l2 = check_l2_raw(list(_TDX_SERVERS), args.code)
    bars_ok = sum(1 for r in l2 if r["status"] == "ok")
    for r in l2:
        print(f"  {r['ip']} status={r['status']} "
              f"bars={r['bars']} quotes={r['quotes']} {r['err']}")
    print(f"L2: {bars_ok}/{len(l2)} 台能返回 K 线")

    print("\n=== L3 应用层（MootdxSource） ===")
    l3 = check_l3_app(args.code)
    print(f"  get_daily: {l3['daily']}")
    print(f"  get_minute: {l3['minute']}")
    l3_ok = any(isinstance(v, int) and v > 0 for v in l3.values())

    print("\n=== L4 腾讯备用源对照 ===")
    l4 = check_l4_tencent(args.code)
    print(f"  tencent quote: {l4['quote']}")

    print("\n=== 结论 ===")
    if bars_ok > 0 or l3_ok:
        print(f"MOOTDX OK：L2 {bars_ok}/{len(l2)} 台可拉数，L3 有数={l3_ok}")
        return 0
    print("MOOTDX DOWN：TCP 建连正常但数据面全空（建连 OK ≠ 能拉数）")
    return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"脚本异常: {type(e).__name__}: {e}")
        sys.exit(3)
