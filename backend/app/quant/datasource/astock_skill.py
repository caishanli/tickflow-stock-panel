"""从 a-stock-data Skill (https://github.com/simonlin1212/a-stock-data) 抽取的
行情相关函数 vendor 副本。

原始仓库以 SKILL.md (Markdown + 内嵌 Python) 形式发布，许可 Apache-2.0。
这里仅保留本平台数据源需要的几个函数：通达信客户端兜底 ``tdx_client``、
腾讯实时行情 ``tencent_quote``、百度K线 ``baidu_kline_with_ma``，以及它们
依赖的辅助函数 ``_TDX_SERVERS`` / ``_probe`` / ``get_prefix``。
代码逻辑与原仓库一致，仅做格式整理。
"""

import socket
import urllib.request

import requests

try:
    from mootdx.quotes import Quotes
except Exception:  # pragma: no cover - mootdx 不可用时由调用方降级
    Quotes = None


_TDX_SERVERS = [
    ('119.97.185.59', 7709), ('124.70.133.119', 7709), ('116.205.183.150', 7709),
    ('123.60.73.44', 7709),  ('116.205.163.254', 7709), ('121.36.225.169', 7709),
    ('123.60.70.228', 7709), ('124.71.9.153', 7709),    ('110.41.147.114', 7709),
    ('124.71.187.122', 7709),
]


def _probe(ip, port, timeout=2.0):
    """TCP 握手探测，判断服务器是否可达。"""
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            return True
    except Exception:
        return False


def tdx_client(market='std'):
    """创建 mootdx 客户端，规避 0.11.x BESTIP.HQ 空串 bug。

    顺序兜底：
      1) 顺序探测 _TDX_SERVERS，用第一个 TCP 可达的显式 server；
      2) 全部不可达 -> 回退 mootdx bestip 测速选优；
      3) 再不行 -> 回退裸 factory；
      4) 仍失败 -> 抛 RuntimeError。
    """
    if Quotes is None:
        raise RuntimeError("mootdx 未安装或不可用")
    for ip, port in _TDX_SERVERS:
        if _probe(ip, port):
            return Quotes.factory(market=market, server=(ip, port))
    try:
        return Quotes.factory(market=market, bestip=True)
    except Exception:
        pass
    try:
        return Quotes.factory(market=market)
    except Exception as e:
        raise RuntimeError(
            "所有 mootdx 服务器均不可达。海外网络通常全部超时（TCP 7709），"
            "请走国内代理或更新 _TDX_SERVERS 列表。原始错误：%s" % e
        )


def get_prefix(code: str) -> str:
    """6位代码 -> 市场前缀 sh/sz/bj。"""
    if code.startswith(("6", "9")):
        return "sh"
    elif code.startswith("8"):
        return "bj"
    else:
        return "sz"


def tencent_quote(codes):
    """批量拉取腾讯财经实时行情。

    codes: ["688017", "300476"]，也支持指数 ["000001","000300","399006"]
    与 ETF ["510050","510300"]。
    返回: {code: {name, price, change_pct, ...}}
    """
    prefixed = []
    for c in codes:
        if c.startswith(("6", "9")):
            prefixed.append(f"sh{c}")
        elif c.startswith("8"):
            prefixed.append(f"bj{c}")
        else:
            prefixed.append(f"sz{c}")

    url = "https://qt.gtimg.cn/q=" + ",".join(prefixed)
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "Mozilla/5.0")
    resp = urllib.request.urlopen(req, timeout=10)
    data = resp.read().decode("gbk")

    result = {}
    for line in data.strip().split(";"):
        if not line.strip() or "=" not in line or '"' not in line:
            continue
        key = line.split("=")[0].split("_")[-1]
        vals = line.split('"')[1].split("~")
        if len(vals) < 53:
            continue
        code = key[2:]
        result[code] = {
            "name":          vals[1],
            "price":         float(vals[3]) if vals[3] else 0,
            "last_close":    float(vals[4]) if vals[4] else 0,
            "open":          float(vals[5]) if vals[5] else 0,
            "change_amt":    float(vals[31]) if vals[31] else 0,
            "change_pct":    float(vals[32]) if vals[32] else 0,
            "high":          float(vals[33]) if vals[33] else 0,
            "low":           float(vals[34]) if vals[34] else 0,
            "amount_wan":    float(vals[37]) if vals[37] else 0,
            "turnover_pct":  float(vals[38]) if vals[38] else 0,
            "pe_ttm":        float(vals[39]) if vals[39] else 0,
            "amplitude_pct": float(vals[43]) if vals[43] else 0,
            "mcap_yi":       float(vals[44]) if vals[44] else 0,
            "float_mcap_yi": float(vals[45]) if vals[45] else 0,
            "pb":            float(vals[46]) if vals[46] else 0,
            "limit_up":      float(vals[47]) if vals[47] else 0,
            "limit_down":    float(vals[48]) if vals[48] else 0,
            "vol_ratio":     float(vals[49]) if vals[49] else 0,
            "pe_static":     float(vals[52]) if vals[52] else 0,
        }
    return result


def baidu_kline_with_ma(code: str, start_time: str = "") -> dict:
    """百度股市通K线 - 返回时自带 ma5/ma10/ma20 均价。

    返回: {"keys": [...], "rows": [...]}，rows 为分号分隔的字符串列表，
    每行按 keys 顺序逗号分隔。
    """
    url = "https://finance.pae.baidu.com/selfselect/getstockquotation"
    params = {
        "all": "1", "isIndex": "false", "isBk": "false", "isBlock": "false",
        "isFutures": "false", "isStock": "true", "newFormat": "1",
        "group": "quotation_kline_ab", "finClientType": "pc",
        "code": code, "start_time": start_time, "ktype": "1",
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/vnd.finance-web.v1+json",
        "Origin": "https://gushitong.baidu.com",
        "Referer": "https://gushitong.baidu.com/",
    }
    r = requests.get(url, params=params, headers=headers, timeout=10)
    d = r.json()
    result = d.get("Result", {})
    md = result.get("newMarketData", {})
    keys = md.get("keys", [])
    rows = md.get("marketData", "").split(";")
    return {"keys": keys, "rows": rows}
