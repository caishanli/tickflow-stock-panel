"""mootdx (通达信) 数据源实现。"""

import socket

import pandas as pd
from mootdx.quotes import Quotes
from mootdx.utils import get_stock_market

from .base import DataSource, DataSourceError


def _to_symbol(code):
    """平台代码 -> mootdx 6位纯数字代码。"""
    return code.split(".")[0]


def _is_index(code):
    """判断是否为指数代码。

    - 399xxx.SZ（深证系列指数，如 399001 深证成指）
    - 000001~000999.SH（上证系列指数，如 000001 上证指数、000300 沪深300）
    注意：SZ 市场的 000xxx（如 000001 平安银行、000157）是**股票**不是指数，
    必须用 ``bars`` 而非 ``index_bars`` 取数 —— 否则对退市/旧代码的 SZ 000xxx
    误走指数接口返回空，触发全服务器轮换（每只 ~8.5s，批量回源累计数十分钟）。
    同时沪市 000001~000999 是真实指数（000001 上证指数），不能被排除。
    """
    pure = code.split(".")[0]
    suffix = code.split(".")[1] if "." in code else ""
    if pure.startswith("399"):
        return True
    if suffix in ("SH", "XSHG") and pure.startswith("000") and len(pure) == 6:
        # 沪市 000xxx 为指数（000001 上证指数 … 000999）；排除个别指数段外的
        # 处理：000xxx 沪市基本都是指数（无沪市 000 股票，沪市股票是 600/601/
        # 603/605/688 等）
        return True
    return False


# 显式 mootdx 行情服务器列表（TCP 7709）。顺序探测，用第一个可达的，
# 规避 0.11.x BESTIP.HQ 空串 bug；海外网络通常全部超时，此时回退
# 到 mootdx 自带 bestip 测速 / 裸 factory。
_TDX_SERVERS = [
    ('115.238.90.165', 7709), ('115.238.56.198', 7709),
    ('218.75.126.9', 7709), ('124.160.88.183', 7709),
    ('60.191.117.167', 7709), ('60.12.136.250', 7709),
    ('119.97.185.59', 7709), ('124.70.133.119', 7709), ('116.205.183.150', 7709),
    ('123.60.73.44', 7709), ('116.205.163.254', 7709), ('121.36.225.169', 7709),
    ('123.60.70.228', 7709), ('124.71.9.153', 7709), ('110.41.147.114', 7709),
    ('124.71.187.122', 7709),
]

# pytdx 连接后 socket 读超时（秒）：只设了 connect time_out=10 的话，服务器
# 静默断开会话时 c.bars() 会在 recv 上永久阻塞 → _with_server_retry 的 10s
# join 只能弃线程，外层 _guarded_get_minute 30s 再把它掐掉、遗弃内层线程继续
# 后台轮换，长跑回源里线程/socket 堆积形成死亡螺旋。设读超时后 recv 会按时
# 抛 socket.timeout，轮换能跑完并命中可用服务器。
_TDX_SOCKET_READ_TIMEOUT = 10.0
# 外层守护超时需覆盖内层整轮服务器轮换的最坏耗时（_TDX_SERVERS 各一次 + 兜底），
# 否则会在轮换中途被掐断、永远到不了可用服务器（08-19 长跑回源每只 30s 超时根因）。
_TDX_FETCH_GUARD_TIMEOUT = len(_TDX_SERVERS) * _TDX_SOCKET_READ_TIMEOUT + 30.0


def _probe(ip, port, timeout=2.0):
    """TCP 握手探测，判断服务器是否可达。"""
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            return True
    except Exception:
        return False


def tdx_client(market='std'):
    """
    创建 mootdx 客户端，规避 0.11.x BESTIP.HQ 空串 bug。
    顺序兜底，保证 IP 列表老化/换网时仍能工作：
      1) 顺序探测 _TDX_SERVERS，用第一个 TCP 可达的显式 server
         （不做数据探活：某服务器对 510300 无数据不代表对其他标的
         也无数据，数据为空由上层 _with_server_retry 换服务器兜底）；
      2) 显式列表全不可达 → 回退 mootdx 自带 bestip 测速选优；
      3) 再不行 → 回退裸 factory（老用户 config 已有可用 BESTIP 时成立）；
      4) 仍失败 → 抛 RuntimeError，明确报错而非死等。
    """
    for ip, port in _TDX_SERVERS:
        if _probe(ip, port):
            try:
                return Quotes.factory(market=market, server=(ip, port))
            except Exception:
                continue
    try:
        return Quotes.factory(market=market)                # 裸 factory（实测可用且不选速）
    except Exception as e:
        raise RuntimeError(
            "所有 mootdx 服务器均不可达。海外网络通常全部超时（TCP 7709），"
            "请走国内代理或更新 _TDX_SERVERS 列表。原始错误：%s" % e
        )


# 证券名称缓存（内存级，跨回测复用；另落盘 data/.stock_names_cache.json）
_STOCK_NAMES_CACHE = None


class MootdxSource(DataSource):
    name = "mootdx"

    def __init__(self, token=""):
        self._client = None
        self._server_idx = -1  # 当前使用的 _TDX_SERVERS 下标（-1=尚未显式选定）
        # xdxr 除权除息记录进程内缓存（code -> rows/None）：除权信息在回测
        # 期间不变，避免同一标的重复网络往返；查询失败缓存 None（保持 raw）
        self._xdxr_cache = {}

    def _api(self):
        if self._client is None:
            self._client = self._make_client()
        return self._client

    def _make_client(self, server=None):
        """创建 mootdx 客户端并用 pytdx 替换底层 tdxpy（tdxpy 0.2.7 协议不兼容）。

        顺序：1) 显式 _TDX_SERVERS 探测 → 2) 裸 factory 兜底。
        返回 StdQuotes 实例（client.client 已替换为 pytdx.TdxHq_API）。
        """
        import pytdx.hq as _pytdx_hq
        from mootdx.quotes import Quotes

        def _patch(quotes_client, ip, port):
            """用 pytdx 替换 mootdx 内部的 tdxpy client，修复 bars/quotes 返回空。"""
            try:
                px = _pytdx_hq.TdxHq_API()
                px.connect(ip, int(port), time_out=10)
                # 读超时：服务器静默断开会话时 recv 不再永久阻塞，而是按时抛
                # socket.timeout，让 _with_server_retry 的轮换能继续换服务器。
                px.client.settimeout(_TDX_SOCKET_READ_TIMEOUT)
                quotes_client.client = px
            except Exception:
                pass
            return quotes_client

        # 1) 显式列表探测
        servers = [server] if server else _TDX_SERVERS
        for ip, port in servers:
            if not _probe(ip, port):
                continue
            try:
                c = Quotes.factory(market="std", server=(ip, port))
                return _patch(c, ip, port)
            except Exception:
                continue
        # 2) 裸 factory 兜底
        try:
            c = Quotes.factory(market="std")
            # 裸 factory 可能选了未知服务器，尝试用已知可用服务器替换
            for ip, port in _TDX_SERVERS:
                if _probe(ip, port):
                    return _patch(c, ip, port)
            return c
        except Exception as e:
            raise DataSourceError(f"mootdx 初始化失败: {e}")

    def _rotate_server(self, to_bestip=False):
        """换服务器重建客户端（运行时取数超时/失败兜底）。

        按 _TDX_SERVERS 顺序轮换，每次都用 pytdx 替换底层 tdxpy。
        列表用尽时**回绕到 -1**（下次取数从首个可达服务器重新探测），
        而不是停在末位 —— 否则批量取数时一次"全服务器超时"会让后续所有
        请求都钉在坏服务器上快速失败（实测 1660 只批量同步 921 只失败）。
        返回新客户端；全部失败返回 None。
        """
        self._server_idx += 1
        if self._server_idx >= len(_TDX_SERVERS):
            self._server_idx = -1  # 回绕：下次从首地址重新探测
            self._client = None    # 同时丢弃坏客户端，_api() 将重建探测
            return None
        ip, port = _TDX_SERVERS[self._server_idx]
        if not _probe(ip, port):
            return self._rotate_server(to_bestip)
        try:
            self._client = self._make_client(server=(ip, port))
            return self._client
        except Exception:
            return self._rotate_server(to_bestip)

    def _with_server_retry(self, fn, empty_ok=False):
        """执行取数 ``fn``，超时/返回空时按 _TDX_SERVERS 轮询换服务器重试。

        - fn 内阻塞超过 15s 视为超时（线程守护，超时即换服务器）；
        - 返回 None/空 且 ``empty_ok=False`` 也触发换服务器；
        - 列表用尽后回退到 bestip/factory 兜底客户端（_api 重建）。
        返回 (df, err)：成功 df 非 None，失败二者皆 None/err 说明。
        """
        import threading
        attempts = len(_TDX_SERVERS) + 1  # 显式列表各一次 + 末次 bestip/factory 兜底
        for attempt in range(attempts):
            c = self._api()
            box = {}
            def _run():
                try:
                    box["df"] = fn(c)
                except Exception as e:
                    box["err"] = e
            t = threading.Thread(target=_run, daemon=True)
            t.start()
            t.join(10)
            if t.is_alive():
                self._rotate_server()
                continue
            if "err" in box:
                self._rotate_server()
                continue
            df = box.get("df")
            if df is None or (not empty_ok and (hasattr(df, "empty") and df.empty)):
                self._rotate_server(to_bestip=(attempt == attempts - 1))
                continue
            return df, None
        return None, "mootdx 所有服务器均超时/无数据"

    def get_daily(self, code, start, end):
        """通达信日线：原始价取数后尝试用 xdxr 除权除息因子换算前复权。

        返回帧 ``attrs`` 标注口径元数据：``source="mootdx"``，
        ``adj="qfq"``（前复权换算成功）或 ``"raw"``（指数无复权概念 /
        xdxr 无记录 / 换算失败，保持通达信原始不复权价）。raw 帧由
        manager 混源防护（_pick_daily_frame）让位给其他源的前复权帧。
        """
        sym = _to_symbol(code)
        def _fn(c):
            if _is_index(code):
                return c.index_bars(symbol=sym, frequency=9)
            return c.bars(symbol=sym, frequency=9)
        df, err = self._with_server_retry(_fn)
        if df is None or df.empty:
            raise DataSourceError(f"mootdx 无日线数据 ({err})")
        # 兼容 JQ 字段名：成交额 = amount -> money；成交量 = vol -> volume
        if "amount" in df.columns:
            df["money"] = df["amount"]
        if "vol" in df.columns and "volume" not in df.columns:
            df["volume"] = df["vol"]
        if "volume" in df.columns:
            df["volume"] = df["volume"] * 100
        df.attrs["source"] = "mootdx"
        df.attrs["adj"] = "raw"
        if not _is_index(code):
            qfq = self._to_qfq(df, sym)
            if qfq is not None:
                df = qfq
        return df

    def _xdxr_rows(self, sym):
        """取通达信除权除息(xdxr)记录（进程内缓存，同一代码只查一次）。

        mootdx 0.11.x 底层为 tdxpy，``TdxHq_API.get_xdxr_info(market, code)``
        返回全部历史除权行（category==1 为除权除息：fenhong=每10股红利(元)、
        songzhuangu=每10股送转、peigu=每10股配股、peigujia=配股价）。
        无记录返回 []、失败返回 None，调用方据此保持 raw 口径。

        失败（整轮服务器轮换耗尽）返回 None 且**不缓存**——下次调用重试；
        只有成功结果（[] 或事件列表）才进缓存。否则一次 socket 抖动会把
        None 钉进缓存，该标的当轮被静默跳过且进程内再无自愈机会
        （159667 拆分事件漏采数周的根因）。
        """
        if sym in self._xdxr_cache:
            return self._xdxr_cache[sym]
        rows = None
        for _ in range(2):  # 整轮轮换失败后再补一轮；仍失败才算真失败
            rows, _err = self._with_server_retry(
                lambda c: c.client.get_xdxr_info(int(get_stock_market(sym)), sym),
                empty_ok=True)
            if rows is not None:
                break
        if rows is None:
            return None  # 不缓存失败
        self._xdxr_cache[sym] = rows
        return rows

    def _to_qfq(self, df, sym):
        """用 xdxr 因子把 raw 日线换算为前复权（最新口径），失败返回 None。

        标准除权参考价公式：除权价 = (昨收 - 每股红利 + 配股价×每股配股)
        / (1 + 每股送转 + 每股配股)；前复权因子 = 除权价 / 昨收，对除权日
        之前的 OHLC 逐次累乘。只调整价格列，volume/money 保持原始（与聚宽
        get_price fq="pre" 口径一致：价格复权、量额不复权）。

        局限：除权日早于帧内首个交易日时，前一收盘价不在帧内、该次因子
        无法计算（跳过）——帧内全部行统一差该因子，不影响收益率序列的
        相对关系，但绝对价与最新口径存在固定偏差。
        """
        if not isinstance(df.index, pd.DatetimeIndex) or "close" not in df.columns:
            return None
        rows = self._xdxr_rows(sym)
        if not rows:
            return None
        close = pd.to_numeric(df["close"], errors="coerce")
        factors = []
        for r in rows:
            if r.get("category") != 1:
                continue  # 只处理除权除息（分红/送转/配股），股本变动类不影响价
            fh = float(r.get("fenhong") or 0) / 10.0      # 每股现金红利(元)
            sg = float(r.get("songzhuangu") or 0) / 10.0  # 每股送转
            pg = float(r.get("peigu") or 0) / 10.0        # 每股配股
            pgj = float(r.get("peigujia") or 0)           # 配股价
            if fh == 0 and sg == 0 and pg == 0:
                continue
            try:
                ex_dt = pd.Timestamp(int(r["year"]), int(r["month"]), int(r["day"]))
            except Exception:
                continue
            prev = close.loc[close.index < ex_dt].dropna()
            if prev.empty:
                continue  # 前一收盘价不在帧内，因子无法计算（见 docstring 局限）
            prev_close = float(prev.iloc[-1])
            if prev_close <= 0:
                continue
            ex_price = (prev_close - fh + pgj * pg) / (1.0 + sg + pg)
            if ex_price <= 0:
                continue
            factors.append((ex_dt, ex_price / prev_close))
        if not factors:
            return None
        df = df.copy()
        adj = pd.Series(1.0, index=df.index)
        for ex_dt, f in factors:
            adj.loc[adj.index < ex_dt] *= f
        for col in ("open", "high", "low", "close"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce") * adj
        df.attrs["adj"] = "qfq"
        return df

    def get_minute(self, code, date="", max_bars=30000):
        """历史 1 分钟 K 线：mootdx 单次最多约 800 根，按 ``start`` 分页回看。

        ``max_bars`` 上限防止对"无数据/长期停牌"标的空转 400 页（每页一次
        阻塞式 socket 调用，单只可卡数分钟）；达到上限即停止分页。
        取数超时/返回空时自动按 _TDX_SERVERS 轮询换服务器重试。
        """
        sym = _to_symbol(code)
        box = {}
        def _fn(c):
            # 先试拉 1 页判断是否有数据：无数据立即失败，避免对停牌标的
            # 空转 400 页。
            first = c.bars(symbol=sym, frequency=8, start=0, offset=800)
            if first is None or first.empty:
                raise DataSourceError("mootdx 无分钟数据")
            box["c"] = c
            return first
        first, err = self._with_server_retry(_fn)
        if first is None:
            raise DataSourceError(f"mootdx 无分钟数据 ({err})")
        c = box.get("c")
        frames = [first]
        fetched = len(first)
        start = 800
        offset = 800
        for _ in range(399):
            if fetched >= max_bars:
                break
            try:
                df = c.bars(symbol=sym, frequency=8, start=start, offset=offset)
            except Exception:
                break
            if df is None or df.empty:
                break
            frames.append(df)
            fetched += len(df)
            if len(df) < offset:
                break
            start += offset
        if not frames:
            raise DataSourceError("mootdx 无分钟数据")
        out = pd.concat(frames)
        out = out[~out.index.duplicated(keep="last")].sort_index()
        if "vol" in out.columns and "volume" not in out.columns:
            out["volume"] = out["vol"]
        if "amount" in out.columns and "money" not in out.columns:
            out["money"] = out["amount"]
        out.index.name = "datetime"
        if "datetime" in out.columns:
            out = out.drop(columns=["datetime"])
        return out

    def get_minute_recent(self, code, pages=1):
        """最近 N 页 1 分钟 K 线（每页约 800 根，含当日盘中实时 bar）。

        与 :meth:`get_minute` 的全量分页回看不同，本方法只取最近 ``pages``
        页（start=0 为最新一页），供模拟盘盘中高频刷新：每只标的每次仅
        ``pages`` 次 socket 调用。
        """
        sym = _to_symbol(code)

        def _fn(c):
            frames = []
            for i in range(max(1, int(pages))):
                df = c.bars(symbol=sym, frequency=8, start=i * 800, offset=800)
                if df is None or df.empty:
                    break
                frames.append(df)
                if len(df) < 800:
                    break
            if not frames:
                raise DataSourceError("mootdx 无分钟数据")
            return pd.concat(frames)

        out, err = self._with_server_retry(_fn)
        if out is None:
            raise DataSourceError(f"mootdx 实时分钟获取失败 ({err})")
        out = out[~out.index.duplicated(keep="last")].sort_index()
        if "vol" in out.columns and "volume" not in out.columns:
            out["volume"] = out["vol"]
        if "amount" in out.columns and "money" not in out.columns:
            out["money"] = out["amount"]
        out.index.name = "datetime"
        if "datetime" in out.columns:
            out = out.drop(columns=["datetime"])
        return out

    def get_index_realtime(self, codes):
        raise DataSourceError("mootdx 暂不支持指数实时")

    def get_etf_list(self):
        """用 TickFlow exchanges.get_instruments 拉取全市场 ETF 名录（免费，无需 token）。

        返回与 tushare fund_basic 兼容的 dict 列表:
        [{"ts_code": "510300.SH", "name": "沪深300ETF", "list_date": "20120528", "delist_date": ""}, ...]
        """
        try:
            from app.services.index_sync import _fetch_instruments_by_type

            df = _fetch_instruments_by_type("etf", "etf")
            if df is None or df.is_empty():
                raise DataSourceError("TickFlow get_instruments 返回空 ETF 列表")
            records = []
            for row in df.to_dicts():
                symbol = str(row.get("symbol", ""))
                name = str(row.get("name", ""))
                records.append({
                    "ts_code": symbol,
                    "name": name,
                    "list_date": "",
                    "delist_date": "",
                })
            return records
        except DataSourceError:
            raise
        except Exception as e:
            raise DataSourceError(f"TickFlow ETF名录获取失败: {e}")

    def get_stock_names(self):
        """返回 {code: name} 字典，名称来自通达信行情（与聚宽 display_name 一致）。

        证券名称在回测期间不变，且仅供展示。每次回测都通过 TDX 网络拉全量
        名录代价极高（单次回测 ~15s），故在内存 + 本地文件做缓存：进程内只取
        一次，跨进程/跨回测直接命中本地缓存，避免重复网络往返。不影响行情正确性。
        """
        import json
        import os

        global _STOCK_NAMES_CACHE
        if _STOCK_NAMES_CACHE is not None:
            return _STOCK_NAMES_CACHE
        cache_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
            "data", ".stock_names_cache.json")
        if os.path.exists(cache_file):
            try:
                with open(cache_file, encoding="utf-8") as f:
                    _STOCK_NAMES_CACHE = json.load(f)
                return _STOCK_NAMES_CACHE
            except Exception:
                pass
        c = self._api()
        out = {}
        for market in (0, 1):
            try:
                df = c.stocks(market=market)
                if df is None or df.empty:
                    continue
                for _, row in df.iterrows():
                    code = str(row["code"]).strip()
                    name = str(row["name"]).replace("\x00", "").strip()
                    out[code] = name
            except Exception:
                continue
        _STOCK_NAMES_CACHE = out
        try:
            os.makedirs(os.path.dirname(cache_file), exist_ok=True)
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(out, f, ensure_ascii=False)
        except Exception:
            pass
        return out

    def get_stock_list(self):
        raise DataSourceError("mootdx 未配置股票池，请优先使用Tushare")

    def get_us_index(self):
        raise DataSourceError("mootdx 不支持美股")

    def test_connection(self):
        try:
            self._api()
            return True, "mootdx 连接正常"
        except Exception as e:
            return False, str(e)


def probe_servers(timeout: float = 1.5) -> list[dict]:
    """并发探测全部显式 mootdx 服务器 TCP 连通与延迟。

    返回按 _TDX_SERVERS 顺序的列表，每项 {ip, port, ok, latency_ms}；
    latency_ms 为连接建立耗时（毫秒，整数），不可达为 None。
    """
    import time
    from concurrent.futures import ThreadPoolExecutor

    def _one(item):
        ip, port = item
        t0 = time.perf_counter()
        ok = False
        try:
            ok = _probe(ip, port, timeout)
        except Exception:
            ok = False
        return {"ip": ip, "port": port, "ok": ok,
                "latency_ms": round((time.perf_counter() - t0) * 1000) if ok else None}

    with ThreadPoolExecutor(max_workers=len(_TDX_SERVERS)) as ex:
        return list(ex.map(_one, _TDX_SERVERS))
