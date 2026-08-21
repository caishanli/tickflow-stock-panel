"""mootdx 连接读超时 + 外层守护超时回归测试。

背景（08-19 长跑回源每只 30s 超时根因）：pytdx 只设 connect time_out=10，
未设 socket 读超时，服务器静默断开会话时 ``c.bars()`` 在 recv 上永久阻塞；
内层 ``_with_server_retry`` 的 10s join 只能弃线程，外层 ``_guarded_get_minute``
30s 又把它掐掉并遗弃内层线程继续后台轮换，线程/socket 堆积形成死亡螺旋——
同一批活跃股新进程 5-8s 成功、长跑进程每只 30s 超时。

修复：连接后设 socket 读超时（recv 按时抛 socket.timeout），外层守护超时
覆盖整轮服务器轮换，保证轮换能跑完并命中可用服务器。
"""
from app.quant.jqengine.datasource import mootdx_src as msrc


class _FakeSocket:
    def __init__(self):
        self.timeout = None

    def settimeout(self, value):
        self.timeout = value


class _FakeTdxApi:
    """伪 pytdx TdxHq_API：connect 后暴露可记录 settimeout 的 socket。"""

    def __init__(self):
        self.client = None

    def connect(self, ip, port, time_out=10):
        self.client = _FakeSocket()
        return self


class _FakeQuotes:
    @staticmethod
    def factory(market="std", server=None):
        return object()


def test_patch_sets_socket_read_timeout(monkeypatch):
    """_make_client/_patch 后 pytdx socket 必须设读超时（否则 recv 永久阻塞）。"""
    monkeypatch.setattr("pytdx.hq.TdxHq_API", _FakeTdxApi)
    monkeypatch.setattr(msrc, "_probe", lambda ip, port, timeout=2.0: True)
    monkeypatch.setattr(msrc, "Quotes", _FakeQuotes)

    src = msrc.MootdxSource()
    client = src._make_client(server=("1.2.3.4", 7709))
    px = client.client
    assert isinstance(px, _FakeTdxApi)
    assert px.client.timeout == msrc._TDX_SOCKET_READ_TIMEOUT, (
        f"pytdx socket 应设读超时 {msrc._TDX_SOCKET_READ_TIMEOUT}s，"
        f"实际 {px.client.timeout}（否则 recv 永久阻塞，回源死亡螺旋）")


def test_guard_timeout_covers_full_server_rotation():
    """外层守护超时必须覆盖整轮服务器轮换的最坏耗时。

    若守护 < 轮换耗时，会在轮换中途被掐断、永远到不了可用服务器
    （08-19 每只 30s 超时的直接原因：30s 守护 < 16 台 × 10s 轮换）。
    """
    assert msrc._TDX_SOCKET_READ_TIMEOUT > 0
    worst = len(msrc._TDX_SERVERS) * msrc._TDX_SOCKET_READ_TIMEOUT
    assert msrc._TDX_FETCH_GUARD_TIMEOUT > worst, (
        f"守护超时 {msrc._TDX_FETCH_GUARD_TIMEOUT}s 应 > 轮换最坏 {worst}s")