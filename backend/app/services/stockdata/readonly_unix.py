"""Additive StockData Unix listener: local reads only, no shared TCP changes.

Install as app.services.stockdata.readonly_unix. Uses the current DataSources
instance; never invokes a scheduler, downloader, current quote or adjustment path.
"""
from __future__ import annotations

import datetime as dt
import os
import pwd
import re
import socket
import socketserver
import stat
import struct
import tempfile
import threading
from pathlib import Path

import msgpack

from . import protocol

SOCKET_PATH = Path('/home/y/trading_core/data/stockdata-readonly.sock')
CONTRACT = 'STOCKDATA_READONLY_UNIX_V1'
ALLOWED = frozenset({'ping', 'status', 'get_security_info', 'get_bars', 'get_trade_days'})
MAX_REQUEST = 8192
MAX_RESPONSE = 16_000_000
MAX_RANGE_DAYS = 92


class ReadOnlyMethodDenied(PermissionError):
    pass


def _symbol(value):
    if not isinstance(value, str) or not re.fullmatch(r'[0-9]{6}\.(SH|SZ|XSHG|XSHE)', value):
        raise ValueError('Explicit SH/SZ/XSHG/XSHE symbol required')
    return value.replace('.SH', '.XSHG').replace('.SZ', '.XSHE')


def _range(params):
    try:
        lo = dt.date.fromisoformat(params['start_date'])
        hi = dt.date.fromisoformat(params['end_date'])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError('Explicit ISO date range required') from exc
    if params['start_date'] != lo.isoformat() or params['end_date'] != hi.isoformat():
        raise ValueError('Dates must use YYYY-MM-DD')
    if not 0 <= (hi - lo).days < MAX_RANGE_DAYS:
        raise ValueError('Date range exceeds 92 calendar days')
    return lo.isoformat(), hi.isoformat()


def dispatch_readonly(method, params, src):
    if not isinstance(method, str) or method not in ALLOWED:
        raise ReadOnlyMethodDenied('Method not in StockData read-only allowlist')
    if not isinstance(params, dict):
        raise ValueError('Parameters must be an object')
    keys = {'ping': set(), 'status': set(), 'get_security_info': {'code'},
            'get_bars': {'security', 'start_date', 'end_date', 'frequency', 'fq'},
            'get_trade_days': {'start_date', 'end_date', 'include_evidence'}}[method]
    if set(params) - keys:
        raise ValueError('Unexpected read-only parameters')
    if method in {'ping', 'status'}:
        from .handlers import handle
        return handle(method, {}, src)
    if method == 'get_security_info':
        code = _symbol(params.get('code'))
        # Existing local instruments lookup, without the legacy all-partition fallback.
        tf = code.replace('.XSHG', '.SH').replace('.XSHE', '.SZ')
        info = src.get_security_infos([code]).get(tf)
        if info is None:
            return 'json', {}
        return 'json', {'code': code, 'name': info.get('name'), 'type': info.get('type'),
                        'start_date': info.get('start_date'), 'end_date': None}
    lo, hi = _range(params)
    if method == 'get_trade_days':
        evidence = params.get('include_evidence', True)
        if type(evidence) is not bool:
            raise ValueError('include_evidence must be boolean')
        return 'json', src.get_trade_days(lo, hi, with_evidence=evidence)
    if params.get('frequency') != 'daily' or params.get('fq') not in {'raw', 'none'}:
        raise ValueError('Read-only Unix V1 requires explicit daily/raw or daily/none')
    codes = params.get('security')
    if isinstance(codes, str):
        codes = [codes]
    if not isinstance(codes, list) or not 1 <= len(codes) <= 2:
        raise ValueError('One or two explicit securities required')
    codes = [_symbol(code) for code in codes]
    if len(set(codes)) != len(codes):
        raise ValueError('Duplicate securities forbidden')
    # get_daily reads existing partitions/cache only; never apply_qfq_daily/minute pull.
    return 'parquet', src.get_daily(codes, lo, hi)


def _receive(conn, n):
    data = bytearray()
    while len(data) < n:
        part = conn.recv(n - len(data))
        if not part:
            raise EOFError('Truncated Unix request')
        data.extend(part)
    return bytes(data)


def serve_request(conn, src):
    """One bounded request per connection. Deny before touching any source method."""
    conn.settimeout(8)
    req_id = 0
    try:
        size = int.from_bytes(_receive(conn, 4), 'big')
        if not 0 < size <= MAX_REQUEST:
            raise ValueError('Request frame exceeds bound')
        msg = msgpack.unpackb(_receive(conn, size), raw=False)
        if (not isinstance(msg, dict) or set(msg) != {'v', 'id', 'm', 'p'}
                or msg['v'] != 1 or type(msg['id']) is not int
                or not 0 <= msg['id'] <= 2**31 - 1):
            raise ValueError('Invalid read-only request envelope')
        req_id = msg['id']
        t, data = dispatch_readonly(msg['m'], msg['p'], src)
        response = protocol.encode_response(req_id, True, t, data)
        if len(response) - 4 > MAX_RESPONSE:
            raise ValueError('Response frame exceeds bound')
    except (EOFError, ConnectionError, socket.timeout):
        return
    except Exception as exc:
        # Do not echo paths, source exceptions, credentials or arbitrary parameters.
        response = protocol.encode_response(req_id, False, 'json',
            {'code': type(exc).__name__, 'msg': 'Read-only request denied or unavailable'})
    body = msgpack.unpackb(response[4:], raw=False)
    body['readonly_contract'] = CONTRACT
    payload = msgpack.packb(body, use_bin_type=True)
    if len(payload) > MAX_RESPONSE:
        payload = msgpack.packb({'v': 1, 'id': req_id, 'ok': False, 't': 'json',
            'd': {'code': 'ValueError', 'msg': 'Response frame exceeds bound'},
            'readonly_contract': CONTRACT}, use_bin_type=True)
    conn.sendall(len(payload).to_bytes(4, 'big') + payload)


class _ReadOnlyHandler(socketserver.BaseRequestHandler):
    def handle(self):
        serve_request(self.request, self.server.data_sources)


class StockDataReadOnlyUnixServer(socketserver.ThreadingUnixStreamServer):
    daemon_threads = True
    block_on_close = False

    def __init__(self, data_sources):
        uid = pwd.getpwnam('y').pw_uid
        if os.geteuid() != uid:
            raise PermissionError('Read-only listener must run as y')
        path = SOCKET_PATH
        for parent in reversed(path.parents):
            if parent.is_symlink():
                raise PermissionError('Symlink socket parent forbidden')
        parent_stat = path.parent.stat()
        if parent_stat.st_uid != uid or parent_stat.st_mode & stat.S_IWOTH:
            raise PermissionError('Socket parent must be controlled by y')
        if path.exists() or path.is_symlink():
            raise FileExistsError('Existing Unix endpoint preserved; operator inspection required')
        self.data_sources, self.peer_uid = data_sources, uid
        self._slots = threading.BoundedSemaphore(4)
        self._published_identity = None
        # Bind privately, chmod before publication, then link exclusively: no umask
        # changes, permissive window, competing endpoint overwrite or stale unlink.
        with tempfile.TemporaryDirectory(prefix='.sd-ro-', dir=path.parent) as staging:
            private = str(Path(staging) / 's')
            super().__init__(private, _ReadOnlyHandler, bind_and_activate=False)
            try:
                self.server_bind()
                os.chmod(private, 0o660)
                self.server_activate()
                os.link(private, path)
                entry = path.lstat()
                self._published_identity = (entry.st_dev, entry.st_ino)
            except BaseException:
                self.server_close()
                raise

    def verify_request(self, request, client_address):
        try:
            _, uid, _ = struct.unpack('3i', request.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
            return uid == self.peer_uid
        except OSError:
            return False

    def process_request(self, request, client_address):
        if not self._slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._slots.release()

    def server_close(self):
        super().server_close()
        if self._published_identity is not None:
            try:
                entry = SOCKET_PATH.lstat()
                if stat.S_ISSOCK(entry.st_mode) and (entry.st_dev, entry.st_ino) == self._published_identity:
                    SOCKET_PATH.unlink()
            except FileNotFoundError:
                pass
            self._published_identity = None
