"""数据校验用的外部源探针。

每个探针只负责「从某个源取一份可比对的标准表」，不做判定、不做修复；
比对与仲裁见 :mod:`app.quant.data_verify.checks_external` / ``policy``。
"""

from __future__ import annotations
