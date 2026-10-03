"""申万行业变迁史回源入库。

数据源: swsresearch.com SwClass2021/StockClassifyUse_stock.xls
       （每只股票历次行业调整一行：股票代码/计入日期/行业代码/更新日期）

证书说明: www.swsresearch.com 2026-05 换发的证书只下发叶子证书（缺
GeoTrustG2TLSCNRSA4096SHA2562022CA1 中间证书），Python 严格校验失败。
本脚本从证书 AIA 扩展给出的官方地址下载中间证书，与 certifi 根库
合并成 CA bundle 后正常 verify=True 下载（不关闭证书校验）。

输出: data/industries_sw/all.parquet
      列: code(6位) industry_code(6位) l1_code l2_code l3_code
          start_date(date) update_date(date)
"""
from __future__ import annotations

import io
import os
import ssl
import subprocess
import sys

import pandas as pd
import polars as pl
import requests

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(REPO, "data")
OUT_DIR = os.path.join(DATA_DIR, "industries_sw")
CERT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "certs")
BUNDLE_PATH = os.path.join(CERT_DIR, "sws_ca_bundle.pem")
SW_URL = "https://www.swsresearch.com/swindex/pdf/SwClass2021/StockClassifyUse_stock.xls"
HOST = "www.swsresearch.com"
AIA_CRT = "http://cacerts.digicert.cn/GeoTrustG2TLSCNRSA4096SHA2562022CA1.crt"


def _build_ca_bundle() -> str:
    """certifi 根库 + 站点缺失的中间证书 → 合并 bundle。失败时抛出可操作错误。"""
    os.makedirs(CERT_DIR, exist_ok=True)
    if os.path.exists(BUNDLE_PATH):
        return BUNDLE_PATH
    import certifi

    # 用 openssl 拉叶子证书并从 AIA 提取中间证书地址，避免 URL 写死失效
    out = subprocess.run(
        ["openssl", "s_client", "-connect", f"{HOST}:443",
         "-servername", HOST],
        input=b"\n", capture_output=True, timeout=30,
    )
    leaf_pem = out.stdout
    if b"BEGIN CERTIFICATE" not in leaf_pem:
        raise RuntimeError(f"openssl 无法连接 {HOST}（见上方输出）")
    leaf = ssl._ssl._test_decode_cert  # noqa: SLF001  仅借 stdlib；实际用 x509 解析
    x509_text = subprocess.run(
        ["openssl", "x509", "-noout", "-text"],
        input=leaf_pem, capture_output=True, timeout=15,
    ).stdout.decode("utf-8", errors="ignore")
    aia = [ln.strip().split("URI:", 1)[-1] for ln in x509_text.splitlines()
           if "CA Issuers" in ln and "URI:" in ln]
    if not aia:
        raise RuntimeError("站点证书无 CA Issuers 扩展，请人工下载中间证书")
    crt_url = aia[0]
    print(f"[cert] 从 AIA 下载中间证书: {crt_url}")
    r = requests.get(crt_url, timeout=30,
                     headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    der = r.content
    # .crt 通常为 DER，转 PEM
    pem = subprocess.run(
        ["openssl", "x509", "-inform", "DER", "-outform", "PEM"],
        input=der, capture_output=True, timeout=15,
    ).stdout
    if b"BEGIN CERTIFICATE" not in pem:
        # 已是 PEM
        pem = der
    with open(BUNDLE_PATH, "w", encoding="utf-8") as f:
        f.write(open(certifi.where(), encoding="utf-8").read())
        f.write("\n")
        f.write(pem.decode("utf-8", errors="ignore"))
    return BUNDLE_PATH


def main() -> None:
    bundle = _build_ca_bundle()
    print(f"[cert] CA bundle: {bundle}")
    r = requests.get(SW_URL, headers={"User-Agent": "Mozilla/5.0"},
                     timeout=120, verify=bundle)
    r.raise_for_status()
    print(f"[dl] {len(r.content)} bytes")

    df = pd.read_excel(io.BytesIO(r.content))
    df = df.rename(columns={"股票代码": "code", "计入日期": "start_date",
                            "行业代码": "industry_code", "更新日期": "update_date"})
    missing = {"code", "start_date", "industry_code"} - set(df.columns)
    if missing:
        raise RuntimeError(f"申万表结构变了，缺列 {sorted(missing)}；实际列={list(df.columns)}")
    df["code"] = df["code"].astype(str).str.zfill(6)
    df["industry_code"] = df["industry_code"].astype(str).str.zfill(6)
    out = pd.DataFrame({
        "code": df["code"],
        "industry_code": df["industry_code"],
        "l1_code": df["industry_code"].str[:2] + "0000",
        "l2_code": df["industry_code"].str[:4] + "00",
        "l3_code": df["industry_code"],
        "start_date": pd.to_datetime(df["start_date"], errors="coerce").dt.date,
        "update_date": pd.to_datetime(df["update_date"], errors="coerce").dt.date,
    }).dropna(subset=["start_date"]).sort_values(["code", "start_date"])
    pl_df = pl.from_pandas(out)
    os.makedirs(OUT_DIR, exist_ok=True)
    out_path = os.path.join(OUT_DIR, "all.parquet")
    pl_df.write_parquet(out_path)
    print(f"[ok] {out_path}: {pl_df.height} 行 | {pl_df['code'].n_unique()} 只 | "
          f"一级行业 {pl_df['l1_code'].n_unique()} / 二级 {pl_df['l2_code'].n_unique()}")
    # 抽查：茅台/平安银行的最近归属
    for c in ("600519", "000001"):
        sub = pl_df.filter(pl.col("code") == c).sort("start_date")
        last = sub.row(-1, named=True)
        print(f"[spot] {c}: {len(sub)} 条, 最近 {last['start_date']} -> L1 {last['l1_code']}")
    cov = pl_df.filter(pl.col("start_date") <= pd.Timestamp("2026-04-01").date())
    print(f"[cov] 2026-04-01 前已有归属记录的标的: {cov['code'].n_unique()} 只")


if __name__ == "__main__":
    sys.exit(main())
