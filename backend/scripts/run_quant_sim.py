"""模拟盘独立进程：由 FastAPI 派生子进程或 pm2/nohup 守护。

用法：
  python scripts/run_quant_sim.py <account_id>          # 运行已有账户（FastAPI/pm2 调用）
  python scripts/run_quant_sim.py --create --name N --capital C [--stop-loss S] \
      --strategy-id SID --start-date YYYY-MM-DD [--autostart] [--account-id AID]
      # 创建账户（可指定 aid 便于验收对齐），--autostart 时经内存门禁拉起
  python scripts/run_quant_sim.py --create --clone-from SRC_AID --strategy-id SID \
      [--name N] [--capital C] [--stop-loss S] [--start-date D] [--account-id AID]
      # 克隆源账户：配置镜像（未传项继承源值）+ sim_state 整行镜像续跑
  python scripts/run_quant_sim.py --reset <account_id>  # 清状态并重新回补
"""
from __future__ import annotations

import sys

from app.quant.simulate.runner import run_loop


def main():
    args = sys.argv[1:]
    if args and args[0] == "--create":
        import argparse
        from app.quant import service

        p = argparse.ArgumentParser(prog="run_quant_sim.py --create")
        p.add_argument("--name", default=None)
        p.add_argument("--capital", type=float, default=None)
        p.add_argument("--stop-loss", dest="stop_loss", type=float, default=None,
                       help="止损线（如 0.05）；缺省 0.05，--clone-from 时继承源账户")
        p.add_argument("--strategy-id", dest="strategy_id", required=True,
                       help="strategies 表已注册的策略 id")
        p.add_argument("--start-date", dest="start_date", default=None,
                       help="回放/补跑起始交易日（历史对齐用）")
        p.add_argument("--account-id", dest="account_id", default=None,
                       help="指定账户 id（验收对齐用固定 id）；缺省自动生成")
        p.add_argument("--clone-from", dest="clone_from", default=None,
                       help="克隆源账户：配置镜像 + sim_state 整行镜像续跑；需显式 --strategy-id")
        p.add_argument("--autostart", action="store_true",
                       help="创建后立即经内存门禁拉起子进程")
        a = p.parse_args(args[1:])
        import uuid
        from app.quant import db
        if a.clone_from:
            src = db.get_sim_account(a.clone_from)
            if not src:
                print(f"clone source account not found: {a.clone_from}", file=sys.stderr)
                sys.exit(1)
            name = a.name or f"{src['name']}-预报告"
            capital = a.capital if a.capital is not None else float(src["capital"])
            stop_loss = float(src["stop_loss"]) if a.stop_loss is None else a.stop_loss
            start_date = a.start_date or src["start_date"]
            frequency = src.get("frequency") or "minute"
            aid = a.account_id or uuid.uuid4().hex[:8]
            db.insert_sim_account(aid, name, capital, stop_loss, "created",
                                  a.strategy_id, start_date, frequency)
            db.update_sim_account(aid, dingtalk_enabled=int(src.get("dingtalk_enabled") or 0))
            src_state = db.read_sim_state(a.clone_from)
            if src_state.get("dt"):
                db.upsert_sim_state(aid, src_state["cash"], src_state["positions_json"],
                                    src_state["net_value"], src_state["pnl"],
                                    src_state["start_cash"], src_state["stop_loss_log_json"],
                                    src_state["dt"])
            print(f"cloned account {a.clone_from} -> {aid}")
        else:
            if a.name is None or a.capital is None or a.start_date is None:
                print("--create 需要 --name/--capital/--start-date（或改用 --clone-from）",
                      file=sys.stderr)
                sys.exit(1)
            stop_loss = 0.05 if a.stop_loss is None else a.stop_loss
            if a.account_id:
                # 固定 id：绕过 account_create 的 uuid 生成，直接落库
                db.insert_sim_account(a.account_id, a.name, float(a.capital),
                                      float(stop_loss), "created",
                                      a.strategy_id, a.start_date, "minute")
                aid = a.account_id
            else:
                aid = service.account_create(a.name, a.capital, stop_loss,
                                             a.strategy_id, a.start_date)
            print(f"created account: {aid}")
        if a.autostart:
            service.account_start(aid)
            print(f"started account: {aid}")
        return
    if args and args[0] == "--reset":
        if len(args) < 2:
            print("usage: run_quant_sim.py --reset <account_id>", file=sys.stderr)
            sys.exit(1)
        from app.quant import service
        service.account_reset(args[1])
        print(f"reset account: {args[1]}")
        return
    if not args:
        print("usage: run_quant_sim.py <account_id> | --create ... | --reset <account_id>",
              file=sys.stderr)
        sys.exit(1)
    run_loop(args[0])


if __name__ == "__main__":
    main()
