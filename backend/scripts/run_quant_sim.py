"""模拟盘独立进程：由 FastAPI 派生子进程或 pm2/nohup 守护。

用法：
  python scripts/run_quant_sim.py <account_id>          # 运行已有账户（FastAPI/pm2 调用）
  python scripts/run_quant_sim.py --create --name N --capital C --stop-loss S \
      --strategy-id SID --start-date YYYY-MM-DD [--autostart] [--account-id AID]
      # 创建账户（可指定 aid 便于验收对齐），--autostart 时经内存门禁拉起
  python scripts/run_quant_sim.py --reset <account_id>  # 清状态并重新回补
"""
from __future__ import annotations

import sys


def main():
    args = sys.argv[1:]
    if args and args[0] == "--create":
        import argparse
        from app.quant import service

        p = argparse.ArgumentParser(prog="run_quant_sim.py --create")
        p.add_argument("--name", required=True)
        p.add_argument("--capital", type=float, required=True)
        p.add_argument("--stop-loss", dest="stop_loss", type=float, default=0.05)
        p.add_argument("--strategy-id", dest="strategy_id", required=True,
                       help="strategies 表已注册的策略 id")
        p.add_argument("--start-date", dest="start_date", required=True,
                       help="回放/补跑起始交易日（历史对齐用）")
        p.add_argument("--account-id", dest="account_id", default=None,
                       help="指定账户 id（验收对齐用固定 id）；缺省自动生成")
        p.add_argument("--autostart", action="store_true",
                       help="创建后立即经内存门禁拉起子进程")
        a = p.parse_args(args[1:])
        if a.account_id:
            import uuid
            # 固定 id：绕过 account_create 的 uuid 生成，直接落库
            from app.quant import db
            db.insert_sim_account(a.account_id, a.name, float(a.capital),
                                  float(a.stop_loss), "created",
                                  a.strategy_id, a.start_date, "minute")
            aid = a.account_id
        else:
            aid = service.account_create(a.name, a.capital, a.stop_loss,
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
    from app.quant.simulate.runner import run_loop
    run_loop(args[0])


if __name__ == "__main__":
    main()
