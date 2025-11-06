# -*- coding: utf-8 -*-
"""
运行A股自适应动量策略 v2.0 回测（改进版）
"""
import sys
import io
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

# 使用环境变量设置编码，而不是直接包装stdout/stderr
if sys.platform == 'win32':
    import os
    os.environ['PYTHONIOENCODING'] = 'utf-8'

from main import run_backtest
import json

# 加载股票池
stock_pool_file = Path(__file__).parent / 'config' / 'stock_pool.json'
with open(stock_pool_file, 'r', encoding='utf-8') as f:
    stock_pool = json.load(f)

# 获取高增长股票列表（去重）
securities = list(set(stock_pool['high_growth_stocks']))

try:
    # 运行回测 - 使用改进版策略
    results = run_backtest(
        strategy_name='adaptive_momentum_a_v2',
        start_date='2024-08-01',
        end_date='2024-10-31',
        securities=securities,
        initial_cash=1000000,
        commission_rate=0.0003,
        slippage=0.001,
        strategy_params={
            'benchmark': '000300.XSHG',
            'growth_index': '399006.XSHE',
            'roc_10_min': 0.015,  # 降低门槛，增加选股
            'roc_20_min': 0.025,
            'max_positions': 8,   # 增加持仓数
            'position_size': 0.12, # 降低单股仓位
            'stop_loss': 0.08,    # 更严格止损
            'take_profit': 0.40,  # 降低止盈，提高胜率
            'use_dynamic_pool': True,
            'use_fundamental': False,  # 暂时关闭，因为需要额外API
            'use_relative_strength': True  # 启用相对强度筛选
        }
    )
    
    if results:
        try:
            print("\n回测完成！")
        except:
            pass
    else:
        try:
            print("\n回测失败，请检查错误信息。")
        except:
            pass
except Exception as e:
    try:
        print(f"\n回测过程中发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
    except:
        import logging
        logging.error(f"回测过程中发生错误: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())

