# -*- coding: utf-8 -*-
"""
运行A股自适应动量策略回测
"""
import sys
import io
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

# 使用环境变量设置编码，而不是直接包装stdout/stderr
# 这样可以避免与jqdatasdk冲突
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
securities = list(set(stock_pool['high_growth_stocks']))[:30]  # 使用前30只股票

try:
    # 运行回测
    results = run_backtest(
        strategy_name='adaptive_momentum_a',
        start_date='2024-08-01',
        end_date='2024-10-31',
        securities=securities,
        initial_cash=1000000,
        commission_rate=0.0003,
        slippage=0.001,
        strategy_params={
            'benchmark': '000300.XSHG',
            'growth_index': '399006.XSHE',
            'roc_10_min': 0.02,
            'roc_20_min': 0.03,
            'max_positions': 7,
            'position_size': 0.15,
            'stop_loss': 0.10,
            'take_profit': 0.50
        }
    )
    
    if results:
        try:
            print("\n回测完成！")
        except:
            pass  # 如果stdout已关闭，忽略错误
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
        # 如果stdout已关闭，使用logger
        import logging
        logging.error(f"回测过程中发生错误: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())

