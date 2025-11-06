# -*- coding: utf-8 -*-
"""测试导入和基本功能"""
import sys
import io
from pathlib import Path

# 设置标准输出为UTF-8编码
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent))

print("=" * 60)
print("JQQuant 导入测试")
print("=" * 60)

# 测试配置管理器
try:
    from config.config_manager import get_config_manager
    config_manager = get_config_manager()
    print("[OK] 配置管理器导入成功")
    
    # 测试加载配置
    jq_config = config_manager.get_jqdata_config()
    if jq_config.get('username'):
        print(f"[OK] 聚宽配置加载成功 (用户名: {jq_config['username']})")
    else:
        print("[ERROR] 聚宽配置为空")
except Exception as e:
    print(f"[ERROR] 配置管理器错误: {e}")

# 测试聚宽客户端
try:
    from jqdata.client import JQDataClient
    print("[OK] 聚宽客户端导入成功")
except Exception as e:
    print(f"[ERROR] 聚宽客户端导入失败: {e}")

# 测试核心模块
try:
    from core.backtest_engine import BacktestEngine
    from core.data_provider import DataProvider
    from core.portfolio import Portfolio
    from core.order_manager import OrderManager
    print("[OK] 核心模块导入成功")
except Exception as e:
    print(f"[ERROR] 核心模块导入失败: {e}")

# 测试策略模块
try:
    from strategies.base_strategy import BaseStrategy
    from strategies.examples.ma_cross import MACrossStrategy
    print("[OK] 策略模块导入成功")
except Exception as e:
    print(f"[ERROR] 策略模块导入失败: {e}")

print("=" * 60)

