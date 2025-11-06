"""
系统配置文件
"""
import os
from pathlib import Path
from typing import Dict, Any
import json

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent

# 数据目录
DATA_DIR = PROJECT_ROOT / "data"
RESULTS_DIR = PROJECT_ROOT / "results"
LOGS_DIR = PROJECT_ROOT / "logs"
STRATEGIES_DIR = PROJECT_ROOT / "strategies"

# 创建必要的目录
for dir_path in [DATA_DIR, RESULTS_DIR, LOGS_DIR]:
    dir_path.mkdir(exist_ok=True)

# 默认回测参数
DEFAULT_BACKTEST_CONFIG = {
    "initial_cash": 1000000,  # 初始资金
    "commission_rate": 0.0003,  # 手续费率
    "slippage": 0.001,  # 滑点
    "benchmark": "000300.XSHG",  # 基准指数（沪深300）
}

# 加载聚宽配置
def load_jqdata_config() -> Dict[str, Any]:
    """加载聚宽配置"""
    config_path = PROJECT_ROOT / "config" / "jqdata_config.json"
    if config_path.exists():
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

# 日志配置
LOG_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file": LOGS_DIR / "jqquant.log"
}

