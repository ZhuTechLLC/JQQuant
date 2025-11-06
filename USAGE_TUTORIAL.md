# JQQuant 使用教程

## 目录

1. [环境准备](#环境准备)
2. [快速开始](#快速开始)
3. [详细使用指南](#详细使用指南)
4. [常见问题](#常见问题)
5. [错误排查](#错误排查)

---

## 环境准备

### 1. 安装Python依赖

```bash
# 确保已加载PowerShell配置（禁用代理）
. $PROFILE

# 安装依赖
pip install -r requirements.txt
```

**重要依赖说明：**

- `jqdatasdk` - 聚宽数据API（必需）
- `pandas`, `numpy` - 数据处理（必需）
- `matplotlib` - 可视化（可选）
- `TA-Lib` - 技术指标（可选，Windows需要额外安装）

**TA-Lib Windows安装：**

1. 下载预编译wheel：https://www.lfd.uci.edu/~gohlke/pythonlibs/#ta-lib
2. 选择对应Python版本的whl文件
3. 安装：`pip install TA_Lib‑0.4.xx‑cp38‑cp38‑win_amd64.whl`

### 2. 配置聚宽账号

配置文件位置：`config/jqdata_config.json`

```json
{
  "username": "18072069583",
  "password": "%5Diamond",
  "api_endpoint": "https://dataapi.joinquant.com",
  "timeout": 30,
  "retry_times": 3
}
```

**注意：** 配置文件已配置好，无需修改（除非更换账号）。

### 3. 验证安装

运行测试脚本：

```bash
python test_imports.py
```

应该看到所有模块导入成功。

---

## 快速开始

### 方式1：命令行运行（推荐）

```bash
python main.py --strategy ma_cross --start 2020-01-01 --end 2023-12-31 --securities 000001.XSHE 000002.XSHE
```

**参数说明：**
- `--strategy`: 策略名称（当前支持 `ma_cross`）
- `--start`: 回测开始日期 (YYYY-MM-DD)
- `--end`: 回测结束日期 (YYYY-MM-DD)
- `--securities`: 股票代码列表（聚宽格式，如 `000001.XSHE`）
- `--cash`: 初始资金（默认1000000）
- `--commission`: 手续费率（默认0.0003）
- `--slippage`: 滑点（默认0.001）
- `--short-window`: 短期均线周期（MA策略，默认5）
- `--long-window`: 长期均线周期（MA策略，默认20）

**示例：**

```bash
# 使用默认参数
python main.py --strategy ma_cross --start 2020-01-01 --end 2023-12-31 --securities 000001.XSHE

# 自定义参数
python main.py --strategy ma_cross --start 2020-01-01 --end 2023-12-31 --securities 000001.XSHE 000002.XSHE --cash 2000000 --short-window 10 --long-window 30
```

### 方式2：使用Cursor任务

1. 按 `Ctrl+Shift+P` 打开命令面板
2. 输入 "Tasks: Run Task"
3. 选择 "JQQuant: 运行回测"

**修改任务参数：**

编辑 `.vscode/tasks.json`，修改 `args` 部分：

```json
{
    "label": "JQQuant: 运行回测",
    "type": "shell",
    "command": "python",
    "args": [
        "main.py",
        "--strategy", "ma_cross",
        "--start", "2020-01-01",
        "--end", "2023-12-31",
        "--securities", "000001.XSHE", "000002.XSHE"
    ]
}
```

### 方式3：Python脚本

创建 `my_backtest.py`：

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from main import run_backtest

# 运行回测
results = run_backtest(
    strategy_name='ma_cross',
    start_date='2020-01-01',
    end_date='2023-12-31',
    securities=['000001.XSHE', '000002.XSHE'],
    initial_cash=1000000,
    commission_rate=0.0003,
    slippage=0.001,
    strategy_params={'short_window': 5, 'long_window': 20}
)

if results:
    print("回测完成！")
    print(f"总收益: {results['summary']['total_profit_rate']*100:.2f}%")
```

运行：

```bash
python my_backtest.py
```

---

## 详细使用指南

### 1. 股票代码格式

聚宽使用特定的股票代码格式：

- **深圳股票**: `000001.XSHE` (平安银行)
- **上海股票**: `600000.XSHG` (浦发银行)
- **指数**: `000300.XSHG` (沪深300)

**获取股票代码：**

```python
from jqdata.client import JQDataClient
from config.config_manager import get_config_manager

# 初始化客户端
config_manager = get_config_manager()
config = config_manager.get_jqdata_config()
jq_client = JQDataClient()
jq_client.authenticate(config['username'], config['password'])

# 获取所有股票
all_stocks = jq_client.get_all_securities(types=['stock'], date='2023-12-31')
print(all_stocks.head())

# 获取指数成分股
hs300_stocks = jq_client.get_index_stocks('000300.XSHG', date='2023-12-31')
print(f"沪深300成分股数量: {len(hs300_stocks)}")
```

### 2. 开发自定义策略

#### 步骤1：创建策略文件

在 `strategies/` 目录下创建新文件，例如 `my_strategy.py`：

```python
from strategies.base_strategy import BaseStrategy
import pandas as pd
from datetime import datetime
from core.order_manager import OrderType

class MyStrategy(BaseStrategy):
    def __init__(self):
        super().__init__(name="My Strategy")
        # 初始化策略参数
        self.param1 = 10
        self.param2 = 20
    
    def initialize(self, context):
        """策略初始化"""
        super().initialize(context)
        # 在这里可以获取股票列表、设置参数等
        print(f"策略 {self.name} 初始化完成")
    
    def handle_data(self, data: pd.DataFrame, date: datetime):
        """处理每日数据"""
        portfolio = self.context['portfolio']
        order_manager = self.context['order_manager']
        data_provider = self.context['data_provider']
        securities = self.context['securities']
        
        # 您的策略逻辑
        for security in securities:
            # 获取持仓
            position = portfolio.get_position(security)
            has_position = position is not None and position.amount > 0
            
            # 示例：简单的买入卖出逻辑
            if not has_position:
                # 买入条件
                order_manager.create_order(
                    security, 
                    100, 
                    order_type=OrderType.MARKET
                )
            else:
                # 卖出条件
                order_manager.create_order(
                    security, 
                    -position.amount, 
                    order_type=OrderType.MARKET
                )
    
    def get_parameters(self):
        """返回策略参数"""
        return {
            'param1': self.param1,
            'param2': self.param2
        }
```

#### 步骤2：注册策略

在 `main.py` 的 `load_strategy` 函数中添加：

```python
def load_strategy(strategy_name: str, **kwargs):
    if strategy_name == 'ma_cross':
        # ... 现有代码
    elif strategy_name == 'my_strategy':
        from strategies.my_strategy import MyStrategy
        return MyStrategy()
    else:
        raise ValueError(f"未知的策略: {strategy_name}")
```

#### 步骤3：运行策略

```bash
python main.py --strategy my_strategy --start 2020-01-01 --end 2023-12-31 --securities 000001.XSHE
```

### 3. 查看回测结果

回测完成后，结果会：

1. **控制台输出** - 显示关键指标
2. **JSON文件** - 保存到 `results/` 目录
3. **日志文件** - 保存到 `logs/jqquant.log`

**结果文件格式：**

```json
{
  "strategy": "ma_cross",
  "start_date": "2020-01-01",
  "end_date": "2023-12-31",
  "securities": ["000001.XSHE"],
  "summary": {
    "initial_cash": 1000000,
    "current_cash": 950000,
    "total_value": 1200000,
    "total_profit": 200000,
    "total_profit_rate": 0.2
  },
  "metrics": {
    "total_return": 0.2,
    "annual_return": 0.05,
    "sharpe_ratio": 1.2,
    "max_drawdown": 0.15,
    "total_trades": 50
  }
}
```

### 4. 可视化结果

使用可视化模块：

```python
from utils.visualization import plot_backtest_results
import json
from pathlib import Path

# 加载结果
results_file = Path("results/backtest_ma_cross_20231231_120000.json")
with open(results_file, 'r', encoding='utf-8') as f:
    results = json.load(f)

# 绘制图表
plot_backtest_results(results, save_path=Path("results/chart.png"))
```

---

## 常见问题

### Q1: 聚宽认证失败

**错误信息：**
```
聚宽认证失败: ...
```

**解决方案：**

1. 检查 `config/jqdata_config.json` 中的账号密码是否正确
2. 确认网络连接正常
3. 检查聚宽账号是否有效
4. 查看日志文件 `logs/jqquant.log` 获取详细错误信息

### Q2: 数据获取失败

**错误信息：**
```
未获取到数据
```

**解决方案：**

1. 检查股票代码格式是否正确（如 `000001.XSHE`）
2. 确认日期范围有效（不能是未来日期）
3. 检查聚宽账号是否有数据权限
4. 尝试单个股票代码测试

### Q3: 导入模块失败

**错误信息：**
```
ModuleNotFoundError: No module named 'xxx'
```

**解决方案：**

1. 确保已安装所有依赖：`pip install -r requirements.txt`
2. 检查Python版本（需要3.8+）
3. 确认在项目根目录运行命令

### Q4: 策略执行错误

**错误信息：**
```
处理日期 xxx 时出错: ...
```

**解决方案：**

1. 查看日志文件获取详细错误
2. 检查策略代码逻辑
3. 确保数据格式正确
4. 检查订单创建逻辑

### Q5: 回测结果异常

**可能原因：**

1. 策略逻辑错误
2. 数据质量问题
3. 参数设置不合理

**调试方法：**

1. 减少回测时间范围
2. 使用单个股票测试
3. 添加日志输出
4. 检查订单执行情况

---

## 错误排查

### 1. 检查配置

```python
from config.config_manager import get_config_manager

config_manager = get_config_manager()
jq_config = config_manager.get_jqdata_config()
print(f"用户名: {jq_config.get('username')}")
print(f"密码已配置: {bool(jq_config.get('password'))}")
```

### 2. 测试聚宽连接

```python
from jqdata.client import JQDataClient
from config.config_manager import get_config_manager

config_manager = get_config_manager()
config = config_manager.get_jqdata_config()

jq_client = JQDataClient()
if jq_client.authenticate(config['username'], config['password']):
    print("认证成功！")
    
    # 测试获取数据
    data = jq_client.get_price(
        securities='000001.XSHE',
        start_date='2023-12-01',
        end_date='2023-12-31',
        frequency='daily'
    )
    print(f"获取到 {len(data)} 条数据")
else:
    print("认证失败，请检查账号密码")
```

### 3. 检查模块导入

运行测试脚本：

```bash
python test_imports.py
```

### 4. 查看日志

日志文件位置：`logs/jqquant.log`

```bash
# 查看最后50行
Get-Content logs/jqquant.log -Tail 50

# 搜索错误
Select-String -Path logs/jqquant.log -Pattern "ERROR"
```

### 5. 调试策略

在策略中添加日志：

```python
import logging
logger = logging.getLogger(__name__)

def handle_data(self, data: pd.DataFrame, date: datetime):
    logger.info(f"处理日期: {date}")
    logger.debug(f"数据: {data}")
    # ... 策略逻辑
```

---

## 下一步

1. **完善策略** - 实现真实的交易逻辑
2. **优化参数** - 使用参数优化工具
3. **添加指标** - 使用TA-Lib计算技术指标
4. **风险管理** - 添加止损、仓位管理等
5. **实盘交易** - 集成实盘交易接口

---

## 获取帮助

- 查看 `README.md` - 项目概述
- 查看 `IMPLEMENTATION_PLAN.md` - 实施计划
- 查看 `QUICK_START.md` - 快速开始
- 查看日志文件 - 详细错误信息
- 聚宽API文档 - https://www.joinquant.com/help/api/help

---

**祝您使用愉快！**

