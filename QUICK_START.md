# 快速开始指南

## 1. 环境准备

### 安装Python依赖

```bash
# 确保已加载PowerShell配置（禁用代理）
. $PROFILE

# 安装依赖
pip install -r requirements.txt
```

**注意**: TA-Lib在Windows上可能需要额外步骤：
1. 下载预编译的wheel文件：https://www.lfd.uci.edu/~gohlke/pythonlibs/#ta-lib
2. 安装：`pip install TA_Lib‑0.4.xx‑cp38‑cp38‑win_amd64.whl`

## 2. 配置聚宽账号

编辑 `config/jqdata_config.json`:

```json
{
  "username": "您的聚宽用户名",
  "password": "您的聚宽密码",
  "api_endpoint": "https://dataapi.joinquant.com",
  "timeout": 30,
  "retry_times": 3
}
```

## 3. 运行第一个回测

### 方式1：命令行

```bash
python main.py --strategy ma_cross --start 2020-01-01 --end 2023-12-31 --securities 000001.XSHE 000002.XSHE
```

### 方式2：Cursor任务

1. 按 `Ctrl+Shift+P` 打开命令面板
2. 输入 "Tasks: Run Task"
3. 选择 "JQQuant: 运行回测"

### 方式3：修改任务参数

编辑 `.vscode/tasks.json`，修改参数后运行任务。

## 4. 查看结果

回测结果会：
- 在控制台输出摘要信息
- 保存JSON文件到 `results/` 目录
- 日志保存到 `logs/jqquant.log`

## 5. 开发自己的策略

### 创建策略文件

在 `strategies/` 目录下创建新文件，例如 `my_strategy.py`:

```python
from strategies.base_strategy import BaseStrategy
import pandas as pd
from datetime import datetime

class MyStrategy(BaseStrategy):
    def __init__(self):
        super().__init__(name="My Strategy")
    
    def initialize(self, context):
        super().initialize(context)
        # 初始化逻辑
    
    def handle_data(self, data: pd.DataFrame, date: datetime):
        # 策略逻辑
        portfolio = self.context['portfolio']
        order_manager = self.context['order_manager']
        
        # 您的交易逻辑
        pass
```

### 注册策略

在 `main.py` 的 `load_strategy` 函数中添加：

```python
elif strategy_name == 'my_strategy':
    from strategies.my_strategy import MyStrategy
    return MyStrategy()
```

## 6. 常见问题

### Q: 聚宽认证失败？
A: 检查 `config/jqdata_config.json` 中的账号密码是否正确。

### Q: 数据获取失败？
A: 确保网络连接正常，聚宽API服务可用。

### Q: 回测结果异常？
A: 检查策略逻辑，查看日志文件 `logs/jqquant.log`。

### Q: 如何添加更多技术指标？
A: 使用TA-Lib或pandas计算，在策略的 `handle_data` 中使用。

## 7. 下一步

- 阅读 `IMPLEMENTATION_PLAN.md` 了解完整架构
- 查看 `strategies/examples/` 中的示例策略
- 阅读聚宽API文档：https://www.joinquant.com/help/api/help

