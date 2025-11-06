# JQQuant 实施计划

## 项目概述

JQQuant 是一个基于聚宽API的量化交易平台，类似QuantConnect的本地化解决方案，可以在Cursor IDE中直接进行策略开发、回测和交易。

## 可行性分析

### ✅ 完全可行

**理由：**
1. **聚宽API成熟稳定** - 提供完整的数据接口和交易接口
2. **Python生态丰富** - 有大量量化交易库支持
3. **Cursor基于VSCode** - 支持完整的Python开发和调试
4. **架构设计合理** - 模块化设计，易于扩展和维护

## 核心功能模块

### 1. 数据层 ✅
- **聚宽API集成** (`jqdata/`)
  - 认证管理
  - 数据获取（价格、财务、指数等）
  - 数据缓存机制

### 2. 回测引擎 ✅
- **事件驱动架构** (`core/backtest_engine.py`)
  - 按日期遍历
  - 订单处理
  - 持仓管理
  - 性能计算

### 3. 策略框架 ✅
- **策略基类** (`strategies/base_strategy.py`)
  - 统一的策略接口
  - 生命周期管理
  - 参数管理

### 4. 投资组合管理 ✅
- **持仓管理** (`core/portfolio.py`)
- **订单管理** (`core/order_manager.py`)
- **风险控制**

### 5. 可视化与报告 ✅
- **结果可视化** (`utils/visualization.py`)
- **性能指标计算**
- **报告生成**

### 6. Cursor集成 ✅
- **任务配置** (`.vscode/tasks.json`)
- **设置配置** (`.vscode/settings.json`)
- **命令行接口** (`main.py`)

## 实施步骤

### 阶段1：基础框架 ✅ (已完成)
- [x] 项目结构设计
- [x] 聚宽API集成
- [x] 回测引擎核心
- [x] 策略框架
- [x] 基础可视化

### 阶段2：功能完善 (进行中)
- [x] 完善数据获取逻辑 ✅（添加持久化缓存）
- [ ] 优化回测性能
- [x] 完善策略示例 ✅
- [x] 增强可视化功能 ✅
- [x] 添加更多技术指标 ✅

### 阶段3：高级功能
- [ ] 策略参数优化
- [ ] 多策略组合
- [ ] 风险管理模块
- [ ] 实时数据流
- [ ] 实盘交易接口

### 阶段4：IDE深度集成
- [ ] Cursor扩展开发（可选）
- [ ] 代码片段模板
- [ ] 调试配置优化
- [ ] 结果面板集成

## 使用方式

### 方式1：命令行
```bash
python main.py --strategy ma_cross --start 2020-01-01 --end 2023-12-31 --securities 000001.XSHE 000002.XSHE
```

### 方式2：Cursor任务
1. 按 `Ctrl+Shift+P` 打开命令面板
2. 选择 "Tasks: Run Task"
3. 选择 "JQQuant: 运行回测"

### 方式3：Python脚本
```python
from jqdata.client import JQDataClient
from core.backtest_engine import BacktestEngine
from strategies.examples.ma_cross import MACrossStrategy

# 初始化并运行回测
```

## 技术栈

- **Python 3.8+**
- **聚宽API** (jqdatasdk)
- **数据处理**: pandas, numpy
- **可视化**: matplotlib, plotly
- **技术指标**: TA-Lib
- **IDE**: Cursor (基于VSCode)

## 与QuantConnect的对比

| 特性 | QuantConnect | JQQuant |
|------|-------------|---------|
| 数据源 | 全球市场 | 中国市场（聚宽） |
| 部署方式 | 云端 | 本地 |
| 成本 | 订阅制 | 免费（需聚宽账号） |
| IDE集成 | Web IDE | Cursor/VSCode |
| 策略语言 | C#, Python | Python |
| 回测速度 | 云端计算 | 本地计算 |
| 数据更新 | 自动 | 手动/定时 |

## 优势

1. **本地化部署** - 数据隐私可控
2. **IDE集成** - 使用熟悉的开发环境
3. **灵活扩展** - 可以自定义任何功能
4. **成本可控** - 只需聚宽数据费用
5. **学习曲线** - 基于Python，易于上手

## 注意事项

1. **聚宽账号** - 需要有效的聚宽账号和数据权限
2. **数据限制** - 受聚宽API调用频率限制
3. **网络要求** - 需要稳定的网络连接
4. **计算资源** - 大规模回测需要足够的计算资源

## 下一步行动

1. **安装依赖**
   ```bash
   pip install -r requirements.txt
   ```

2. **配置聚宽账号**
   - 编辑 `config/jqdata_config.json`
   - 填入您的聚宽账号信息

3. **运行示例策略**
   ```bash
   python main.py --strategy ma_cross --start 2020-01-01 --end 2023-12-31 --securities 000001.XSHE
   ```

4. **开发自己的策略**
   - 继承 `BaseStrategy`
   - 实现 `handle_data` 方法
   - 在 `strategies/` 目录下创建新文件

## 开发路线图

```
当前状态: 基础框架完成 ✅
    ↓
完善功能 → 性能优化 → 高级特性 → 生产就绪
    ↓           ↓          ↓          ↓
  2-4周       4-8周      8-12周     12+周
```

## 贡献指南

欢迎提交Issue和Pull Request！

## 许可证

MIT License

