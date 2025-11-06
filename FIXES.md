# 修复记录

## 2024-11-07 修复内容（续）

### 6. 增强可视化功能

**问题**: 可视化功能简单，缺乏详细的回测分析图表

**修复**:
- 增强 `plot_backtest_results()` 函数，添加更多图表：
  - 资产曲线（包含现金和总资产）
  - 累计收益率曲线
  - 回撤曲线（新增）
  - 日收益率分布直方图（新增）
  - 性能指标表格（新增）
- 新增 `plot_comparison()` 函数，支持多策略对比：
  - 资产曲线对比
  - 总收益率对比
  - 夏普比率对比
  - 最大回撤对比
- 改进图表样式和布局，使用更专业的配色
- 支持中文字体显示
- 添加 `show` 参数控制是否显示图表

**文件**: `utils/visualization.py`

**优势**:
- 更全面的回测结果分析
- 支持策略对比分析
- 专业的图表展示
- 便于生成报告

### 7. 优化数据提供者缓存机制

**问题**: 数据提供者只有内存缓存，程序重启后需要重新获取数据

**修复**:
- 添加持久化磁盘缓存机制
- 使用pickle格式保存数据，提高加载速度
- 添加缓存元数据（JSON格式），包含缓存键、时间戳等信息
- 使用MD5哈希避免文件名过长
- 添加缓存验证机制，确保缓存数据正确
- 新增 `get_cache_info()` 方法获取缓存统计信息
- 增强 `clear_cache()` 方法，支持清空磁盘缓存

**文件**: `core/data_provider.py`

**优势**:
- 程序重启后无需重新获取数据
- 减少API调用次数，提高效率
- 支持缓存管理（查看、清空）
- 提高回测速度

### 5. 创建技术指标工具模块

**问题**: 策略中技术指标计算代码重复，缺乏统一的技术指标工具

**修复**:
- 创建 `utils/indicators.py` 技术指标工具模块
- 实现常用技术指标：
  - SMA/EMA（简单/指数移动平均线）
  - RSI（相对强弱指标）
  - MACD（指数平滑异同移动平均线）
  - 布林带（Bollinger Bands）
  - ROC（变动率指标）
  - ATR（平均真实波幅）
  - 随机指标（Stochastic Oscillator）
  - 均线交叉判断（ma_cross）
  - 成交量比率（volume_ratio）
- 支持numpy数组和pandas Series两种输入格式
- 更新 `utils/__init__.py` 导出所有技术指标函数
- 更新 `ma_cross.py` 策略使用新的indicators模块

**文件**: 
- `utils/indicators.py`（新建）
- `utils/__init__.py`
- `strategies/examples/ma_cross.py`

**优势**:
- 代码复用性提高
- 策略代码更简洁
- 统一的技术指标计算逻辑
- 易于测试和维护
- 方便添加新的技术指标

## 2024-11-07 修复内容

### 4. 均线交叉策略完善

**问题**: `strategies/examples/ma_cross.py` 策略只是占位符，没有实现真正的均线计算逻辑

**修复**:
- 实现真正的均线计算：获取足够的历史数据（至少 long_window + 10 天）
- 计算短期和长期移动平均线
- 实现金叉/死叉判断逻辑：
  - 金叉（买入信号）：短期均线上穿长期均线
  - 死叉（卖出信号）：短期均线下穿长期均线
- 维护上一期均线状态以准确判断交叉信号
- 优化买入数量计算（使用可用资金的80%，按100股取整）
- 支持单股票和多股票数据格式

**文件**: `strategies/examples/ma_cross.py`

**改进点**:
- 使用 `numpy` 计算均线，性能更好
- 正确处理多股票数据格式（包含 'security' 列的情况）
- 添加详细的日志记录，便于调试
- 添加异常处理，提高稳定性

## 2024-11-06 修复内容

### 1. 聚宽API参数错误修复

**问题**: `get_price() got an unexpected keyword argument 'securities'`

**原因**: 聚宽API的 `jq.get_price()` 函数参数是 `security`（单只股票），不是 `securities`（多只股票）

**修复**:
- 修改 `jqdata/client.py` 中的 `get_price` 方法
- 单只股票：使用 `security` 参数
- 多只股票：循环调用 `get_price`，然后合并数据，并添加 `security` 列用于区分

**文件**: `jqdata/client.py`

### 2. 数据格式处理优化

**问题**: 多只股票数据格式处理不正确

**修复**:
- 更新 `core/backtest_engine.py` 中的数据访问逻辑
- 支持单股票（Series）和多股票（DataFrame with 'security' column）两种格式
- 修复价格获取逻辑，正确处理多股票数据

**文件**: `core/backtest_engine.py`

### 3. Unicode编码问题修复

**问题**: Windows PowerShell中Unicode字符显示错误

**修复**:
- 在所有Python文件开头添加 `# -*- coding: utf-8 -*-`
- 在 `main.py` 和 `test_imports.py` 中添加UTF-8编码设置
- 创建 `setup_encoding.ps1` 脚本用于配置PowerShell编码

**文件**: 
- `main.py`
- `test_imports.py`
- `setup_encoding.ps1`

## 测试建议

运行以下命令测试修复：

```bash
# 1. 测试导入
python test_imports.py

# 2. 运行回测（单只股票）
python main.py --strategy ma_cross --start 2020-01-01 --end 2023-12-31 --securities 000001.XSHE

# 3. 运行回测（多只股票）
python main.py --strategy ma_cross --start 2020-01-01 --end 2023-12-31 --securities 000001.XSHE 000002.XSHE
```

## 注意事项

1. **数据格式**: 聚宽返回的多股票数据现在包含 `security` 列用于区分不同股票
2. **性能**: 多股票回测时，数据获取需要循环调用API，可能较慢
3. **编码**: 在Windows上运行前，建议先运行 `setup_encoding.ps1` 或设置环境变量

