"""检查交易数据的净现金流计算是否正确"""
import json
from pathlib import Path

# 读取回测结果
result_file = Path("results/backtest_adaptive_momentum_a_v2_20251107_030012.json")
with open(result_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

summary = data['summary']
trade_history = data['trade_history']

print("=" * 80)
print("交易数据验证")
print("=" * 80)
print(f"\n初始资金: {summary['initial_cash']:,.2f}")
print(f"最终总资产: {summary['total_value']:,.2f}")
print(f"最终现金: {summary['current_cash']:,.2f}")
print(f"总盈利: {summary['total_profit']:,.2f}")
print(f"总收益率: {summary['total_profit_rate']*100:.2f}%")

# 计算买入和卖出的现金流
total_buy_value = 0  # 买入总金额（不含佣金）
total_buy_commission = 0  # 买入佣金
total_sell_value = 0  # 卖出总金额（不含佣金）
total_sell_commission = 0  # 卖出佣金

# 当前代码计算的净现金流（错误的）
total_net_outflow_wrong = 0  # 错误的净流出
total_net_inflow_wrong = 0  # 错误的净流入

# 正确的净现金流
total_net_outflow_correct = 0  # 正确的净流出（买入）
total_net_inflow_correct = 0  # 正确的净流入（卖出）

for trade in trade_history:
    if trade['type'] == '买入':
        total_buy_value += trade['value']
        total_buy_commission += trade['commission']
        # 当前代码：net_value = value - commission（错误！买入应该是流出）
        total_net_outflow_wrong += trade['net_value']
        # 正确：买入净流出 = value + commission
        total_net_outflow_correct += trade['value'] + trade['commission']
    else:  # 卖出
        total_sell_value += trade['value']
        total_sell_commission += trade['commission']
        # 当前代码：net_value = value + commission（错误！卖出应该是流入）
        total_net_inflow_wrong += trade['net_value']
        # 正确：卖出净流入 = value - commission
        total_net_inflow_correct += trade['value'] - trade['commission']

print("\n" + "=" * 80)
print("交易统计（当前代码的计算）")
print("=" * 80)
print(f"买入交易次数: {sum(1 for t in trade_history if t['type'] == '买入')}")
print(f"卖出交易次数: {sum(1 for t in trade_history if t['type'] == '卖出')}")
print(f"总交易次数: {len(trade_history)}")
print(f"\n买入总金额（不含佣金）: {total_buy_value:,.2f}")
print(f"买入总佣金: {total_buy_commission:,.2f}")
print(f"卖出总金额（不含佣金）: {total_sell_value:,.2f}")
print(f"卖出总佣金: {total_sell_commission:,.2f}")

print("\n" + "=" * 80)
print("净现金流计算（当前代码 - 错误）")
print("=" * 80)
print(f"买入净流出（错误计算）: {total_net_outflow_wrong:,.2f}")
print(f"卖出净流入（错误计算）: {total_net_inflow_wrong:,.2f}")
print(f"净现金流（错误）: {total_net_inflow_wrong - total_net_outflow_wrong:,.2f}")

print("\n" + "=" * 80)
print("净现金流计算（正确）")
print("=" * 80)
print(f"买入净流出（正确）: {total_net_outflow_correct:,.2f}")
print(f"卖出净流入（正确）: {total_net_inflow_correct:,.2f}")
print(f"净现金流（正确）: {total_net_inflow_correct - total_net_outflow_correct:,.2f}")

print("\n" + "=" * 80)
print("验证")
print("=" * 80)
# 正确的计算：最终现金 = 初始现金 - 买入净流出 + 卖出净流入
final_cash_calculated = summary['initial_cash'] - total_net_outflow_correct + total_net_inflow_correct
print(f"初始现金: {summary['initial_cash']:,.2f}")
print(f"买入净流出: {total_net_outflow_correct:,.2f}")
print(f"卖出净流入: {total_net_inflow_correct:,.2f}")
print(f"计算最终现金: {final_cash_calculated:,.2f}")
print(f"实际最终现金: {summary['current_cash']:,.2f}")
print(f"差异: {abs(final_cash_calculated - summary['current_cash']):,.2f}")

# 总资产 = 现金 + 持仓市值
print(f"\n最终总资产: {summary['total_value']:,.2f}")
print(f"最终现金: {summary['current_cash']:,.2f}")
print(f"持仓市值: {summary['total_value'] - summary['current_cash']:,.2f}")

# 盈利 = 最终总资产 - 初始资金
profit_calculated = summary['total_value'] - summary['initial_cash']
print(f"\n计算盈利: {profit_calculated:,.2f}")
print(f"实际盈利: {summary['total_profit']:,.2f}")

print("\n" + "=" * 80)
print("问题诊断")
print("=" * 80)
print("当前代码中net_value的计算逻辑：")
print("  买入: net_value = value - commission  ❌ 错误！应该是 value + commission")
print("  卖出: net_value = value + commission  ❌ 错误！应该是 value - commission")
print("\n正确的逻辑应该是：")
print("  买入: net_value = -(value + commission)  （负值表示流出）")
print("  卖出: net_value = value - commission      （正值表示流入）")

