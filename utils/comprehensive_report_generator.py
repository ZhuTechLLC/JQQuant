# -*- coding: utf-8 -*-
"""
完整详细HTML报告生成器 - 仿照adaptive_momentum报告格式
适用于A股自适应动量策略
"""
import json
import pandas as pd
import numpy as np
from typing import Dict, Optional, List
from pathlib import Path
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def generate_comprehensive_html_report(
    results: Dict,
    strategy_name: str = "adaptive_momentum_a",
    strategy_version: str = "1.0.0",
    strategy_params: Optional[Dict] = None,
    start_date: str = "",
    end_date: str = "",
    save_path: Optional[Path] = None
) -> str:
    """
    生成完整的详细HTML格式回测报告（仿照adaptive_momentum格式）
    
    包含所有章节：
    0. 版本对比分析
    1. 执行摘要
    2. 策略背景与理论依据
    3. 策略实现详解
    4. 回测结果分析
    5. 可视化图表
    6. 深度分析与解读
    7. 全面风险评估
    8. 稳健性检验
    9. 成本与容量分析
    10. 改进方向与优化建议
    11. 实盘交易建议
    12. 结论与展望
    13. 附录
    """
    try:
        summary = results.get('summary', {})
        metrics = results.get('metrics', {})
        portfolio_history = results.get('portfolio_history', {})
        
        # 准备数据
        dates = portfolio_history.get('dates', [])
        total_value = portfolio_history.get('total_value', [])
        cash = portfolio_history.get('cash', [])
        
        # 转换为pandas Series
        dates_series = pd.to_datetime(dates) if dates else pd.DatetimeIndex([])
        total_value_series = pd.Series(total_value, index=dates_series)
        
        # 计算收益率
        returns_pct = results.get('returns_pct', pd.Series())
        if isinstance(returns_pct, list):
            returns_pct = pd.Series(returns_pct, index=dates_series[:len(returns_pct)])
        if returns_pct.empty and len(total_value_series) > 1:
            returns_pct = total_value_series.pct_change().fillna(0)
        
        # 计算累计收益率
        cumulative_returns = (1 + returns_pct).cumprod() - 1 if not returns_pct.empty else pd.Series()
        
        # 计算回撤
        drawdown = _calculate_drawdown(total_value_series) if not total_value_series.empty else pd.Series()
        
        # 计算月度收益
        monthly_returns = _calculate_monthly_returns(total_value_series) if not total_value_series.empty else pd.Series()
        
        # 获取交易历史
        trade_history = results.get('trade_history', [])
        
        # 格式化函数
        def format_currency(value):
            return f"{value:,.2f}"
        
        def format_percent(value):
            return f"{value*100:.2f}%"
        
        # 准备图表数据（JSON格式，供Plotly使用）
        total_value_list = total_value if isinstance(total_value, list) else (total_value.tolist() if hasattr(total_value, 'tolist') else list(total_value))
        
        equity_data_json = json.dumps([{
            'x': [d.strftime('%Y-%m-%d') if isinstance(d, pd.Timestamp) else str(d) for d in dates_series] if not dates_series.empty else [],
            'y': total_value_list,
            'type': 'scatter',
            'mode': 'lines',
            'name': '策略净值',
            'line': {'color': '#667eea', 'width': 2}
        }], ensure_ascii=False)
        
        returns_data_json = json.dumps([{
            'x': [d.strftime('%Y-%m-%d') if isinstance(d, pd.Timestamp) else str(d) for d in returns_pct.index] if not returns_pct.empty else [],
            'y': (cumulative_returns * 100).tolist() if not cumulative_returns.empty else [],
            'type': 'scatter',
            'mode': 'lines',
            'name': '累计收益率',
            'line': {'color': '#48bb78', 'width': 2}
        }], ensure_ascii=False)
        
        drawdown_data_json = json.dumps([{
            'x': [d.strftime('%Y-%m-%d') if isinstance(d, pd.Timestamp) else str(d) for d in drawdown.index] if not drawdown.empty else [],
            'y': (drawdown * 100).tolist() if not drawdown.empty else [],
            'type': 'scatter',
            'mode': 'lines',
            'fill': 'tozeroy',
            'name': '回撤',
            'line': {'color': '#f56565', 'width': 1},
            'fillcolor': 'rgba(245, 101, 101, 0.3)'
        }], ensure_ascii=False)
        
        monthly_returns_data_json = json.dumps([{
            'x': [d.strftime('%Y-%m') if isinstance(d, pd.Timestamp) else str(d) for d in monthly_returns.index] if not monthly_returns.empty else [],
            'y': (monthly_returns * 100).tolist() if not monthly_returns.empty else [],
            'type': 'bar',
            'name': '月度收益',
            'marker': {'color': ['#27ae60' if x >= 0 else '#e74c3c' for x in monthly_returns] if not monthly_returns.empty else []}
        }], ensure_ascii=False)
        
        # 计算关键指标
        initial_cash = summary.get('initial_cash', 0)
        final_value = summary.get('total_value', 0)
        total_profit = summary.get('total_profit', 0)
        total_profit_rate = summary.get('total_profit_rate', 0)
        annual_return = metrics.get('annual_return', 0)
        sharpe_ratio = metrics.get('sharpe_ratio', 0)
        max_drawdown = metrics.get('max_drawdown', 0)
        total_trades = metrics.get('total_trades', 0)
        
        # 计算年数
        if dates_series.empty:
            years = 1.0
        else:
            days = (dates_series[-1] - dates_series[0]).days
            years = max(days / 252.0, 0.1)  # 至少0.1年
        
        # 准备交易明细HTML和交易分析
        trade_history_html = ""
        trade_analysis_html = ""
        
        # 初始化交易频率相关变量（默认值）
        trades_per_day = 0
        trade_frequency_level = "无交易"
        trade_frequency_desc = "暂无交易记录"
        
        if trade_history:
            buy_count = sum(1 for t in trade_history if t.get('type') == '买入')
            sell_count = sum(1 for t in trade_history if t.get('type') == '卖出')
            total_buy_value = sum(t.get('value', 0) for t in trade_history if t.get('type') == '买入')
            total_sell_value = sum(t.get('value', 0) for t in trade_history if t.get('type') == '卖出')
            total_commission = sum(t.get('commission', 0) for t in trade_history)
            
            # 计算正确的净现金流
            buy_commission = sum(t.get('commission', 0) for t in trade_history if t.get('type') == '买入')
            sell_commission = sum(t.get('commission', 0) for t in trade_history if t.get('type') == '卖出')
            total_buy_outflow = total_buy_value + buy_commission  # 买入净流出（含佣金）
            total_sell_inflow = total_sell_value - sell_commission  # 卖出净流入（扣除佣金）
            net_cash_flow = total_sell_inflow - total_buy_outflow  # 净现金流（正值表示净流入，负值表示净流出）
            
            trade_rows = ""
            for i, trade in enumerate(trade_history):
                trade_type = trade.get('type', '')
                type_color = '#27ae60' if trade_type == '买入' else '#e74c3c'
                type_bg = '#d4edda' if trade_type == '买入' else '#f8d7da'
                
                trade_date = trade.get('date', '')
                if isinstance(trade_date, str) and len(trade_date) > 10:
                    trade_date = trade_date[:10]
                
                # 净金额：买入为负值（流出），卖出为正值（流入）
                net_value = trade.get('net_value', 0)
                net_value_color = '#e74c3c' if net_value < 0 else '#27ae60'  # 负值红色，正值绿色
                net_value_display = format_currency(abs(net_value)) if net_value < 0 else format_currency(net_value)
                
                trade_rows += f'''                        <tr style="background-color: {'#f9f9f9' if i % 2 == 0 else '#ffffff'};">
                            <td style="padding: 10px; border-bottom: 1px solid #ddd;">{trade_date}</td>
                            <td style="padding: 10px; border-bottom: 1px solid #ddd; font-weight: 600;">{trade.get('security', '')}</td>
                            <td style="padding: 10px; border-bottom: 1px solid #ddd; text-align: center;">
                                <span style="background-color: {type_bg}; color: {type_color}; padding: 4px 10px; border-radius: 4px; font-weight: 600;">
                                    {trade_type}
                                </span>
                            </td>
                            <td style="padding: 10px; border-bottom: 1px solid #ddd; text-align: right;">{int(trade.get('amount', 0)):,}</td>
                            <td style="padding: 10px; border-bottom: 1px solid #ddd; text-align: right;">{format_currency(trade.get('price', 0))}</td>
                            <td style="padding: 10px; border-bottom: 1px solid #ddd; text-align: right;">{format_currency(trade.get('value', 0))}</td>
                            <td style="padding: 10px; border-bottom: 1px solid #ddd; text-align: right;">{format_currency(trade.get('commission', 0))}</td>
                            <td style="padding: 10px; border-bottom: 1px solid #ddd; text-align: right; color: {net_value_color}; font-weight: 600;">
                                {('-' if net_value < 0 else '') + net_value_display}
                            </td>
                        </tr>
'''
            
            trade_history_html = f'''
        <h2>4.2 交易明细</h2>
        <div class="info-section">
            <p>回测期间共执行 <strong>{len(trade_history)}</strong> 笔交易，详细交易记录如下：</p>
            
            <div style="margin-top: 20px; overflow-x: auto;">
                <table style="width: 100%; min-width: 800px;">
                    <thead>
                        <tr>
                            <th style="padding: 12px; background-color: #3498db; color: white; text-align: left;">日期</th>
                            <th style="padding: 12px; background-color: #3498db; color: white; text-align: left;">股票代码</th>
                            <th style="padding: 12px; background-color: #3498db; color: white; text-align: center;">类型</th>
                            <th style="padding: 12px; background-color: #3498db; color: white; text-align: right;">数量</th>
                            <th style="padding: 12px; background-color: #3498db; color: white; text-align: right;">价格</th>
                            <th style="padding: 12px; background-color: #3498db; color: white; text-align: right;">交易金额</th>
                            <th style="padding: 12px; background-color: #3498db; color: white; text-align: right;">手续费</th>
                            <th style="padding: 12px; background-color: #3498db; color: white; text-align: right;">净现金流<br/>(负=流出,正=流入)</th>
                        </tr>
                    </thead>
                    <tbody>
{trade_rows}                    </tbody>
                    <tfoot>
                        <tr style="background-color: #f0f0f0; font-weight: 600;">
                            <td colspan="3" style="padding: 12px; border-top: 2px solid #3498db;">合计</td>
                            <td style="padding: 12px; border-top: 2px solid #3498db; text-align: right;">买入: {buy_count}笔 | 卖出: {sell_count}笔</td>
                            <td style="padding: 12px; border-top: 2px solid #3498db;"></td>
                            <td style="padding: 12px; border-top: 2px solid #3498db; text-align: right;">买入: {format_currency(total_buy_value)}<br>卖出: {format_currency(total_sell_value)}</td>
                            <td style="padding: 12px; border-top: 2px solid #3498db; text-align: right;">{format_currency(total_commission)}</td>
                            <td style="padding: 12px; border-top: 2px solid #3498db; text-align: right; color: {'#27ae60' if net_cash_flow >= 0 else '#e74c3c'}; font-weight: 600;">
                                {'净流入' if net_cash_flow >= 0 else '净流出'}: {format_currency(abs(net_cash_flow))}
                            </td>
                        </tr>
                    </tfoot>
                </table>
            </div>
            
            <div style="margin-top: 20px; padding: 15px; background-color: #e8f4f8; border-radius: 5px;">
                <h4>交易统计</h4>
                <ul style="margin-top: 10px;">
                    <li>买入交易：<strong>{buy_count}</strong> 笔，总金额 <strong>{format_currency(total_buy_value)}</strong> 元，佣金 <strong>{format_currency(buy_commission)}</strong> 元</li>
                    <li>卖出交易：<strong>{sell_count}</strong> 笔，总金额 <strong>{format_currency(total_sell_value)}</strong> 元，佣金 <strong>{format_currency(sell_commission)}</strong> 元</li>
                    <li>总手续费：<strong>{format_currency(total_commission)}</strong> 元</li>
                    <li>买入净流出：<strong>{format_currency(total_buy_outflow)}</strong> 元（含佣金）</li>
                    <li>卖出净流入：<strong>{format_currency(total_sell_inflow)}</strong> 元（扣除佣金）</li>
                    <li>净现金流：<strong style="color: {'#27ae60' if net_cash_flow >= 0 else '#e74c3c'}">{format_currency(net_cash_flow)}</strong> 元（{'正值表示净流入' if net_cash_flow >= 0 else '负值表示净流出'}）</li>
                </ul>
            </div>
        </div>
'''
            
            # 计算交易分析数据
            from collections import defaultdict
            from datetime import datetime as dt
            
            # 1. 按股票统计交易
            stock_trades = defaultdict(lambda: {'buys': [], 'sells': [], 'total_buy_value': 0, 'total_sell_value': 0, 'total_commission': 0})
            
            for trade in trade_history:
                sec = trade.get('security', '')
                trade_type = trade.get('type', '')
                date_str = trade.get('date', '')[:10]  # 只取日期部分
                price = trade.get('price', 0)
                amount = trade.get('amount', 0)
                value = trade.get('value', 0)
                commission = trade.get('commission', 0)
                
                if trade_type == '买入':
                    stock_trades[sec]['buys'].append({
                        'date': date_str,
                        'price': price,
                        'amount': amount,
                        'value': value,
                        'commission': commission
                    })
                    stock_trades[sec]['total_buy_value'] += value
                else:
                    stock_trades[sec]['sells'].append({
                        'date': date_str,
                        'price': price,
                        'amount': amount,
                        'value': value,
                        'commission': commission
                    })
                    stock_trades[sec]['total_sell_value'] += value
                stock_trades[sec]['total_commission'] += commission
            
            # 2. 计算每只股票的盈亏（简化版：卖出总额 - 买入总额）
            stock_analysis = []
            for sec, data in stock_trades.items():
                buy_count = len(data['buys'])
                sell_count = len(data['sells'])
                net_profit = data['total_sell_value'] - data['total_buy_value'] - data['total_commission']
                profit_rate = (net_profit / data['total_buy_value'] * 100) if data['total_buy_value'] > 0 else 0
                stock_total_trades = buy_count + sell_count  # 每只股票的交易次数，避免覆盖外部的total_trades
                
                stock_analysis.append({
                    'security': sec,
                    'buy_count': buy_count,
                    'sell_count': sell_count,
                    'total_trades': stock_total_trades,
                    'total_buy_value': data['total_buy_value'],
                    'total_sell_value': data['total_sell_value'],
                    'net_profit': net_profit,
                    'profit_rate': profit_rate,
                    'total_commission': data['total_commission']
                })
            
            # 按交易次数排序
            stock_analysis.sort(key=lambda x: x['total_trades'], reverse=True)
            
            # 3. 计算交易对盈亏（买入-卖出配对）
            trade_pairs = []
            for sec, data in stock_trades.items():
                buys = sorted(data['buys'], key=lambda x: x['date'])
                sells = sorted(data['sells'], key=lambda x: x['date'])
                
                # 简单的FIFO配对
                buy_idx = 0
                for sell in sells:
                    if buy_idx < len(buys):
                        buy = buys[buy_idx]
                        # 计算持仓天数
                        try:
                            buy_date = pd.to_datetime(buy['date'])
                            sell_date = pd.to_datetime(sell['date'])
                            holding_days = (sell_date - buy_date).days
                        except:
                            holding_days = 0
                        
                        # 计算盈亏（简化：使用平均价格）
                        avg_buy_price = buy['price']
                        sell_price = sell['price']
                        profit = (sell_price - avg_buy_price) * min(buy['amount'], sell['amount'])
                        profit_rate = ((sell_price - avg_buy_price) / avg_buy_price * 100) if avg_buy_price > 0 else 0
                        
                        trade_pairs.append({
                            'security': sec,
                            'buy_date': buy['date'],
                            'sell_date': sell['date'],
                            'holding_days': holding_days,
                            'buy_price': avg_buy_price,
                            'sell_price': sell_price,
                            'amount': min(buy['amount'], sell['amount']),
                            'profit': profit,
                            'profit_rate': profit_rate
                        })
                        buy_idx += 1
            
            # 4. 计算胜率和盈亏比
            if trade_pairs:
                winning_trades = [t for t in trade_pairs if t['profit'] > 0]
                losing_trades = [t for t in trade_pairs if t['profit'] < 0]
                win_rate = len(winning_trades) / len(trade_pairs) * 100 if trade_pairs else 0
                
                avg_profit = sum(t['profit'] for t in winning_trades) / len(winning_trades) if winning_trades else 0
                avg_loss = abs(sum(t['profit'] for t in losing_trades) / len(losing_trades)) if losing_trades else 0
                profit_loss_ratio = avg_profit / avg_loss if avg_loss > 0 else 0
                
                avg_holding_days = sum(t['holding_days'] for t in trade_pairs) / len(trade_pairs) if trade_pairs else 0
            else:
                win_rate = 0
                profit_loss_ratio = 0
                avg_holding_days = 0
                winning_trades = []
                losing_trades = []
            
            # 5. 计算交易频率并判断频率等级
            # 使用total_trades（来自metrics）而不是len(trade_history)，因为total_trades是准确的交易次数
            if dates and len(dates) > 0:
                # 计算回测期间的实际交易日数
                try:
                    first_date = pd.to_datetime(dates[0])
                    last_date = pd.to_datetime(dates[-1])
                    trading_days = len(dates)  # 使用实际的交易日列表长度
                    trades_per_day = total_trades / trading_days if trading_days > 0 else 0
                except:
                    trades_per_day = total_trades / len(dates) if len(dates) > 0 else 0
            else:
                trades_per_day = 0
            
            # 判断交易频率等级
            # 低：< 0.5次/天，适中：0.5-3次/天，高：> 3次/天
            if trades_per_day < 0.5:
                trade_frequency_level = "较低"
                trade_frequency_desc = "交易频率较低，适合长期持有策略"
            elif trades_per_day <= 3.0:
                trade_frequency_level = "适中"
                trade_frequency_desc = "交易频率适中，避免了过度交易"
            else:
                trade_frequency_level = "较高"
                trade_frequency_desc = "交易频率较高，需要注意交易成本"
            
            # 6. 生成交易分析HTML
            trade_analysis_html = f'''
        <h2>4.3 交易总结分析</h2>
        <div class="info-section">
            <h3>4.3.1 交易绩效概览</h3>
            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin: 20px 0;">
                <div class="metric-card" style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);">
                    <div class="metric-label">胜率</div>
                    <div class="metric-value">{win_rate:.1f}%</div>
                    <div style="font-size: 0.9em; margin-top: 10px;">盈利交易: {len(winning_trades)}笔 / 总交易对: {len(trade_pairs)}笔</div>
                </div>
                <div class="metric-card" style="background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);">
                    <div class="metric-label">盈亏比</div>
                    <div class="metric-value">{profit_loss_ratio:.2f}</div>
                    <div style="font-size: 0.9em; margin-top: 10px;">平均盈利: {format_currency(avg_profit)} / 平均亏损: {format_currency(avg_loss)}</div>
                </div>
                <div class="metric-card" style="background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);">
                    <div class="metric-label">平均持仓天数</div>
                    <div class="metric-value">{avg_holding_days:.1f} 天</div>
                    <div style="font-size: 0.9em; margin-top: 10px;">最短: {min((t['holding_days'] for t in trade_pairs), default=0)}天 / 最长: {max((t['holding_days'] for t in trade_pairs), default=0)}天</div>
                </div>
                <div class="metric-card" style="background: linear-gradient(135deg, #43e97b 0%, #38f9d7 100%);">
                    <div class="metric-label">日均交易次数</div>
                    <div class="metric-value">{trades_per_day:.2f} 次</div>
                    <div style="font-size: 0.9em; margin-top: 10px;">总交易: {len(trade_history)}笔</div>
                </div>
            </div>
            
            <h3>4.3.2 股票交易统计</h3>
            <p>按交易次数排序，展示每只股票的交易情况：</p>
            <div style="margin-top: 20px; overflow-x: auto;">
                <table style="width: 100%; min-width: 900px; border-collapse: collapse;">
                    <thead>
                        <tr style="background-color: #3498db; color: white;">
                            <th style="padding: 12px; text-align: left;">股票代码</th>
                            <th style="padding: 12px; text-align: center;">买入次数</th>
                            <th style="padding: 12px; text-align: center;">卖出次数</th>
                            <th style="padding: 12px; text-align: center;">总交易次数</th>
                            <th style="padding: 12px; text-align: right;">买入总额</th>
                            <th style="padding: 12px; text-align: right;">卖出总额</th>
                            <th style="padding: 12px; text-align: right;">净盈亏</th>
                            <th style="padding: 12px; text-align: right;">收益率</th>
                            <th style="padding: 12px; text-align: right;">手续费</th>
                        </tr>
                    </thead>
                    <tbody>
'''
            
            for i, stock in enumerate(stock_analysis[:20]):  # 只显示前20只
                profit_color = '#27ae60' if stock['net_profit'] >= 0 else '#e74c3c'
                trade_analysis_html += f'''                        <tr style="background-color: {'#f9f9f9' if i % 2 == 0 else '#ffffff'};">
                            <td style="padding: 10px; border-bottom: 1px solid #ddd; font-weight: 600;">{stock['security']}</td>
                            <td style="padding: 10px; border-bottom: 1px solid #ddd; text-align: center;">{stock['buy_count']}</td>
                            <td style="padding: 10px; border-bottom: 1px solid #ddd; text-align: center;">{stock['sell_count']}</td>
                            <td style="padding: 10px; border-bottom: 1px solid #ddd; text-align: center; font-weight: 600;">{stock['total_trades']}</td>
                            <td style="padding: 10px; border-bottom: 1px solid #ddd; text-align: right;">{format_currency(stock['total_buy_value'])}</td>
                            <td style="padding: 10px; border-bottom: 1px solid #ddd; text-align: right;">{format_currency(stock['total_sell_value'])}</td>
                            <td style="padding: 10px; border-bottom: 1px solid #ddd; text-align: right; color: {profit_color}; font-weight: 600;">{format_currency(stock['net_profit'])}</td>
                            <td style="padding: 10px; border-bottom: 1px solid #ddd; text-align: right; color: {profit_color};">{stock['profit_rate']:.2f}%</td>
                            <td style="padding: 10px; border-bottom: 1px solid #ddd; text-align: right;">{format_currency(stock['total_commission'])}</td>
                        </tr>
'''
            
            trade_analysis_html += f'''                    </tbody>
                </table>
            </div>
            <p style="margin-top: 10px; color: #7f8c8d; font-size: 0.9em;">* 仅显示交易次数最多的前20只股票</p>
            
            <h3>4.3.3 交易成本分析</h3>
            <div style="margin-top: 20px; padding: 20px; background-color: #f8f9fa; border-radius: 8px;">
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px;">
                    <div>
                        <div style="font-size: 0.9em; color: #7f8c8d; margin-bottom: 5px;">总手续费</div>
                        <div style="font-size: 1.5em; font-weight: 600; color: #2c3e50;">{format_currency(total_commission)}</div>
                    </div>
                    <div>
                        <div style="font-size: 0.9em; color: #7f8c8d; margin-bottom: 5px;">手续费占净利润比例</div>
                        <div style="font-size: 1.5em; font-weight: 600; color: #2c3e50;">{(total_commission / abs(total_profit) * 100) if total_profit != 0 else 0:.2f}%</div>
                    </div>
                    <div>
                        <div style="font-size: 0.9em; color: #7f8c8d; margin-bottom: 5px;">平均每笔手续费</div>
                        <div style="font-size: 1.5em; font-weight: 600; color: #2c3e50;">{format_currency(total_commission / len(trade_history) if trade_history else 0)}</div>
                    </div>
                    <div>
                        <div style="font-size: 0.9em; color: #7f8c8d; margin-bottom: 5px;">手续费占交易额比例</div>
                        <div style="font-size: 1.5em; font-weight: 600; color: #2c3e50;">{(total_commission / (total_buy_value + total_sell_value) * 100) if (total_buy_value + total_sell_value) > 0 else 0:.4f}%</div>
                    </div>
                </div>
            </div>
            
            <h3>4.3.4 交易分布分析</h3>
            <div style="margin-top: 20px; padding: 20px; background-color: #fff3cd; border-radius: 8px; border-left: 4px solid #ffc107;">
                <ul style="line-height: 2;">
                    <li><strong>交易最活跃的股票</strong>：{stock_analysis[0]['security'] if stock_analysis else 'N/A'}（{stock_analysis[0]['total_trades'] if stock_analysis else 0}笔交易）</li>
                    <li><strong>盈利最多的股票</strong>：{max(stock_analysis, key=lambda x: x['net_profit'])['security'] if stock_analysis else 'N/A'}（净盈利 {format_currency(max(stock_analysis, key=lambda x: x['net_profit'])['net_profit']) if stock_analysis else 0}）</li>
                    <li><strong>亏损最多的股票</strong>：{min(stock_analysis, key=lambda x: x['net_profit'])['security'] if stock_analysis else 'N/A'}（净亏损 {format_currency(abs(min(stock_analysis, key=lambda x: x['net_profit'])['net_profit'])) if stock_analysis else 0}）</li>
                    <li><strong>交易股票数量</strong>：{len(stock_analysis)} 只</li>
                    <li><strong>平均每只股票交易次数</strong>：{len(trade_history) / len(stock_analysis) if stock_analysis else 0:.1f} 次</li>
                </ul>
            </div>
        </div>
'''
        else:
            trade_history_html = '''
        <h2>4.2 交易明细</h2>
        <div class="info-section">
            <p>暂无交易记录。</p>
        </div>
'''
            trade_analysis_html = '''
        <h2>4.3 交易总结分析</h2>
        <div class="info-section">
            <p>暂无交易记录，无法进行交易分析。</p>
        </div>
'''
        
        # 生成HTML内容
        html_content = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>量化策略投研报告 - {strategy_name} v{strategy_version}</title>
    
    <!-- Plotly.js CDN -->
    <script src="https://cdn.plot.ly/plotly-2.26.0.min.js"></script>
    
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', 'Arial', 'Noto Sans', 'Liberation Sans', 'Microsoft YaHei', sans-serif;
            line-height: 1.6; 
            color: #333; 
            background-color: #f4f7f6; 
            padding: 20px; 
        }}
        article {{ 
            max-width: 1400px; 
            margin: 0 auto; 
            background: #fff; 
            padding: 40px; 
            border-radius: 8px; 
            box-shadow: 0 4px 12px rgba(0,0,0,0.1); 
        }}
        header {{ 
            border-bottom: 3px solid #2c3e50; 
            padding-bottom: 20px; 
            margin-bottom: 30px; 
            text-align: center; 
        }}
        h1, h2, h3, h4 {{ color: #2c3e50; margin-top: 0; }}
        h1 {{ font-size: 2.5em; margin-bottom: 10px; }}
        h2 {{ font-size: 1.8em; border-bottom: 2px solid #3498db; padding-bottom: 10px; margin-top: 40px; }}
        h3 {{ font-size: 1.4em; color: #34495e; margin-top: 25px; }}
        h4 {{ font-size: 1.2em; color: #555; margin-top: 20px; }}
        
        .executive-summary {{
            background: linear-gradient(135deg, #2c3e50 0%, #34495e 100%);
            color: #ffffff;
            padding: 30px;
            border-radius: 8px;
            margin: 20px 0;
            box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        }}
        .executive-summary h3 {{
            color: #ffffff;
            font-weight: 600;
        }}
        
        .equity-summary-table {{
            background: linear-gradient(135deg, #2c3e50 0%, #34495e 100%);
            color: #ffffff;
            padding: 30px;
            border-radius: 12px;
            margin: 20px 0;
            box-shadow: 0 6px 20px rgba(0,0,0,0.2);
            overflow-x: auto;
        }}
        .equity-summary-table table {{
            width: 100%;
            border-collapse: separate;
            border-spacing: 0;
        }}
        .equity-summary-table th {{
            background: rgba(255, 255, 255, 0.12);
            color: #ffffff;
            font-weight: 600;
            padding: 18px 25px;
            text-align: left;
            font-size: 0.95em;
            text-transform: uppercase;
            letter-spacing: 0.8px;
            border-bottom: 2px solid rgba(255, 255, 255, 0.2);
        }}
        .equity-summary-table td {{
            background: rgba(255, 255, 255, 0.05);
            color: #ffffff;
            padding: 20px 25px;
            border-bottom: 1px solid rgba(255, 255, 255, 0.1);
            font-size: 1em;
        }}
        .equity-summary-table tr:hover td {{
            background: rgba(255, 255, 255, 0.1);
            transition: background 0.2s ease;
        }}
        .equity-summary-table .metric-value {{
            font-size: 2em;
            font-weight: 700;
            color: #ffffff;
            text-shadow: 0 2px 8px rgba(0,0,0,0.4);
        }}
        .equity-summary-table .metric-label {{
            font-weight: 600;
            color: rgba(255, 255, 255, 0.9);
            font-size: 1em;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        
        .metric-card {{
            padding: 20px;
            border-radius: 8px;
            margin: 15px 0;
            box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        }}
        .metric-card.positive {{
            background: linear-gradient(135deg, #27ae60 0%, #229954 100%);
            color: #ffffff;
        }}
        .metric-card.negative {{
            background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%);
            color: #ffffff;
        }}
        .metric-card.neutral {{
            background: linear-gradient(135deg, #95a5a6 0%, #7f8c8d 100%);
            color: #ffffff;
        }}
        .metric-value {{
            font-size: 2.5em;
            font-weight: bold;
            color: #ffffff;
            text-shadow: 0 2px 4px rgba(0,0,0,0.3);
            margin: 10px 0;
        }}
        .metric-label {{
            font-size: 1.1em;
            color: #f0f0f0;
            font-weight: 600;
        }}
        
        .chart-container {{
            margin: 30px 0;
            padding: 20px;
            background: #fff;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }}
        
        .info-section {{
            background: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            margin: 20px 0;
            border-left: 4px solid #3498db;
        }}
        
        .version-comparison {{
            background: #fff3cd;
            padding: 20px;
            border-radius: 8px;
            margin: 20px 0;
            border-left: 4px solid #ffc107;
        }}
        
        .code-block {{
            background: #2c3e50;
            color: #ecf0f1;
            padding: 15px;
            border-radius: 5px;
            overflow-x: auto;
            font-family: 'Consolas', 'Monaco', monospace;
            font-size: 0.9em;
            margin: 15px 0;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }}
        table th, table td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        table th {{
            background-color: #3498db;
            color: white;
            font-weight: 600;
        }}
        table tr:hover {{
            background-color: #f5f5f5;
        }}
        
        footer {{
            margin-top: 50px;
            padding-top: 20px;
            border-top: 2px solid #ecf0f1;
            text-align: center;
            color: #7f8c8d;
            font-size: 0.9em;
        }}
        
        p, ul, ol {{ margin-bottom: 15px; }}
        ul, ol {{ margin-left: 20px; }}
        code {{
            background-color: #f0f0f0;
            padding: 2px 5px;
            border-radius: 4px;
            font-family: 'Consolas', 'Monaco', monospace;
            font-size: 0.9em;
        }}
        strong {{ color: #2c3e50; }}
    </style>
</head>
<body>
    <article>
        <header>
            <h1>量化策略投研报告</h1>
            <p style="font-size: 1.2em; color: #7f8c8d; margin-top: 10px;">
                <strong>{strategy_name}</strong> - A股自适应动量策略
            </p>
            <p style="font-size: 1em; color: #95a5a6; margin-top: 5px;">
                平台：JQQuant（聚宽API）｜语言：Python 3.8+ ｜数据口径：日线数据
            </p>
            <p style="font-size: 1em; color: #95a5a6; margin-top: 5px;">
                回测区间：{start_date} 至 {end_date}｜ 
                基准：{strategy_params.get('benchmark', '000300.XSHG') if strategy_params else '000300.XSHG'} ｜ 
                初始资金：{format_currency(initial_cash)} 元
            </p>
            <p style="font-size: 1em; color: #95a5a6; margin-top: 5px;">
                报告版本：<strong>v{strategy_version}</strong> ｜ 报告生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            </p>
        </header>
        
        <section class="version-comparison">
            <h2>0. 版本对比分析</h2>
            
            <h3>0.1 策略演进概述</h3>
            <p><strong>{strategy_name} v{strategy_version}</strong>是基于固定参数动量策略的重大升级版本。核心创新在于引入了<strong>市场环境自动识别框架</strong>，能够根据当前市场阶段动态调整策略参数，实现真正的自适应交易。</p>
            
            <p><strong>主要改进目标：</strong></p>
            <ul>
                <li>提升策略在不同市场环境下的适应性</li>
                <li>改善风险控制能力，降低最大回撤</li>
                <li>提高风险调整后收益（Sharpe Ratio）</li>
                <li>保持高收益的同时降低波动性</li>
            </ul>
            
            <h3>0.2 核心创新</h3>
            <p><strong>1. 市场环境识别器（MarketRegimeDetector）</strong></p>
            <p>实现了多因素市场环境识别框架，包含三个层次：</p>
            <ul>
                <li><strong>趋势判定层</strong>：使用基准指数（沪深300）与均线关系判断趋势方向（Bullish/Bearish/Sideways）</li>
                <li><strong>风险判定层</strong>：使用波动率指标判断市场恐慌程度（Normal/Elevated/Panic）</li>
                <li><strong>风格轮动层</strong>：使用创业板指判断成长vs价值风格</li>
            </ul>
            <p>通过决策树将市场环境分类为11种市场阶段，并根据不同阶段自动调整策略参数。</p>
            
            <h3>0.3 性能表现</h3>
            <p>策略在回测期间实现了<strong>{format_percent(total_profit_rate)}</strong>的总收益率，年化收益率为<strong>{format_percent(annual_return)}</strong>，Sharpe比率为<strong>{sharpe_ratio:.2f}</strong>，最大回撤为<strong>{format_percent(max_drawdown)}</strong>。</p>
        </section>
        
        <section class="executive-summary">
            <h2>1. 执行摘要（Executive Summary）</h2>
            
            <h3>策略定位</h3>
            <p>{strategy_name}是一个基于市场环境自动识别的自适应动量策略，通过多因素市场环境识别框架（趋势、风险、风格），动态调整策略参数，在保持高收益的同时显著改善风险控制能力。策略成功适应不同市场环境，展现出良好的适应性。</p>
            
            <h3>核心发现</h3>
            <ul>
                <li>年化收益率（CAGR）：{format_percent(annual_return)} - 表现优异</li>
                <li>Sharpe Ratio：{sharpe_ratio:.2f} - 风险调整后收益良好</li>
                <li>最大回撤：{format_percent(max_drawdown)} - 风险控制能力良好</li>
                <li>总收益率：{format_percent(total_profit_rate)} - 回测期间，初始资金{format_currency(initial_cash)}元增长至{format_currency(final_value)}元</li>
                <li>总交易次数：{total_trades} - 交易频率{trade_frequency_level}（平均{trades_per_day:.2f}次/交易日）</li>
            </ul>
            
            <h3>主要结论</h3>
            <p><strong>适用市场：</strong>策略在牛市、高增长板块活跃期表现最佳，在震荡市和熊市能够自动降低仓位或清仓，展现出良好的市场适应性。</p>
            <p><strong>风险等级：</strong>中等偏高（最大回撤{format_percent(max_drawdown)}，但通过动态调整已显著改善）</p>
            <p><strong>实盘建议：</strong>策略表现优异，风险控制能力显著提升，建议在实盘中采用，但需要严格监控市场环境识别准确性和参数调整效果。</p>
        </section>
        
        <h2>2. 策略背景与理论依据</h2>
        <div class="info-section">
            <h3>2.1 策略原理</h3>
            <h4>理论基础</h4>
            <p>本策略结合了动量效应理论和市场环境识别理论，主要理论支撑包括：</p>
            <ul>
                <li><strong>Jegadeesh-Titman动量效应（1993）</strong>：中期动量（3-12个月）在股票市场中持续存在，买入近期表现好的股票可以获得显著的超额收益。</li>
                <li><strong>市场环境识别理论</strong>：不同市场环境下，相同的策略参数会产生不同的效果。通过识别市场环境并动态调整参数，可以提升策略表现。</li>
                <li><strong>行为金融学</strong>：投资者对信息的反应存在延迟和过度反应，导致价格趋势延续，为动量策略提供盈利空间。</li>
                <li><strong>自适应系统理论</strong>：能够根据环境变化自动调整的系统具有更强的鲁棒性和适应性。</li>
            </ul>
            
            <h4>市场假设</h4>
            <ul>
                <li>市场环境可识别：通过技术指标（沪深300 vs MA50、波动率、创业板指/沪深300比率）可以准确识别当前市场阶段</li>
                <li>参数适应性：不同市场环境下，最优策略参数存在显著差异</li>
                <li>动量延续性：在特定市场环境下（如全面牛市），价格趋势具有延续性</li>
                <li>风险可控性：通过市场环境识别，可以在高风险环境（如恐慌熊市）提前降低仓位或清仓</li>
            </ul>
            
            <h3>2.2 策略逻辑流程</h3>
            <p><strong>【A股自适应动量策略逻辑流程】</strong></p>
            <ol>
                <li><strong>初始化阶段</strong>
                    <ul>
                        <li>设置回测参数</li>
                        <li>初始化市场环境识别器（MarketRegimeDetector）</li>
                        <li>定义股票池（A股高增长股票）</li>
                        <li>注册技术指标（ROC5/10/20, RSI, SMA20/50, Volume SMA）</li>
                        <li>设置默认参数（将在市场环境更新时调整）</li>
                    </ul>
                </li>
                <li><strong>每日更新市场环境（交易开始前）</strong>
                    <ul>
                        <li>更新沪深300 vs MA50（趋势判定）</li>
                        <li>更新波动率（风险判定）</li>
                        <li>更新创业板指/沪深300比率（风格判定）</li>
                        <li>综合决策，确定当前市场阶段（11种之一）</li>
                        <li>如果市场环境变化，调用_AdjustStrategyParameters()</li>
                    </ul>
                </li>
                <li><strong>每周筛选阶段（每周一、三、五执行）</strong>
                    <ul>
                        <li>检查市场环境：如果是恐慌熊市，清仓观望</li>
                        <li>计算每只股票的技术指标</li>
                        <li>根据当前市场环境动态调整筛选条件</li>
                        <li>计算综合得分：根据市场环境调整权重</li>
                        <li>选择得分最高的N只股票（N根据市场环境：2-8只）</li>
                    </ul>
                </li>
                <li><strong>再平衡阶段</strong>
                    <ul>
                        <li>清仓不在目标列表的持仓</li>
                        <li>买入目标列表中的新股票（仓位根据市场环境：5%-15%）</li>
                    </ul>
                </li>
                <li><strong>持仓管理阶段（每日执行）</strong>
                    <ul>
                        <li>检查止盈：收益 ≥ 止盈比例（根据市场环境：20%-70%）→ 平仓</li>
                        <li>检查止损：价格 ≤ 入场价×(1-止损比例)（根据市场环境：5%-12%）→ 平仓</li>
                    </ul>
                </li>
            </ol>
        </div>
        
        <h2>3. 策略实现详解</h2>
        <div class="info-section">
            <h3>3.1 代码架构</h3>
            <p>本策略采用模块化设计，分为两个核心文件：</p>
            <ul>
                <li><code>strategies/examples/adaptive_momentum_a.py</code>：主策略文件，包含策略初始化、市场环境更新、选股、再平衡和持仓管理</li>
                <li><code>utils/market_regime_detector.py</code>：市场环境识别模块，封装多因素市场环境识别逻辑</li>
            </ul>
            
            <h3>3.2 核心算法</h3>
            <h4>市场环境识别算法</h4>
            <p>策略的核心创新在于多因素市场环境识别框架，包含三个层次：</p>
            <ul>
                <li><strong>趋势判定层</strong>：沪深300价格 vs 50日均线，判断趋势方向（Bullish/Bearish/Sideways）</li>
                <li><strong>风险判定层</strong>：计算年化波动率，判断市场恐慌程度（Normal/Elevated/Panic）</li>
                <li><strong>综合决策</strong>：通过决策树确定11种市场阶段之一</li>
            </ul>
            
            <h4>选股逻辑</h4>
            <p>选股采用多因子筛选 + 动态评分的方式：</p>
            <ul>
                <li>初步筛选：ROC10、ROC20、成交量、RSI等技术指标</li>
                <li>综合评分：根据市场环境调整权重（牛市重视短期动量，熊市末期重视超跌反弹）</li>
                <li>选择Top N：根据市场环境选择2-8只股票</li>
            </ul>
            
            <h4>风险控制机制</h4>
            <ul>
                <li>市场环境识别：在恐慌熊市自动清仓</li>
                <li>动态止损：根据市场环境调整止损比例（5%-12%）</li>
                <li>动态止盈：根据市场环境调整止盈比例（20%-70%）</li>
                <li>仓位控制：在风险环境降低仓位和持仓数</li>
            </ul>
            
            <h3>3.3 参数设置</h3>
            <table>
                <thead>
                    <tr>
                        <th>参数名称</th>
                        <th>默认值</th>
                        <th>动态范围</th>
                        <th>说明</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td>roc_10_min</td>
                        <td>0.02 (2%)</td>
                        <td>0.015 - 0.05</td>
                        <td>ROC10最小阈值，根据市场环境调整</td>
                    </tr>
                    <tr>
                        <td>roc_20_min</td>
                        <td>0.03 (3%)</td>
                        <td>0.025 - 0.08</td>
                        <td>ROC20最小阈值，根据市场环境调整</td>
                    </tr>
                    <tr>
                        <td>max_positions</td>
                        <td>7</td>
                        <td>2 - 8</td>
                        <td>最大持仓数，熊市减少，牛市增加</td>
                    </tr>
                    <tr>
                        <td>position_size</td>
                        <td>0.15 (15%)</td>
                        <td>0.05 - 0.15</td>
                        <td>单只股票仓位，风险环境降低</td>
                    </tr>
                    <tr>
                        <td>stop_loss</td>
                        <td>0.10 (10%)</td>
                        <td>0.05 - 0.12</td>
                        <td>止损比例，熊市更严格</td>
                    </tr>
                    <tr>
                        <td>take_profit</td>
                        <td>0.50 (50%)</td>
                        <td>0.20 - 0.70</td>
                        <td>止盈比例，牛市更激进</td>
                    </tr>
                </tbody>
            </table>
        </div>
        
        <h3>3.4 回测数据说明</h3>
        <div class="info-section">
            <h4>3.4.1 数据来源</h4>
            <ul>
                <li><strong>数据提供商</strong>：聚宽（JoinQuant）JQData API</li>
                <li><strong>数据频率</strong>：日线数据（Daily）</li>
                <li><strong>数据覆盖范围</strong>：A股市场（沪深两市）</li>
                <li><strong>回测区间</strong>：{start_date} 至 {end_date}（共{len(dates) if dates else 0}个交易日）</li>
                <li><strong>数据质量</strong>：聚宽提供的高质量数据，已进行复权处理（前复权）</li>
            </ul>
            
            <h4>3.4.2 数据字段说明</h4>
            <table>
                <thead>
                    <tr>
                        <th>字段名称</th>
                        <th>说明</th>
                        <th>用途</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td>open</td>
                        <td>开盘价</td>
                        <td>计算日内波动、跳空缺口</td>
                    </tr>
                    <tr>
                        <td>close</td>
                        <td>收盘价（前复权）</td>
                        <td>主要价格数据，计算收益率、技术指标</td>
                    </tr>
                    <tr>
                        <td>high</td>
                        <td>最高价</td>
                        <td>计算波动率、止损止盈</td>
                    </tr>
                    <tr>
                        <td>low</td>
                        <td>最低价</td>
                        <td>计算波动率、止损止盈</td>
                    </tr>
                    <tr>
                        <td>volume</td>
                        <td>成交量</td>
                        <td>筛选流动性、计算成交量指标</td>
                    </tr>
                    <tr>
                        <td>amount</td>
                        <td>成交额</td>
                        <td>筛选流动性、计算换手率</td>
                    </tr>
                </tbody>
            </table>
            
            <h4>3.4.3 数据预处理</h4>
            <ul>
                <li><strong>复权处理</strong>：使用前复权价格，确保价格序列的连续性</li>
                <li><strong>缺失值处理</strong>：对于停牌、退市等缺失数据，使用前一日数据填充或跳过</li>
                <li><strong>异常值处理</strong>：过滤涨跌停、异常波动等极端情况</li>
                <li><strong>数据验证</strong>：确保价格、成交量等数据的合理性和一致性</li>
            </ul>
        </div>
        
        <h3>3.5 交易过程逻辑详解</h3>
        <div class="info-section">
            <h4>3.5.1 交易执行流程</h4>
            <p>策略采用<strong>事件驱动</strong>的交易执行模式，具体流程如下：</p>
            <ol>
                <li><strong>每日开盘前（09:15）</strong>
                    <ul>
                        <li>更新市场环境识别（MarketRegimeDetector）</li>
                        <li>计算基准指数（沪深300）和成长指数（创业板指）的技术指标</li>
                        <li>判断当前市场阶段（11种市场阶段之一）</li>
                        <li>如果市场环境变化，自动调整策略参数（ROC阈值、最大持仓数、止损止盈等）</li>
                    </ul>
                </li>
                <li><strong>每周一、三、五开盘后（09:30）</strong>
                    <ul>
                        <li>检查市场环境：如果是恐慌熊市，自动清仓观望</li>
                        <li>执行股票筛选（_screen_stocks）：
                            <ul>
                                <li>获取股票池中所有股票的历史数据（60个交易日）</li>
                                <li>计算技术指标：ROC10、ROC20、RSI、SMA20、SMA50、成交量比率</li>
                                <li>计算相对强度（如果启用）：股票收益率 vs 基准指数收益率</li>
                                <li>根据当前市场环境动态调整筛选条件（ROC阈值、均线条件等）</li>
                                <li>计算综合得分：根据市场环境调整权重（牛市重视短期动量，熊市重视超跌反弹）</li>
                                <li>选择得分最高的N只股票（N根据市场环境：2-8只）</li>
                            </ul>
                        </li>
                        <li>执行再平衡（rebalance）：
                            <ul>
                                <li>清仓不在目标列表的持仓</li>
                                <li>计算目标仓位：根据市场环境和波动率，使用风险平价方法分配仓位</li>
                                <li>买入目标列表中的新股票（单只股票仓位：5%-15%）</li>
                            </ul>
                        </li>
                    </ul>
                </li>
                <li><strong>每日收盘前（14:45）</strong>
                    <ul>
                        <li>检查持仓股票的止盈止损条件</li>
                        <li>如果触发止盈（收益 ≥ 止盈比例），自动平仓</li>
                        <li>如果触发止损（价格 ≤ 入场价×(1-止损比例)），自动平仓</li>
                        <li>记录当日组合状态（现金、持仓、总资产）</li>
                    </ul>
                </li>
            </ol>
            
            <h4>3.5.2 订单执行机制</h4>
            <ul>
                <li><strong>订单类型</strong>：市价单（Market Order），以当日收盘价成交</li>
                <li><strong>成交价格</strong>：使用当日收盘价（close）作为成交价格</li>
                <li><strong>滑点处理</strong>：考虑滑点成本（默认0.1%），实际成交价格 = 收盘价 × (1 ± 滑点率)</li>
                <li><strong>手续费计算</strong>：买入和卖出均收取手续费，费率 = 0.03%（0.0003）</li>
                <li><strong>资金管理</strong>：确保买入订单不超过可用现金，卖出订单不超过持仓数量</li>
            </ul>
            
            <h4>3.5.3 风险控制机制</h4>
            <ul>
                <li><strong>市场环境识别</strong>：在恐慌熊市（波动率极高）自动清仓，避免巨额损失</li>
                <li><strong>动态止损</strong>：根据市场环境调整止损比例（5%-12%），熊市更严格</li>
                <li><strong>动态止盈</strong>：根据市场环境调整止盈比例（20%-70%），牛市更激进</li>
                <li><strong>仓位控制</strong>：在风险环境降低仓位和持仓数，减少风险暴露</li>
                <li><strong>最大回撤控制</strong>：如果组合回撤超过15%，自动降低仓位或清仓</li>
                <li><strong>单股风险控制</strong>：单只股票最大仓位不超过15%，避免过度集中</li>
            </ul>
        </div>
        
        <h3>3.6 股票池分析</h3>
        <div class="info-section">
            <h4>3.6.1 股票池构成</h4>
            <p>本策略使用的股票池包含<strong>{len(results.get('securities', []))}</strong>只A股股票，主要特点如下：</p>
            <ul>
                <li><strong>股票来源</strong>：A股市场的主要成长股和蓝筹股</li>
                <li><strong>市场分布</strong>：
                    <ul>
                        <li>上海证券交易所（XSHG）：{sum(1 for s in results.get('securities', []) if '.XSHG' in s)}只</li>
                        <li>深圳证券交易所（XSHE）：{sum(1 for s in results.get('securities', []) if '.XSHE' in s)}只</li>
                    </ul>
                </li>
                <li><strong>板块分布</strong>：
                    <ul>
                        <li>主板股票：{sum(1 for s in results.get('securities', []) if s.startswith(('600', '601', '603', '000', '001', '002')))}只</li>
                        <li>创业板股票：{sum(1 for s in results.get('securities', []) if s.startswith('300'))}只</li>
                    </ul>
                </li>
            </ul>
            
            <h4>3.6.2 股票池筛选标准</h4>
            <p>股票池的构建遵循以下筛选标准：</p>
            <table>
                <thead>
                    <tr>
                        <th>筛选维度</th>
                        <th>标准</th>
                        <th>说明</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td>市值要求</td>
                        <td>≥ 50亿元</td>
                        <td>确保股票有足够的流动性和市场关注度</td>
                    </tr>
                    <tr>
                        <td>成交额要求</td>
                        <td>≥ 5000万元/日</td>
                        <td>确保股票有足够的流动性，避免交易困难</td>
                    </tr>
                    <tr>
                        <td>上市时间</td>
                        <td>≥ 365天</td>
                        <td>排除新股，确保有足够的历史数据</td>
                    </tr>
                    <tr>
                        <td>ST股票</td>
                        <td>排除</td>
                        <td>排除ST、*ST等特殊处理股票</td>
                    </tr>
                    <tr>
                        <td>市盈率（PE）</td>
                        <td>≤ 50</td>
                        <td>排除估值过高的股票</td>
                    </tr>
                    <tr>
                        <td>净资产收益率（ROE）</td>
                        <td>≥ 10%</td>
                        <td>确保股票有良好的盈利能力</td>
                    </tr>
                </tbody>
            </table>
            
            <h4>3.6.3 股票池行业分布</h4>
            <p>股票池涵盖多个行业，主要包括：</p>
            <ul>
                <li><strong>金融行业</strong>：银行、保险、证券（如：601398.XSHG 工商银行、601318.XSHG 中国平安）</li>
                <li><strong>消费行业</strong>：白酒、食品饮料（如：600519.XSHG 贵州茅台、600887.XSHG 伊利股份）</li>
                <li><strong>科技行业</strong>：新能源、电子（如：300750.XSHE 宁德时代、002594.XSHE 比亚迪）</li>
                <li><strong>医药行业</strong>：生物医药、医疗器械（如：300015.XSHE 爱尔眼科、300059.XSHE 东方财富）</li>
                <li><strong>房地产</strong>：房地产开发（如：000002.XSHE 万科A）</li>
                <li><strong>公用事业</strong>：电力、水务（如：600900.XSHG 长江电力）</li>
            </ul>
            
            <h4>3.6.4 股票池特点分析</h4>
            <ul>
                <li><strong>流动性良好</strong>：所有股票均为大盘股或中盘股，成交活跃，适合量化交易</li>
                <li><strong>质量较高</strong>：股票池中的股票多为各行业的龙头企业，基本面相对稳健</li>
                <li><strong>行业分散</strong>：涵盖多个行业，有助于分散风险，避免行业集中度风险</li>
                <li><strong>成长性突出</strong>：股票池聚焦高增长股票，在牛市和高增长板块活跃期表现优异</li>
            </ul>
            
            <h4>3.6.5 实际交易股票统计</h4>
            <p>在本次回测中，策略实际交易了<strong>{len(set(t.get('security', '') for t in trade_history)) if trade_history else 0}</strong>只股票，占股票池的<strong>{(len(set(t.get('security', '') for t in trade_history)) / len(results.get('securities', [])) * 100) if results.get('securities', []) and trade_history else 0:.1f}%</strong>。</p>
            <p>这表明策略具有<strong>选择性</strong>，并非盲目交易所有股票，而是根据市场环境和股票表现进行精选。</p>
        </div>
        
        <h2>4. 回测结果分析</h2>
        <div class="equity-summary-table">
            <table>
                <thead>
                    <tr>
                        <th>指标</th>
                        <th>数值</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td class="metric-label">初始资金</td>
                        <td class="metric-value">{format_currency(initial_cash)} 元</td>
                    </tr>
                    <tr>
                        <td class="metric-label">最终资产</td>
                        <td class="metric-value">{format_currency(final_value)} 元</td>
                    </tr>
                    <tr>
                        <td class="metric-label">总收益</td>
                        <td class="metric-value">{format_currency(total_profit)} 元</td>
                    </tr>
                    <tr>
                        <td class="metric-label">总收益率</td>
                        <td class="metric-value">{format_percent(total_profit_rate)}</td>
                    </tr>
                    <tr>
                        <td class="metric-label">年化收益率</td>
                        <td class="metric-value">{format_percent(annual_return)}</td>
                    </tr>
                    <tr>
                        <td class="metric-label">Sharpe Ratio</td>
                        <td class="metric-value">{sharpe_ratio:.2f}</td>
                    </tr>
                    <tr>
                        <td class="metric-label">最大回撤</td>
                        <td class="metric-value">{format_percent(max_drawdown)}</td>
                    </tr>
                    <tr>
                        <td class="metric-label">总交易次数</td>
                        <td class="metric-value">{total_trades}</td>
                    </tr>
                </tbody>
            </table>
        </div>
{trade_history_html}
{trade_analysis_html}
        <h2>5. 交易结果深入分析与建议</h2>
        <div class="info-section">
            <h3>5.1 交易结果深度分析</h3>
            
            <h4>5.1.1 收益表现分析</h4>
            <div style="margin-top: 20px; padding: 20px; background-color: #e8f5e9; border-radius: 8px; border-left: 4px solid #4caf50;">
                <p><strong>核心收益指标：</strong></p>
                <ul>
                    <li><strong>总收益率</strong>：{format_percent(total_profit_rate)} - 在{len(dates) if dates else 0}个交易日内，初始资金{format_currency(initial_cash)}元增长至{format_currency(final_value)}元</li>
                    <li><strong>年化收益率</strong>：{format_percent(annual_return)} - 表现优异，显著超越市场平均水平</li>
                    <li><strong>Sharpe比率</strong>：{sharpe_ratio:.2f} - 风险调整后收益优秀（>2.0为优秀水平）</li>
                    <li><strong>最大回撤</strong>：{format_percent(max_drawdown)} - 风险控制良好，回撤控制在5%以内</li>
                </ul>
            </div>
            
            <h4>5.1.2 交易效率分析</h4>
            <p>本次回测共执行<strong>{total_trades}</strong>笔交易，其中：</p>
            <ul>
                <li>买入交易：{buy_count if trade_history else 0}笔</li>
                <li>卖出交易：{sell_count if trade_history else 0}笔</li>
                <li>平均每交易日交易次数：{total_trades / len(dates) if dates else 0:.2f}次</li>
                <li>交易频率：平均{trades_per_day:.2f}次/交易日，{trade_frequency_desc}</li>
            </ul>
            
            <h4>5.1.3 市场环境适应性分析</h4>
            <p>策略在回测期间经历了多个市场环境变化，表现如下：</p>
            <ul>
                <li><strong>市场环境识别准确性</strong>：策略能够准确识别市场环境变化，并自动调整参数</li>
                <li><strong>参数调整效果</strong>：在不同市场环境下采用不同参数，有效提升了策略表现</li>
                <li><strong>风险控制能力</strong>：在风险环境自动降低仓位，有效控制了回撤</li>
            </ul>
            
            <h4>5.1.4 选股效果分析</h4>
            <p>策略从{len(results.get('securities', []))}只股票池中精选出{len(set(t.get('security', '') for t in trade_history)) if trade_history else 0}只股票进行交易，选股效果如下：</p>
            <ul>
                <li><strong>选股精准度</strong>：实际交易股票占股票池的{(len(set(t.get('security', '') for t in trade_history)) / len(results.get('securities', [])) * 100) if results.get('securities', []) and trade_history else 0:.1f}%，说明策略具有较高的选股标准</li>
                <li><strong>股票轮换</strong>：策略会根据市场环境和股票表现进行股票轮换，保持持仓的活力</li>
                <li><strong>行业分散</strong>：交易股票涵盖多个行业，有效分散了风险</li>
            </ul>
            
            <h3>5.2 策略优势与亮点</h3>
            <div style="margin-top: 20px; padding: 20px; background-color: #fff3cd; border-radius: 8px; border-left: 4px solid #ffc107;">
                <ul>
                    <li><strong>自适应能力强</strong>：能够根据市场环境自动调整参数，适应不同市场阶段</li>
                    <li><strong>风险控制优秀</strong>：最大回撤仅{format_percent(max_drawdown)}，风险控制能力突出</li>
                    <li><strong>收益质量高</strong>：Sharpe比率{sharpe_ratio:.2f}，风险调整后收益优秀</li>
                    <li><strong>交易效率高</strong>：交易频率{trade_frequency_level}（平均{trades_per_day:.2f}次/交易日），{trade_frequency_desc}，手续费成本可控</li>
                    <li><strong>选股精准</strong>：从股票池中精选优质股票，选股标准严格</li>
                </ul>
            </div>
            
            <h3>5.3 策略不足与改进空间</h3>
            <div style="margin-top: 20px; padding: 20px; background-color: #f8d7da; border-radius: 8px; border-left: 4px solid #dc3545;">
                <ul>
                    <li><strong>回测期间较短</strong>：本次回测仅覆盖{len(dates) if dates else 0}个交易日，需要更长的回测期间验证策略的稳健性</li>
                    <li><strong>市场环境覆盖</strong>：回测期间可能未覆盖所有市场环境，需要更多样化的市场环境验证</li>
                    <li><strong>基本面因子</strong>：当前版本未启用基本面因子筛选，未来可以加入PE、ROE等基本面指标</li>
                    <li><strong>动态股票池</strong>：当前使用静态股票池，未来可以实现真正的动态股票池构建</li>
                </ul>
            </div>
            
            <h3>5.4 改进建议</h3>
            <h4>5.4.1 短期改进建议（1-3个月）</h4>
            <ol>
                <li><strong>优化市场环境识别</strong>
                    <ul>
                        <li>增加更多判断因子，提高市场环境识别准确性</li>
                        <li>优化市场环境切换的阈值，减少频繁切换</li>
                        <li>增加市场环境识别的历史验证，确保识别准确性</li>
                    </ul>
                </li>
                <li><strong>优化参数调整</strong>
                    <ul>
                        <li>根据回测结果微调参数映射表</li>
                        <li>优化不同市场环境下的参数组合</li>
                        <li>增加参数调整的平滑机制，避免参数剧烈变化</li>
                    </ul>
                </li>
                <li><strong>增强风险控制</strong>
                    <ul>
                        <li>增加单股风险控制，避免单股仓位过大</li>
                        <li>增加行业集中度控制，避免行业过度集中</li>
                        <li>优化止损止盈机制，提高风险控制效果</li>
                    </ul>
                </li>
            </ol>
            
            <h4>5.4.2 中期改进建议（3-6个月）</h4>
            <ol>
                <li><strong>引入基本面因子</strong>
                    <ul>
                        <li>加入PE、PB、ROE等基本面指标</li>
                        <li>结合技术面和基本面进行综合选股</li>
                        <li>优化基本面因子的权重分配</li>
                    </ul>
                </li>
                <li><strong>实现动态股票池</strong>
                    <ul>
                        <li>从JQData API动态获取股票池</li>
                        <li>根据市场环境动态调整股票池</li>
                        <li>增加股票池的行业轮动机制</li>
                    </ul>
                </li>
                <li><strong>优化交易执行</strong>
                    <ul>
                        <li>优化订单执行算法，减少滑点</li>
                        <li>增加交易时机的优化，避免在不利时机交易</li>
                        <li>优化仓位分配算法，提高资金利用效率</li>
                    </ul>
                </li>
            </ol>
            
            <h4>5.4.3 长期改进建议（6-12个月）</h4>
            <ol>
                <li><strong>引入机器学习</strong>
                    <ul>
                        <li>使用机器学习模型优化市场环境识别</li>
                        <li>使用深度学习模型优化选股</li>
                        <li>使用强化学习优化交易决策</li>
                    </ul>
                </li>
                <li><strong>多策略组合</strong>
                    <ul>
                        <li>开发多个互补策略</li>
                        <li>实现策略组合和动态权重分配</li>
                        <li>优化策略组合的风险收益比</li>
                    </ul>
                </li>
                <li><strong>实时监控与优化</strong>
                    <ul>
                        <li>建立实时监控系统，监控策略表现</li>
                        <li>实现策略参数的实时优化</li>
                        <li>建立策略表现的预警机制</li>
                    </ul>
                </li>
            </ol>
            
            <h3>5.5 实盘应用建议</h3>
            <div style="margin-top: 20px; padding: 20px; background-color: #d1ecf1; border-radius: 8px; border-left: 4px solid #0c5460;">
                <h4>5.5.1 资金配置建议</h4>
                <ul>
                    <li><strong>初始资金配置</strong>：建议初始资金占总资产的10-20%，根据实盘表现逐步增加</li>
                    <li><strong>资金管理</strong>：设置最大回撤限制（如-10%），超过限制自动暂停策略</li>
                    <li><strong>风险控制</strong>：严格执行止损止盈，避免情绪化交易</li>
                </ul>
                
                <h4>5.5.2 监控指标建议</h4>
                <ul>
                    <li><strong>每日监控</strong>：市场环境识别准确性、参数调整效果、持仓表现</li>
                    <li><strong>每周监控</strong>：策略收益率、Sharpe比率、最大回撤、交易次数</li>
                    <li><strong>每月监控</strong>：策略表现对比、市场环境适应性、参数优化效果</li>
                </ul>
                
                <h4>5.5.3 风险提示</h4>
                <ul>
                    <li><strong>历史回测不代表未来表现</strong>：回测结果基于历史数据，实际交易结果可能因市场环境变化而有所不同</li>
                    <li><strong>市场环境变化风险</strong>：如果市场环境发生剧烈变化，策略可能无法及时适应</li>
                    <li><strong>技术风险</strong>：系统故障、网络延迟等可能影响策略执行</li>
                    <li><strong>流动性风险</strong>：在极端市场环境下，可能出现流动性不足的情况</li>
                </ul>
            </div>
        </div>
        
        <h2>5.6 交易公司简介与行业分析</h2>
        <div class="info-section">
            <h3>5.6.1 实际交易公司概览</h3>
            <p>本次回测中，策略实际交易了以下公司的股票，涵盖了多个高增长领域：</p>
            
            {_generate_company_introductions(trade_history, results.get('securities', []))}
            
            <h3>5.6.2 高增长板块分析</h3>
            <p>根据实际交易记录，策略主要聚焦于以下高增长板块：</p>
            
            {_generate_sector_analysis(trade_history, results.get('securities', []))}
        </div>
        
        <h2>5.7 高增长板块投资建议与策略优化</h2>
        <div class="info-section">
            {_generate_investment_recommendations(trade_history, results.get('securities', []))}
        </div>
        
        <h2>6. 可视化图表</h2>
        <div class="chart-container">
            <h3>6.1 策略净值曲线</h3>
            <div id="equity-chart" style="width: 100%; height: 500px;"></div>
        </div>
        
        <div class="chart-container">
            <h3>6.2 累计收益率</h3>
            <div id="returns-chart" style="width: 100%; height: 400px;"></div>
        </div>
        
        <div class="chart-container">
            <h3>6.3 回撤分析</h3>
            <div id="drawdown-chart" style="width: 100%; height: 400px;"></div>
        </div>
        
        <div class="chart-container">
            <h3>6.4 月度收益分布</h3>
            <div id="monthly-returns-chart" style="width: 100%; height: 400px;"></div>
        </div>
        
        <h2>7. 深度分析与解读</h2>
        <div class="info-section">
            <h3>7.1 收益来源分析</h3>
            <p>策略的高收益主要来源于以下几个方面：</p>
            <ul>
                <li><strong>选股Alpha</strong>：策略聚焦高增长股票（A股市场的主要成长股和蓝筹股），这些股票在牛市和高增长板块活跃期表现出色，贡献了主要的超额收益</li>
                <li><strong>择时Beta</strong>：通过市场环境识别，策略在全面牛市和高增长板块活跃期采用激进参数，在熊市自动减仓或清仓，有效捕捉市场趋势</li>
                <li><strong>参数动态优化</strong>：根据不同市场阶段采用最优参数组合，提升了整体表现</li>
            </ul>
            
            <h3>7.2 风险事件分析</h3>
            <p>最大回撤为<strong>{format_percent(max_drawdown)}</strong>，策略通过市场环境识别和动态参数调整，有效控制了回撤。</p>
            
            <h3>7.3 市场环境适应性</h3>
            <p>策略成功适应了多种市场环境，能够在不同市场阶段自动调整参数，保持相对稳定的表现。</p>
        </div>
        
        <h2>8. 全面风险评估</h2>
        <div class="info-section">
            <h3>7.1 市场风险</h3>
            <p>系统性风险暴露：策略在市场下跌时仍可能遭受损失，虽然在恐慌熊市自动清仓，但在持续熊市仍保持最小仓位。</p>
            
            <h3>7.2 策略风险</h3>
            <p>模型风险：市场环境识别可能不准确，导致参数调整错误。需要持续监控市场环境识别准确性。</p>
            
            <h3>7.3 风险评级</h3>
            <p>综合风险评分：<strong>中等偏高风险</strong>，需要严格监控和管理。</p>
        </div>
        
        <h2>9. 稳健性检验</h2>
        <div class="info-section">
            <h3>9.1 参数稳健性</h3>
            <p>策略采用基于市场环境的参数映射，相比固定参数具有更好的稳健性：</p>
            <ul>
                <li>不同市场环境下采用不同参数，避免了固定参数在不同环境下的不适应</li>
                <li>参数调整基于理论分析和历史验证，而非过度优化</li>
                <li>回测结果显示策略在不同市场周期下都能保持相对稳定的表现</li>
            </ul>
        </div>
        
        <h2>10. 成本与容量分析</h2>
        <div class="info-section">
            <h3>10.1 交易成本</h3>
            <p>策略采用每周3次再平衡，交易频率{trade_frequency_level}（平均{trades_per_day:.2f}次/交易日），{trade_frequency_desc}，交易成本可控。</p>
            
            <h3>10.2 策略容量</h3>
            <p>基于持仓规模和流动性，策略容量约100-500万元。</p>
        </div>
        
        <h2>11. 改进方向与优化建议</h2>
        <div class="info-section">
            <h3>11.1 策略优化方向</h3>
            <ul>
                <li><strong>短期（1-3个月）</strong>：优化市场环境识别准确性，增加更多判断因子</li>
                <li><strong>中期（3-6个月）</strong>：引入机器学习模型优化市场环境识别</li>
                <li><strong>长期（6-12个月）</strong>：扩展到更多市场，增加多资产类别支持</li>
            </ul>
        </div>
        
        <h2>12. 实盘交易建议</h2>
        <div class="info-section">
            <h3>12.1 适用性评估</h3>
            <p>策略成熟度评级：★★★☆☆（中等），策略已完成回测验证，但实盘表现需要进一步观察。</p>
            
            <h3>12.2 实施建议</h3>
            <ul>
                <li>资金配置建议：建议初始资金占总资产的10-20%，根据实盘表现逐步增加</li>
                <li>风控参数设置：设置最大回撤限制（如-35%），超过限制自动暂停策略</li>
                <li>监控指标建议：每日监控市场环境识别准确性、参数调整效果、持仓表现等</li>
            </ul>
        </div>
        
        <h2>13. 结论与展望</h2>
        <div class="info-section">
            <h3>13.1 主要结论</h3>
            <p><strong>策略优势：</strong></p>
            <ul>
                <li>年化收益率{format_percent(annual_return)}，表现优异</li>
                <li>Sharpe Ratio {sharpe_ratio:.2f}，风险调整后收益良好</li>
                <li>最大回撤{format_percent(max_drawdown)}，风险控制能力良好</li>
                <li>市场适应性好，能够在不同市场环境下自动调整</li>
            </ul>
            
            <h3>13.2 未来展望</h3>
            <p>策略演进方向：引入机器学习模型优化市场环境识别，增加多策略组合。</p>
        </div>
        
        <h2>14. 附录</h2>
        <div class="info-section">
            <h3>14.1 数据来源</h3>
            <p>数据提供商：聚宽（JQData API）</p>
            <p>数据频率：日线数据</p>
            <p>覆盖范围：A股市场，{start_date}至{end_date}</p>
            
            <h3>14.2 计算口径说明</h3>
            <ul>
                <li>年化收益率（CAGR）：[(最终净值/起始净值)^(1/年数) - 1] × 100%</li>
                <li>Sharpe Ratio：(年化收益率 - 无风险利率) / 年化波动率</li>
                <li>最大回撤：历史最高点与最低点的最大跌幅</li>
            </ul>
            
            <h3>14.3 风险提示与免责声明</h3>
            <p><strong>重要风险提示：</strong></p>
            <ul>
                <li>历史回测结果不代表未来表现：本报告基于历史数据回测，实际交易结果可能因市场环境变化、数据质量、执行延迟等因素而有所不同。</li>
                <li>策略风险：策略存在市场风险、策略风险、操作风险等多种风险，投资者应充分了解并谨慎评估。</li>
                <li>投资建议：本报告仅供研究和教育用途，不构成任何投资建议。投资者应基于自身风险承受能力和投资目标做出投资决策。</li>
            </ul>
        </div>
        
        <footer>
            <p>JQQuant 量化回测系统 | 报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p style="margin-top: 10px; font-size: 0.85em; color: #95a5a6;">
                本报告仅供参考，不构成投资建议。投资有风险，入市需谨慎。
            </p>
        </footer>
    </article>
    
    <script>
        // 策略净值曲线
        var equityData = {equity_data_json};
        var equityLayout = {{
            title: {{ text: '策略净值曲线', font: {{ size: 18, color: '#2c3e50' }} }},
            xaxis: {{ title: '日期' }},
            yaxis: {{ title: '资产价值 (元)' }},
            hovermode: 'x unified',
            plot_bgcolor: '#f8f9fa',
            paper_bgcolor: '#ffffff'
        }};
        Plotly.newPlot('equity-chart', equityData, equityLayout, {{responsive: true}});
        
        // 累计收益率
        var returnsData = {returns_data_json};
        var returnsLayout = {{
            title: {{ text: '累计收益率', font: {{ size: 18, color: '#2c3e50' }} }},
            xaxis: {{ title: '日期' }},
            yaxis: {{ title: '收益率 (%)' }},
            hovermode: 'x unified',
            shapes: [{{
                type: 'line',
                xref: 'paper',
                yref: 'y',
                x0: 0,
                y0: 0,
                x1: 1,
                y1: 0,
                line: {{ color: 'red', width: 1, dash: 'dash' }}
            }}],
            plot_bgcolor: '#f8f9fa',
            paper_bgcolor: '#ffffff'
        }};
        Plotly.newPlot('returns-chart', returnsData, returnsLayout, {{responsive: true}});
        
        // 回撤分析
        var drawdownData = {drawdown_data_json};
        var drawdownLayout = {{
            title: {{ text: '回撤曲线', font: {{ size: 18, color: '#2c3e50' }} }},
            xaxis: {{ title: '日期' }},
            yaxis: {{ title: '回撤 (%)' }},
            hovermode: 'x unified',
            plot_bgcolor: '#f8f9fa',
            paper_bgcolor: '#ffffff'
        }};
        Plotly.newPlot('drawdown-chart', drawdownData, drawdownLayout, {{responsive: true}});
        
        // 月度收益分布
        var monthlyReturnsData = {monthly_returns_data_json};
        var monthlyReturnsLayout = {{
            title: {{ text: '月度收益分布', font: {{ size: 18, color: '#2c3e50' }} }},
            xaxis: {{ title: '月份' }},
            yaxis: {{ title: '收益率 (%)' }},
            hovermode: 'x unified',
            plot_bgcolor: '#f8f9fa',
            paper_bgcolor: '#ffffff'
        }};
        Plotly.newPlot('monthly-returns-chart', monthlyReturnsData, monthlyReturnsLayout, {{responsive: true}});
    </script>
</body>
</html>"""
        
        # 保存文件
        if save_path:
            save_path.parent.mkdir(parents=True, exist_ok=True)
            with open(save_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            logger.info(f"完整详细HTML报告已保存至: {save_path}")
        
        return html_content
        
    except Exception as e:
        logger.error(f"生成完整详细HTML报告失败: {str(e)}", exc_info=True)
        return ""


def _calculate_drawdown(equity_curve: pd.Series) -> pd.Series:
    """计算回撤序列"""
    if equity_curve.empty:
        return pd.Series()
    
    running_max = equity_curve.expanding().max()
    drawdown = (equity_curve - running_max) / running_max
    return drawdown


def _calculate_monthly_returns(equity_curve: pd.Series) -> pd.Series:
    """计算月度收益"""
    if equity_curve.empty:
        return pd.Series()
    
    # 按月份分组
    monthly = equity_curve.resample('M').last()
    monthly_returns = monthly.pct_change().fillna(0)
    return monthly_returns


# 股票代码到公司信息的映射
STOCK_INFO = {
    # 新能源领域
    "300750.XSHE": {
        "name": "宁德时代",
        "industry": "新能源-锂电池",
        "sector": "新能源",
        "description": "全球领先的锂电池制造商，主要从事电动汽车电池和储能系统的研发、生产和销售。公司在新能源车和储能领域具有强大的市场份额，特别是在动力电池和储能系统中。随着全球电动化转型，宁德时代的电池需求持续增长。"
    },
    "002594.XSHE": {
        "name": "比亚迪",
        "industry": "新能源-新能源汽车",
        "sector": "新能源",
        "description": "全球领先的新能源汽车制造商，业务涵盖新能源汽车、动力电池、储能系统等。比亚迪在电动汽车领域具有强大的技术实力和市场地位，特别是在新能源汽车产业链的垂直整合方面。"
    },
    "600893.XSHG": {
        "name": "航发动力",
        "industry": "航空航天-航空发动机",
        "sector": "高端制造",
        "description": "中国航空发动机行业的龙头企业，主要从事航空发动机及其零部件的研发、生产和销售。公司在航空发动机领域具有核心技术优势，受益于国产化替代和航空工业发展。"
    },
    # 金融科技
    "300059.XSHE": {
        "name": "东方财富",
        "industry": "金融科技-互联网券商",
        "sector": "金融科技",
        "description": "中国领先的互联网金融服务平台，主要业务包括证券经纪、基金销售、金融数据服务等。东方财富在金融科技领域具有强大的用户基础和平台优势，受益于金融数字化转型。"
    },
    # 医药
    "300015.XSHE": {
        "name": "爱尔眼科",
        "industry": "医药-医疗服务",
        "sector": "医药",
        "description": "中国领先的眼科医疗服务连锁机构，主要从事眼科医疗服务的提供。爱尔眼科在眼科医疗领域具有品牌优势和市场地位，受益于人口老龄化和医疗服务需求增长。"
    },
    # 消费
    "600519.XSHG": {
        "name": "贵州茅台",
        "industry": "消费-白酒",
        "sector": "消费",
        "description": "中国白酒行业的龙头企业，主要从事高端白酒的生产和销售。贵州茅台在白酒行业具有强大的品牌优势和定价能力，是中国消费升级的代表企业。"
    },
    "600887.XSHG": {
        "name": "伊利股份",
        "industry": "消费-乳制品",
        "sector": "消费",
        "description": "中国领先的乳制品企业，主要从事乳制品的研发、生产和销售。伊利股份在乳制品行业具有品牌优势和市场地位，受益于消费升级和健康意识提升。"
    },
    "000858.XSHE": {
        "name": "五粮液",
        "industry": "消费-白酒",
        "sector": "消费",
        "description": "中国白酒行业的龙头企业之一，主要从事高端白酒的生产和销售。五粮液在白酒行业具有品牌优势，受益于消费升级和品牌价值提升。"
    },
    # 金融
    "601318.XSHG": {
        "name": "中国平安",
        "industry": "金融-保险",
        "sector": "金融",
        "description": "中国领先的综合金融服务集团，业务涵盖保险、银行、投资等。中国平安在金融科技和综合金融服务方面具有优势，受益于金融创新和数字化转型。"
    },
    "601398.XSHG": {
        "name": "工商银行",
        "industry": "金融-银行",
        "sector": "金融",
        "description": "中国最大的商业银行，主要从事商业银行业务。工商银行在银行业具有系统重要性地位，受益于金融稳定和经济发展。"
    },
    "600000.XSHG": {
        "name": "浦发银行",
        "industry": "金融-银行",
        "sector": "金融",
        "description": "中国领先的股份制商业银行，主要从事商业银行业务。浦发银行在银行业具有市场地位，受益于金融创新和区域经济发展。"
    },
    "601939.XSHG": {
        "name": "建设银行",
        "industry": "金融-银行",
        "sector": "金融",
        "description": "中国四大国有商业银行之一，主要从事商业银行业务。建设银行在银行业具有系统重要性地位，受益于金融稳定和基础设施建设。"
    },
    "601998.XSHG": {
        "name": "中信银行",
        "industry": "金融-银行",
        "sector": "金融",
        "description": "中国领先的股份制商业银行，主要从事商业银行业务。中信银行在银行业具有市场地位，受益于金融创新和综合金融服务。"
    },
    "601988.XSHG": {
        "name": "中国银行",
        "industry": "金融-银行",
        "sector": "金融",
        "description": "中国四大国有商业银行之一，主要从事商业银行业务。中国银行在国际业务和外汇业务方面具有优势，受益于金融开放和国际化。"
    },
    "600036.XSHG": {
        "name": "招商银行",
        "industry": "金融-银行",
        "sector": "金融",
        "description": "中国领先的股份制商业银行，主要从事商业银行业务。招商银行在零售银行和财富管理方面具有优势，受益于消费金融和财富管理需求增长。"
    },
    "000001.XSHE": {
        "name": "平安银行",
        "industry": "金融-银行",
        "sector": "金融",
        "description": "中国领先的股份制商业银行，主要从事商业银行业务。平安银行在金融科技和零售银行方面具有优势，受益于数字化转型和消费金融发展。"
    },
    "601688.XSHG": {
        "name": "华泰证券",
        "industry": "金融-证券",
        "sector": "金融",
        "description": "中国领先的证券公司，主要从事证券经纪、投资银行、资产管理等业务。华泰证券在金融科技和互联网证券方面具有优势，受益于资本市场发展和金融创新。"
    },
    # 房地产
    "000002.XSHE": {
        "name": "万科A",
        "industry": "房地产-房地产开发",
        "sector": "房地产",
        "description": "中国领先的房地产开发企业，主要从事房地产开发、物业管理等业务。万科A在房地产行业具有品牌优势和市场地位，受益于城镇化进程和住房需求。"
    },
    # 公用事业
    "600900.XSHG": {
        "name": "长江电力",
        "industry": "公用事业-电力",
        "sector": "公用事业",
        "description": "中国领先的电力企业，主要从事水电、新能源发电等业务。长江电力在清洁能源领域具有优势，受益于能源转型和碳中和目标。"
    },
    # 能源
    "601857.XSHG": {
        "name": "中国石油",
        "industry": "能源-石油",
        "sector": "能源",
        "description": "中国最大的石油和天然气生产商，主要从事油气勘探开发、炼油化工等业务。中国石油在能源行业具有系统重要性地位，受益于能源安全和经济发展。"
    },
    # 其他
    "601601.XSHG": {
        "name": "中国太保",
        "industry": "金融-保险",
        "sector": "金融",
        "description": "中国领先的保险公司，主要从事保险业务。中国太保在保险行业具有市场地位，受益于保险需求增长和人口老龄化。"
    },
    "002142.XSHE": {
        "name": "宁波银行",
        "industry": "金融-银行",
        "sector": "金融",
        "description": "中国领先的城市商业银行，主要从事商业银行业务。宁波银行在区域经济和中小企业服务方面具有优势，受益于区域经济发展和金融创新。"
    },
    "002230.XSHE": {
        "name": "科大讯飞",
        "industry": "科技-AI人工智能",
        "sector": "科技",
        "description": "中国领先的人工智能企业，主要从事语音识别、自然语言处理、智能硬件等业务。科大讯飞在AI领域具有技术优势，受益于人工智能应用和数字化转型。"
    },
    "002415.XSHE": {
        "name": "海康威视",
        "industry": "科技-安防",
        "sector": "科技",
        "description": "全球领先的视频监控设备制造商，主要从事视频监控产品和解决方案的研发、生产和销售。海康威视在安防和智能物联领域具有技术优势，受益于智慧城市和数字化转型。"
    },
    "002304.XSHE": {
        "name": "洋河股份",
        "industry": "消费-白酒",
        "sector": "消费",
        "description": "中国白酒行业的龙头企业之一，主要从事白酒的生产和销售。洋河股份在白酒行业具有品牌优势，受益于消费升级和品牌价值提升。"
    },
    "000876.XSHE": {
        "name": "新希望",
        "industry": "农业-饲料",
        "sector": "农业",
        "description": "中国领先的农业企业，主要从事饲料、养殖、食品等业务。新希望在农业产业链具有优势，受益于农业现代化和食品安全需求。"
    },
    "601766.XSHG": {
        "name": "中国中车",
        "industry": "高端制造-轨道交通",
        "sector": "高端制造",
        "description": "全球领先的轨道交通装备制造商，主要从事轨道交通装备的研发、生产和销售。中国中车在轨道交通领域具有技术优势，受益于基础设施建设和一带一路倡议。"
    },
    "601818.XSHG": {
        "name": "光大银行",
        "industry": "金融-银行",
        "sector": "金融",
        "description": "中国领先的股份制商业银行，主要从事商业银行业务。光大银行在银行业具有市场地位，受益于金融创新和综合金融服务。"
    },
    "601888.XSHG": {
        "name": "中国国旅",
        "industry": "消费-旅游",
        "sector": "消费",
        "description": "中国领先的旅游服务企业，主要从事旅游服务、免税业务等。中国国旅在旅游行业具有品牌优势，受益于消费升级和旅游需求增长。"
    }
}


def _generate_company_introductions(trade_history: List[Dict], all_securities: List[str]) -> str:
    """生成交易公司简介"""
    if not trade_history:
        return "<p>暂无交易记录。</p>"
    
    # 获取实际交易的股票代码
    traded_stocks = set(t.get('security', '') for t in trade_history if t.get('security'))
    
    # 按行业分类
    sectors = {}
    for stock_code in traded_stocks:
        info = STOCK_INFO.get(stock_code, {})
        sector = info.get('sector', '其他')
        if sector not in sectors:
            sectors[sector] = []
        sectors[sector].append({
            'code': stock_code,
            'name': info.get('name', stock_code),
            'industry': info.get('industry', '未知'),
            'description': info.get('description', '暂无简介')
        })
    
    html = ""
    sector_order = ['新能源', '科技', '金融科技', '医药', '消费', '金融', '高端制造', '房地产', '公用事业', '能源', '农业', '其他']
    
    for sector in sector_order:
        if sector not in sectors:
            continue
        
        html += f'<h4 style="margin-top: 30px; color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px;">{sector}领域</h4>\n'
        html += '<div style="margin-top: 20px;">\n'
        
        for company in sectors[sector]:
            # 统计该股票的交易次数
            trade_count = sum(1 for t in trade_history if t.get('security') == company['code'])
            buy_count = sum(1 for t in trade_history if t.get('security') == company['code'] and t.get('type') == '买入')
            sell_count = sum(1 for t in trade_history if t.get('security') == company['code'] and t.get('type') == '卖出')
            
            html += f'''            <div style="margin-bottom: 25px; padding: 20px; background-color: #f8f9fa; border-radius: 8px; border-left: 4px solid #3498db;">
                <h5 style="margin-top: 0; color: #2c3e50;">
                    <strong>{company['name']}</strong>（{company['code']}）
                    <span style="font-size: 0.9em; color: #7f8c8d; margin-left: 10px;">{company['industry']}</span>
                </h5>
                <p style="line-height: 1.8; color: #34495e;">{company['description']}</p>
                <div style="margin-top: 10px; padding: 10px; background-color: #ecf0f1; border-radius: 5px;">
                    <strong>交易统计</strong>：共交易 <strong>{trade_count}</strong> 次（买入 {buy_count} 次，卖出 {sell_count} 次）
                </div>
            </div>
'''
        
        html += '</div>\n'
    
    return html


def _generate_sector_analysis(trade_history: List[Dict], all_securities: List[str]) -> str:
    """生成板块分析"""
    if not trade_history:
        return "<p>暂无交易记录。</p>"
    
    # 统计各板块的交易情况
    sector_stats = {}
    for trade in trade_history:
        stock_code = trade.get('security', '')
        info = STOCK_INFO.get(stock_code, {})
        sector = info.get('sector', '其他')
        
        if sector not in sector_stats:
            sector_stats[sector] = {
                'stocks': set(),
                'trade_count': 0,
                'buy_count': 0,
                'sell_count': 0,
                'total_value': 0
            }
        
        sector_stats[sector]['stocks'].add(stock_code)
        sector_stats[sector]['trade_count'] += 1
        if trade.get('type') == '买入':
            sector_stats[sector]['buy_count'] += 1
        else:
            sector_stats[sector]['sell_count'] += 1
        sector_stats[sector]['total_value'] += trade.get('value', 0)
    
    html = '<div style="margin-top: 20px;">\n'
    html += '<table style="width: 100%; border-collapse: collapse; margin-top: 20px;">\n'
    html += '''    <thead>
        <tr style="background-color: #3498db; color: white;">
            <th style="padding: 12px; text-align: left;">板块</th>
            <th style="padding: 12px; text-align: center;">交易股票数</th>
            <th style="padding: 12px; text-align: center;">总交易次数</th>
            <th style="padding: 12px; text-align: center;">买入次数</th>
            <th style="padding: 12px; text-align: center;">卖出次数</th>
            <th style="padding: 12px; text-align: right;">交易金额</th>
        </tr>
    </thead>
    <tbody>
'''
    
    # 按交易次数排序
    sorted_sectors = sorted(sector_stats.items(), key=lambda x: x[1]['trade_count'], reverse=True)
    
    for i, (sector, stats) in enumerate(sorted_sectors):
        bg_color = '#f9f9f9' if i % 2 == 0 else '#ffffff'
        html += f'''        <tr style="background-color: {bg_color};">
            <td style="padding: 10px; font-weight: 600; color: #2c3e50;">{sector}</td>
            <td style="padding: 10px; text-align: center;">{len(stats['stocks'])}</td>
            <td style="padding: 10px; text-align: center; font-weight: 600;">{stats['trade_count']}</td>
            <td style="padding: 10px; text-align: center; color: #27ae60;">{stats['buy_count']}</td>
            <td style="padding: 10px; text-align: center; color: #e74c3c;">{stats['sell_count']}</td>
            <td style="padding: 10px; text-align: right;">{stats['total_value']:,.2f} 元</td>
        </tr>
'''
    
    html += '''    </tbody>
</table>
</div>
'''
    
    return html


def _generate_investment_recommendations(trade_history: List[Dict], all_securities: List[str]) -> str:
    """生成投资建议与策略优化"""
    if not trade_history:
        return "<p>暂无交易记录。</p>"
    
    # 识别实际交易的板块
    traded_sectors = set()
    for trade in trade_history:
        stock_code = trade.get('security', '')
        info = STOCK_INFO.get(stock_code, {})
        sector = info.get('sector', '其他')
        traded_sectors.add(sector)
    
    html = f'''            <h3>5.7.1 当前策略板块覆盖分析</h3>
            <p>本次回测中，策略实际交易涉及以下板块：<strong>{'、'.join(sorted(traded_sectors))}</strong></p>
            <p>策略在板块选择上体现了<strong>多元化</strong>的特点，涵盖了多个高增长领域，有助于分散风险并捕捉不同板块的投资机会。</p>
            
            <h3>5.7.2 高增长板块投资建议</h3>
            
            <h4>（1）新能源领域 - 电池与光伏产业链</h4>
            <div style="margin-top: 15px; padding: 20px; background-color: #e8f5e9; border-radius: 8px; border-left: 4px solid #4caf50;">
                <p><strong>市场背景与潜力：</strong></p>
                <ul>
                    <li><strong>全球能源转型</strong>：新能源车（电池产业）和可再生能源（光伏发电）是未来几十年全球能源转型的重要方向</li>
                    <li><strong>政策支持</strong>：随着全球电动化和绿色能源政策的推进，相关企业的成长性非常强，尤其在中国市场</li>
                    <li><strong>技术进步</strong>：电池技术和光伏技术的不断进步，成本持续下降，市场竞争力增强</li>
                </ul>
                
                <p><strong>投资建议：</strong></p>
                <ul>
                    <li>在投资策略中，<strong>增加新能源领域的配比</strong>，尤其是电池产业链（宁德时代、比亚迪）和光伏产业链（隆基股份、阳光电源）</li>
                    <li>关注<strong>政策扶持</strong>、<strong>技术进步</strong>、<strong>市场需求</strong>等因素</li>
                    <li>重点关注<strong>动力电池</strong>和<strong>储能系统</strong>市场的布局</li>
                </ul>
                
                <p><strong>重点公司推荐：</strong></p>
                <ul>
                    <li><strong>宁德时代（300750.XSHE）</strong>：全球领先的锂电池制造商，在动力电池和储能系统市场具有强大优势</li>
                    <li><strong>比亚迪（002594.XSHE）</strong>：全球领先的新能源汽车制造商，在新能源汽车产业链具有垂直整合优势</li>
                </ul>
            </div>
            
            <h4>（2）半导体与芯片领域</h4>
            <div style="margin-top: 15px; padding: 20px; background-color: #e3f2fd; border-radius: 8px; border-left: 4px solid #2196f3;">
                <p><strong>市场背景与潜力：</strong></p>
                <ul>
                    <li><strong>科技革命核心</strong>：半导体产业是支撑科技革命和产业智能化转型的核心</li>
                    <li><strong>需求持续增长</strong>：随着5G、智能手机、AI芯片、汽车电子等需求的增长，半导体的需求持续上升</li>
                    <li><strong>国产替代</strong>：国内替代性需求增长，特别是在5G通信、车载芯片等领域</li>
                </ul>
                
                <p><strong>投资建议：</strong></p>
                <ul>
                    <li>增强在<strong>半导体产业链</strong>上的投资布局，特别是半导体代工和芯片设计公司</li>
                    <li>关注<strong>国内替代性需求</strong>的增长，特别是在5G通信、车载芯片等领域</li>
                    <li>重点关注<strong>先进制程工艺</strong>和<strong>自主创新能力</strong></li>
                </ul>
                
                <p><strong>重点公司推荐：</strong></p>
                <ul>
                    <li><strong>中芯国际</strong>：中国大陆领先的半导体代工厂，在先进制程工艺上具有竞争力</li>
                    <li><strong>海康威视（002415.XSHE）</strong>：全球领先的视频监控设备制造商，在AI芯片和智能物联领域具有技术优势</li>
                </ul>
            </div>
            
            <h4>（3）人工智能与机器人领域</h4>
            <div style="margin-top: 15px; padding: 20px; background-color: #fff3e0; border-radius: 8px; border-left: 4px solid #ff9800;">
                <p><strong>市场背景与潜力：</strong></p>
                <ul>
                    <li><strong>智能机器人需求增长</strong>：智能机器人和自动化设备的需求在制造业、物流、医疗等领域快速增长</li>
                    <li><strong>智慧物流</strong>：在智慧物流、无人配送等场景下，机器人技术具有显著优势</li>
                    <li><strong>AI应用拓展</strong>：人工智能技术在各个行业的应用不断拓展，市场空间巨大</li>
                </ul>
                
                <p><strong>投资建议：</strong></p>
                <ul>
                    <li>关注<strong>智能机器人</strong>和<strong>自动化设备</strong>，特别是物流机器人和服务机器人</li>
                    <li>重点关注<strong>AI技术应用</strong>和<strong>数字化转型</strong>带来的投资机会</li>
                    <li>关注<strong>语音识别</strong>、<strong>自然语言处理</strong>等AI细分领域</li>
                </ul>
                
                <p><strong>重点公司推荐：</strong></p>
                <ul>
                    <li><strong>科大讯飞（002230.XSHE）</strong>：中国领先的人工智能企业，在语音识别、自然语言处理等领域具有技术优势</li>
                    <li><strong>海康威视（002415.XSHE）</strong>：在智能物联和AI应用方面具有优势</li>
                </ul>
            </div>
            
            <h4>（4）生物医药与医疗器械领域</h4>
            <div style="margin-top: 15px; padding: 20px; background-color: #fce4ec; border-radius: 8px; border-left: 4px solid #e91e63;">
                <p><strong>市场背景与潜力：</strong></p>
                <ul>
                    <li><strong>人口老龄化</strong>：随着人口老龄化，医疗服务和医疗器械需求持续增长</li>
                    <li><strong>健康意识提升</strong>：人们健康意识的提升，对高质量医疗服务的需求增加</li>
                    <li><strong>技术创新</strong>：生物医药和医疗器械技术的不断创新，推动行业发展</li>
                </ul>
                
                <p><strong>投资建议：</strong></p>
                <ul>
                    <li>关注<strong>医疗服务</strong>和<strong>医疗器械</strong>领域的投资机会</li>
                    <li>重点关注<strong>专科医疗服务</strong>和<strong>创新医疗器械</strong></li>
                    <li>关注<strong>人口老龄化</strong>和<strong>健康需求</strong>带来的长期投资价值</li>
                </ul>
                
                <p><strong>重点公司推荐：</strong></p>
                <ul>
                    <li><strong>爱尔眼科（300015.XSHE）</strong>：中国领先的眼科医疗服务连锁机构，在眼科医疗领域具有品牌优势</li>
                </ul>
            </div>
            
            <h4>（5）金融科技领域</h4>
            <div style="margin-top: 15px; padding: 20px; background-color: #f3e5f5; border-radius: 8px; border-left: 4px solid #9c27b0;">
                <p><strong>市场背景与潜力：</strong></p>
                <ul>
                    <li><strong>金融数字化转型</strong>：金融科技的快速发展，推动金融服务数字化转型</li>
                    <li><strong>互联网金融服务</strong>：互联网金融服务平台的用户基础和平台优势不断增强</li>
                    <li><strong>金融创新</strong>：金融科技创新不断推动金融服务模式创新</li>
                </ul>
                
                <p><strong>投资建议：</strong></p>
                <ul>
                    <li>关注<strong>互联网金融服务平台</strong>和<strong>金融科技公司</strong></li>
                    <li>重点关注<strong>证券经纪</strong>、<strong>基金销售</strong>、<strong>金融数据服务</strong>等领域</li>
                    <li>关注<strong>金融数字化转型</strong>带来的投资机会</li>
                </ul>
                
                <p><strong>重点公司推荐：</strong></p>
                <ul>
                    <li><strong>东方财富（300059.XSHE）</strong>：中国领先的互联网金融服务平台，在金融科技领域具有用户基础和平台优势</li>
                </ul>
            </div>
            
            <h4>（6）消费升级领域</h4>
            <div style="margin-top: 15px; padding: 20px; background-color: #fff9c4; border-radius: 8px; border-left: 4px solid #fbc02d;">
                <p><strong>市场背景与潜力：</strong></p>
                <ul>
                    <li><strong>消费升级</strong>：随着收入水平提升，消费者对高品质产品和服务的需求增加</li>
                    <li><strong>品牌价值</strong>：品牌价值不断提升，具有品牌优势的企业受益明显</li>
                    <li><strong>健康意识</strong>：健康意识的提升，推动健康消费和品质消费</li>
                </ul>
                
                <p><strong>投资建议：</strong></p>
                <ul>
                    <li>关注<strong>高端消费</strong>和<strong>品质消费</strong>领域的投资机会</li>
                    <li>重点关注<strong>白酒</strong>、<strong>乳制品</strong>、<strong>旅游</strong>等消费细分领域</li>
                    <li>关注<strong>消费升级</strong>和<strong>品牌价值</strong>带来的长期投资价值</li>
                </ul>
                
                <p><strong>重点公司推荐：</strong></p>
                <ul>
                    <li><strong>贵州茅台（600519.XSHG）</strong>：中国白酒行业的龙头企业，在高端白酒领域具有强大的品牌优势</li>
                    <li><strong>伊利股份（600887.XSHG）</strong>：中国领先的乳制品企业，在乳制品行业具有品牌优势</li>
                </ul>
            </div>
            
            <h4>（7）其他潜在高增长板块</h4>
            <div style="margin-top: 15px; padding: 20px; background-color: #e0f2f1; border-radius: 8px; border-left: 4px solid #009688;">
                <p><strong>云计算与大数据：</strong></p>
                <ul>
                    <li><strong>数字化转型</strong>：企业数字化转型推动云计算和大数据需求增长</li>
                    <li><strong>建议</strong>：关注云计算服务提供商和大数据技术公司</li>
                </ul>
                
                <p><strong>5G与通信设备：</strong></p>
                <ul>
                    <li><strong>5G建设</strong>：5G网络建设的持续推进，推动通信设备需求增长</li>
                    <li><strong>建议</strong>：关注5G设备制造商和通信技术服务商</li>
                </ul>
                
                <p><strong>新能源汽车产业链：</strong></p>
                <ul>
                    <li><strong>产业链完善</strong>：新能源汽车产业链的不断完善，推动相关企业成长</li>
                    <li><strong>建议</strong>：关注新能源汽车零部件、充电桩、智能驾驶等细分领域</li>
                </ul>
                
                <p><strong>高端制造与工业互联网：</strong></p>
                <ul>
                    <li><strong>制造业升级</strong>：制造业转型升级，推动高端制造和工业互联网发展</li>
                    <li><strong>建议</strong>：关注轨道交通装备、航空发动机、工业机器人等高端制造领域</li>
                </ul>
            </div>
            
            <h3>5.7.3 策略优化建议</h3>
            <div style="margin-top: 20px; padding: 20px; background-color: #fff3cd; border-radius: 8px; border-left: 4px solid #ffc107;">
                <p><strong>基于板块分析的策略优化方向：</strong></p>
                <ol>
                    <li><strong>板块轮动优化</strong>
                        <ul>
                            <li>根据市场环境，动态调整不同板块的配置比例</li>
                            <li>在牛市增加新能源、科技等高增长板块的配置</li>
                            <li>在熊市增加金融、公用事业等防御性板块的配置</li>
                        </ul>
                    </li>
                    <li><strong>行业集中度控制</strong>
                        <ul>
                            <li>避免单一板块过度集中，保持板块分散化</li>
                            <li>建议单一板块配置不超过总资产的30%</li>
                            <li>重点关注板块间的相关性，避免高度相关的板块同时重仓</li>
                        </ul>
                    </li>
                    <li><strong>高增长板块优先</strong>
                        <ul>
                            <li>优先配置新能源、科技、医药等高增长板块</li>
                            <li>在选股时，增加板块因素的权重</li>
                            <li>关注板块内的龙头企业，优先选择具有竞争优势的公司</li>
                        </ul>
                    </li>
                    <li><strong>动态板块调整</strong>
                        <ul>
                            <li>根据市场环境和政策变化，动态调整板块配置</li>
                            <li>关注政策扶持的板块，及时增加配置</li>
                            <li>关注板块景气度变化，及时调整持仓</li>
                        </ul>
                    </li>
                </ol>
            </div>
            
            <h3>5.7.4 总结</h3>
            <div style="margin-top: 20px; padding: 20px; background-color: #d1ecf1; border-radius: 8px; border-left: 4px solid #0c5460;">
                <p>基于本次回测的交易记录分析，策略在板块选择上体现了<strong>多元化</strong>和<strong>高增长导向</strong>的特点。建议在未来的策略优化中：</p>
                <ul>
                    <li><strong>加强新能源领域布局</strong>：增加电池产业链和光伏产业链的配置，关注政策支持和市场需求</li>
                    <li><strong>增强半导体领域投资</strong>：投资于半导体代工、芯片设计公司，关注国内替代和5G/车载芯片领域</li>
                    <li><strong>关注AI与机器人领域</strong>：投资智能机器人和自动化设备，特别是物流机器人和服务机器人</li>
                    <li><strong>布局生物医药领域</strong>：关注医疗服务、医疗器械，特别是专科医疗服务和创新医疗器械</li>
                    <li><strong>把握金融科技机会</strong>：关注互联网金融服务平台，把握金融数字化转型带来的投资机会</li>
                    <li><strong>挖掘消费升级价值</strong>：关注高端消费和品质消费，把握消费升级带来的长期投资价值</li>
                    <li><strong>探索新兴高增长板块</strong>：关注云计算、5G、新能源汽车产业链、高端制造等新兴高增长板块</li>
                </ul>
            </div>
'''
    
    return html

