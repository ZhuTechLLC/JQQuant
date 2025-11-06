# -*- coding: utf-8 -*-
"""
HTML报告生成器
"""
import json
import pandas as pd
import numpy as np
from typing import Dict, Optional
from pathlib import Path
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def generate_html_report(
    results: Dict,
    strategy_name: str,
    strategy_version: str = "1.0.0",
    strategy_params: Optional[Dict] = None,
    save_path: Optional[Path] = None
) -> str:
    """
    生成HTML格式的回测报告
    
    Args:
        results: 回测结果字典
        strategy_name: 策略名称
        strategy_version: 策略版本
        strategy_params: 策略参数
        save_path: 保存路径
    
    Returns:
        HTML字符串
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
        if returns_pct.empty and len(total_value_series) > 1:
            returns_pct = total_value_series.pct_change().fillna(0)
        
        # 计算累计收益率
        cumulative_returns = (1 + returns_pct).cumprod() - 1 if not returns_pct.empty else pd.Series()
        
        # 计算回撤
        drawdown = _calculate_drawdown(total_value_series) if not total_value_series.empty else pd.Series()
        
        # 准备图表数据（JSON格式，供Plotly使用）
        # 确保total_value是list
        total_value_list = total_value if isinstance(total_value, list) else (total_value.tolist() if hasattr(total_value, 'tolist') else list(total_value))
        
        equity_data = {
            'x': [d.strftime('%Y-%m-%d') for d in dates_series] if not dates_series.empty else [],
            'y': total_value_list,
            'type': 'scatter',
            'mode': 'lines',
            'name': '总资产',
            'line': {'color': '#667eea', 'width': 2}
        }
        
        returns_data = {
            'x': [d.strftime('%Y-%m-%d') for d in returns_pct.index] if not returns_pct.empty else [],
            'y': (cumulative_returns * 100).tolist() if not cumulative_returns.empty else [],
            'type': 'scatter',
            'mode': 'lines',
            'name': '累计收益率',
            'line': {'color': '#48bb78', 'width': 2}
        }
        
        drawdown_data = {
            'x': [d.strftime('%Y-%m-%d') for d in drawdown.index] if not drawdown.empty else [],
            'y': (drawdown * 100).tolist() if not drawdown.empty else [],
            'type': 'scatter',
            'mode': 'lines',
            'fill': 'tozeroy',
            'name': '回撤',
            'line': {'color': '#f56565', 'width': 1},
            'fillcolor': 'rgba(245, 101, 101, 0.3)'
        }
        
        # 格式化数值
        def format_currency(value):
            return f"{value:,.2f}"
        
        def format_percent(value):
            return f"{value*100:.2f}%"
        
        def format_number(value, decimals=2):
            return f"{value:,.{decimals}f}"
        
        # 生成HTML
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
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', 'Arial', 'Noto Sans', 'Liberation Sans', sans-serif;
            line-height: 1.6; 
            color: #333; 
            background-color: #f4f7f6; 
            padding: 20px; 
        }}
        article {{ 
            max-width: 1200px; 
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
        h1, h2, h3 {{ color: #2c3e50; margin-top: 0; }}
        h1 {{ font-size: 2.5em; margin-bottom: 10px; }}
        h2 {{ font-size: 1.8em; border-bottom: 2px solid #3498db; padding-bottom: 10px; margin-top: 40px; }}
        h3 {{ font-size: 1.4em; color: #34495e; margin-top: 25px; }}
        
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
        
        footer {{
            margin-top: 50px;
            padding-top: 20px;
            border-top: 2px solid #ecf0f1;
            text-align: center;
            color: #7f8c8d;
            font-size: 0.9em;
        }}
    </style>
</head>
<body>
    <article>
        <header>
            <h1>量化策略投研报告</h1>
            <p style="font-size: 1.2em; color: #7f8c8d; margin-top: 10px;">
                {strategy_name} v{strategy_version}
            </p>
            <p style="font-size: 1em; color: #95a5a6; margin-top: 5px;">
                生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            </p>
        </header>
        
        <section class="executive-summary">
            <h3>📊 执行摘要</h3>
            <p style="margin-top: 15px; line-height: 1.8;">
                本报告展示了 <strong>{strategy_name}</strong> 策略在回测期间的表现。
                策略在测试期间实现了 <strong>{format_percent(summary.get('total_profit_rate', 0))}</strong> 的总收益率，
                年化收益率为 <strong>{format_percent(metrics.get('annual_return', 0))}</strong>。
                夏普比率为 <strong>{format_number(metrics.get('sharpe_ratio', 0), 2)}</strong>，
                最大回撤为 <strong>{format_percent(metrics.get('max_drawdown', 0))}</strong>。
                策略共执行了 <strong>{metrics.get('total_trades', 0)}</strong> 笔交易。
            </p>
        </section>
        
        <h2>📈 收益指标</h2>
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
                        <td class="metric-value">{format_currency(summary.get('initial_cash', 0))} 元</td>
                    </tr>
                    <tr>
                        <td class="metric-label">最终资产</td>
                        <td class="metric-value">{format_currency(summary.get('total_value', 0))} 元</td>
                    </tr>
                    <tr>
                        <td class="metric-label">总收益</td>
                        <td class="metric-value">{format_currency(summary.get('total_profit', 0))} 元</td>
                    </tr>
                    <tr>
                        <td class="metric-label">总收益率</td>
                        <td class="metric-value">{format_percent(summary.get('total_profit_rate', 0))}</td>
                    </tr>
                    <tr>
                        <td class="metric-label">年化收益率</td>
                        <td class="metric-value">{format_percent(metrics.get('annual_return', 0))}</td>
                    </tr>
                </tbody>
            </table>
        </div>
        
        <h2>⚖️ 风险指标</h2>
        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin: 20px 0;">
            <div class="metric-card {'positive' if metrics.get('sharpe_ratio', 0) > 1 else 'negative' if metrics.get('sharpe_ratio', 0) < 0 else 'neutral'}">
                <div class="metric-label">夏普比率</div>
                <div class="metric-value">{format_number(metrics.get('sharpe_ratio', 0), 2)}</div>
            </div>
            <div class="metric-card {'negative' if metrics.get('max_drawdown', 0) > 0.2 else 'neutral'}">
                <div class="metric-label">最大回撤</div>
                <div class="metric-value">{format_percent(metrics.get('max_drawdown', 0))}</div>
            </div>
            <div class="metric-card neutral">
                <div class="metric-label">交易次数</div>
                <div class="metric-value">{metrics.get('total_trades', 0)}</div>
            </div>
        </div>
        
        <h2>📊 资产曲线</h2>
        <div class="chart-container">
            <div id="equity-chart" style="width: 100%; height: 500px;"></div>
        </div>
        
        <h2>📉 累计收益率</h2>
        <div class="chart-container">
            <div id="returns-chart" style="width: 100%; height: 400px;"></div>
        </div>
        
        <h2>📊 回撤分析</h2>
        <div class="chart-container">
            <div id="drawdown-chart" style="width: 100%; height: 400px;"></div>
        </div>
        
        {f'''
        <h2>⚙️ 策略参数</h2>
        <div class="info-section">
            <ul style="list-style: none; padding: 0;">
                {''.join([f'<li style="padding: 8px 0; border-bottom: 1px solid #ecf0f1;"><strong>{k}:</strong> {v}</li>' for k, v in (strategy_params or {}).items()])}
            </ul>
        </div>
        ''' if strategy_params else ''}
        
        <footer>
            <p>JQQuant 量化回测系统 | 报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p style="margin-top: 10px; font-size: 0.85em; color: #95a5a6;">
                本报告仅供参考，不构成投资建议。投资有风险，入市需谨慎。
            </p>
        </footer>
    </article>
    
    <script>
        // 资产曲线
        var equityData = {json.dumps([equity_data], ensure_ascii=False)};
        var equityLayout = {{
            title: {{ text: '资产曲线', font: {{ size: 18, color: '#2c3e50' }} }},
            xaxis: {{ title: '日期' }},
            yaxis: {{ title: '资产价值 (元)' }},
            hovermode: 'x unified',
            plot_bgcolor: '#f8f9fa',
            paper_bgcolor: '#ffffff'
        }};
        Plotly.newPlot('equity-chart', equityData, equityLayout, {{responsive: true}});
        
        // 累计收益率
        var returnsData = {json.dumps([returns_data], ensure_ascii=False)};
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
        var drawdownData = {json.dumps([drawdown_data], ensure_ascii=False)};
        var drawdownLayout = {{
            title: {{ text: '回撤曲线', font: {{ size: 18, color: '#2c3e50' }} }},
            xaxis: {{ title: '日期' }},
            yaxis: {{ title: '回撤 (%)' }},
            hovermode: 'x unified',
            plot_bgcolor: '#f8f9fa',
            paper_bgcolor: '#ffffff'
        }};
        Plotly.newPlot('drawdown-chart', drawdownData, drawdownLayout, {{responsive: true}});
    </script>
</body>
</html>"""
        
        # 保存文件
        if save_path:
            save_path.parent.mkdir(parents=True, exist_ok=True)
            with open(save_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            logger.info(f"HTML报告已保存至: {save_path}")
        
        return html_content
        
    except Exception as e:
        logger.error(f"生成HTML报告失败: {str(e)}", exc_info=True)
        return ""


def _calculate_drawdown(equity_curve: pd.Series) -> pd.Series:
    """计算回撤序列"""
    if len(equity_curve) == 0:
        return pd.Series()
    
    cumulative = equity_curve / equity_curve.iloc[0]
    running_max = cumulative.expanding().max()
    drawdown = (cumulative - running_max) / running_max
    return drawdown

