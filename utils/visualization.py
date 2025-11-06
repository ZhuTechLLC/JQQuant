# -*- coding: utf-8 -*-
"""
可视化模块
提供回测结果的可视化功能
"""
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import numpy as np
from typing import Dict, Optional, List
from pathlib import Path
import logging

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

logger = logging.getLogger(__name__)

def plot_backtest_results(results: Dict, save_path: Optional[Path] = None, show: bool = True):
    """
    绘制回测结果（增强版）
    
    Args:
        results: 回测结果字典
        save_path: 保存路径
        show: 是否显示图表
    """
    try:
        portfolio_history = results.get('portfolio_history', {})
        dates = portfolio_history.get('dates', [])
        total_value = portfolio_history.get('total_value', [])
        cash = portfolio_history.get('cash', [])
        
        if not dates or not total_value:
            logger.warning("没有足够的数据进行可视化")
            return
        
        # 转换为pandas Series以便处理
        dates_series = pd.to_datetime(dates) if dates else pd.DatetimeIndex([])
        total_value_series = pd.Series(total_value, index=dates_series)
        cash_series = pd.Series(cash, index=dates_series) if cash else pd.Series()
        
        # 计算收益率
        returns_pct = results.get('returns_pct', pd.Series())
        if returns_pct.empty and len(total_value_series) > 1:
            returns_pct = total_value_series.pct_change().fillna(0)
        
        # 创建图表 - 2x2布局
        fig = plt.figure(figsize=(16, 10))
        gs = fig.add_gridspec(3, 2, hspace=0.3, wspace=0.3)
        
        # 1. 资产曲线（包含现金和总资产）
        ax1 = fig.add_subplot(gs[0, :])
        ax1.plot(dates_series, total_value_series, label='总资产', linewidth=2, color='#667eea')
        if not cash_series.empty:
            ax1.plot(dates_series, cash_series, label='现金', linewidth=1.5, color='#48bb78', alpha=0.7)
        ax1.set_title('回测资产曲线', fontsize=14, fontweight='bold', pad=10)
        ax1.set_xlabel('日期', fontsize=11)
        ax1.set_ylabel('资产价值 (元)', fontsize=11)
        ax1.legend(loc='best', fontsize=10)
        ax1.grid(True, alpha=0.3)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        # 2. 累计收益率曲线
        ax2 = fig.add_subplot(gs[1, 0])
        if not returns_pct.empty:
            cumulative_returns = (1 + returns_pct).cumprod() - 1
            ax2.plot(returns_pct.index, cumulative_returns * 100, 
                    label='累计收益率', linewidth=2, color='#48bb78')
            ax2.axhline(y=0, color='r', linestyle='--', alpha=0.5)
            ax2.set_title('累计收益率曲线', fontsize=12, fontweight='bold')
            ax2.set_xlabel('日期', fontsize=10)
            ax2.set_ylabel('收益率 (%)', fontsize=10)
            ax2.legend(fontsize=9)
            ax2.grid(True, alpha=0.3)
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        # 3. 回撤曲线
        ax3 = fig.add_subplot(gs[1, 1])
        drawdown = _calculate_drawdown(total_value_series)
        if not drawdown.empty:
            ax3.fill_between(drawdown.index, drawdown * 100, 0, 
                            alpha=0.3, color='red', label='回撤')
            ax3.plot(drawdown.index, drawdown * 100, linewidth=1.5, color='red')
            ax3.set_title('回撤曲线', fontsize=12, fontweight='bold')
            ax3.set_xlabel('日期', fontsize=10)
            ax3.set_ylabel('回撤 (%)', fontsize=10)
            ax3.legend(fontsize=9)
            ax3.grid(True, alpha=0.3)
            ax3.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        # 4. 收益率分布
        ax4 = fig.add_subplot(gs[2, 0])
        if not returns_pct.empty:
            returns_pct_clean = returns_pct.dropna()
            if len(returns_pct_clean) > 0:
                ax4.hist(returns_pct_clean * 100, bins=50, alpha=0.7, color='#4299e1', edgecolor='black')
                ax4.axvline(x=0, color='r', linestyle='--', alpha=0.5)
                ax4.set_title('日收益率分布', fontsize=12, fontweight='bold')
                ax4.set_xlabel('日收益率 (%)', fontsize=10)
                ax4.set_ylabel('频数', fontsize=10)
                ax4.grid(True, alpha=0.3, axis='y')
        
        # 5. 性能指标表格
        ax5 = fig.add_subplot(gs[2, 1])
        ax5.axis('off')
        metrics = results.get('metrics', {})
        summary = results.get('summary', {})
        
        # 准备指标数据
        metrics_text = [
            ['指标', '数值'],
            ['初始资金', f"{summary.get('initial_cash', 0):,.2f} 元"],
            ['最终资产', f"{summary.get('total_value', 0):,.2f} 元"],
            ['总收益', f"{summary.get('total_profit', 0):,.2f} 元"],
            ['总收益率', f"{summary.get('total_profit_rate', 0)*100:.2f}%"],
            ['年化收益', f"{metrics.get('annual_return', 0)*100:.2f}%"],
            ['夏普比率', f"{metrics.get('sharpe_ratio', 0):.2f}"],
            ['最大回撤', f"{metrics.get('max_drawdown', 0)*100:.2f}%"],
            ['交易次数', f"{metrics.get('total_trades', 0)}"],
        ]
        
        table = ax5.table(cellText=metrics_text[1:], 
                          colLabels=metrics_text[0],
                          cellLoc='left',
                          loc='center',
                          colWidths=[0.5, 0.5])
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1, 2)
        
        # 设置表格样式
        for i in range(len(metrics_text[0])):
            table[(0, i)].set_facecolor('#667eea')
            table[(0, i)].set_text_props(weight='bold', color='white')
        
        ax5.set_title('性能指标', fontsize=12, fontweight='bold', pad=20)
        
        plt.suptitle('回测结果分析报告', fontsize=16, fontweight='bold', y=0.995)
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            logger.info(f"图表已保存至: {save_path}")
        
        if show:
            plt.show()
        else:
            plt.close()
        
    except Exception as e:
        logger.error(f"绘制图表失败: {str(e)}", exc_info=True)


def _calculate_drawdown(equity_curve: pd.Series) -> pd.Series:
    """
    计算回撤序列
    
    Args:
        equity_curve: 资产曲线
    
    Returns:
        回撤序列（负数表示回撤）
    """
    if len(equity_curve) == 0:
        return pd.Series()
    
    cumulative = equity_curve / equity_curve.iloc[0]
    running_max = cumulative.expanding().max()
    drawdown = (cumulative - running_max) / running_max
    return drawdown


def plot_comparison(strategies_results: List[Dict], labels: Optional[List[str]] = None, 
                   save_path: Optional[Path] = None, show: bool = True):
    """
    对比多个策略的回测结果
    
    Args:
        strategies_results: 多个策略的回测结果列表
        labels: 策略标签列表
        save_path: 保存路径
        show: 是否显示图表
    """
    try:
        if not strategies_results:
            logger.warning("没有策略结果可对比")
            return
        
        if labels is None:
            labels = [f"策略{i+1}" for i in range(len(strategies_results))]
        
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        
        # 1. 资产曲线对比
        ax1 = axes[0, 0]
        colors = plt.cm.tab10(np.linspace(0, 1, len(strategies_results)))
        for i, (result, label) in enumerate(zip(strategies_results, labels)):
            portfolio_history = result.get('portfolio_history', {})
            dates = portfolio_history.get('dates', [])
            total_value = portfolio_history.get('total_value', [])
            if dates and total_value:
                dates_series = pd.to_datetime(dates)
                ax1.plot(dates_series, total_value, label=label, linewidth=2, color=colors[i])
        ax1.set_title('资产曲线对比', fontsize=12, fontweight='bold')
        ax1.set_xlabel('日期', fontsize=10)
        ax1.set_ylabel('资产价值 (元)', fontsize=10)
        ax1.legend(fontsize=9)
        ax1.grid(True, alpha=0.3)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        # 2. 收益率对比
        ax2 = axes[0, 1]
        metrics_list = []
        for result in strategies_results:
            metrics = result.get('metrics', {})
            metrics_list.append(metrics.get('total_return', 0) * 100)
        bars = ax2.bar(range(len(labels)), metrics_list, color=colors[:len(labels)])
        ax2.set_title('总收益率对比', fontsize=12, fontweight='bold')
        ax2.set_xlabel('策略', fontsize=10)
        ax2.set_ylabel('收益率 (%)', fontsize=10)
        ax2.set_xticks(range(len(labels)))
        ax2.set_xticklabels(labels, rotation=45, ha='right')
        ax2.axhline(y=0, color='r', linestyle='--', alpha=0.5)
        ax2.grid(True, alpha=0.3, axis='y')
        
        # 添加数值标签
        for bar in bars:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.2f}%', ha='center', va='bottom', fontsize=9)
        
        # 3. 夏普比率对比
        ax3 = axes[1, 0]
        sharpe_list = []
        for result in strategies_results:
            metrics = result.get('metrics', {})
            sharpe_list.append(metrics.get('sharpe_ratio', 0))
        bars = ax3.bar(range(len(labels)), sharpe_list, color=colors[:len(labels)])
        ax3.set_title('夏普比率对比', fontsize=12, fontweight='bold')
        ax3.set_xlabel('策略', fontsize=10)
        ax3.set_ylabel('夏普比率', fontsize=10)
        ax3.set_xticks(range(len(labels)))
        ax3.set_xticklabels(labels, rotation=45, ha='right')
        ax3.axhline(y=0, color='r', linestyle='--', alpha=0.5)
        ax3.grid(True, alpha=0.3, axis='y')
        
        # 添加数值标签
        for bar in bars:
            height = bar.get_height()
            ax3.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.2f}', ha='center', va='bottom', fontsize=9)
        
        # 4. 最大回撤对比
        ax4 = axes[1, 1]
        drawdown_list = []
        for result in strategies_results:
            metrics = result.get('metrics', {})
            drawdown_list.append(metrics.get('max_drawdown', 0) * 100)
        bars = ax4.bar(range(len(labels)), drawdown_list, color=colors[:len(labels)])
        ax4.set_title('最大回撤对比', fontsize=12, fontweight='bold')
        ax4.set_xlabel('策略', fontsize=10)
        ax4.set_ylabel('最大回撤 (%)', fontsize=10)
        ax4.set_xticks(range(len(labels)))
        ax4.set_xticklabels(labels, rotation=45, ha='right')
        ax4.grid(True, alpha=0.3, axis='y')
        
        # 添加数值标签
        for bar in bars:
            height = bar.get_height()
            ax4.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.2f}%', ha='center', va='top', fontsize=9)
        
        plt.suptitle('策略对比分析', fontsize=14, fontweight='bold')
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            logger.info(f"对比图表已保存至: {save_path}")
        
        if show:
            plt.show()
        else:
            plt.close()
        
    except Exception as e:
        logger.error(f"绘制对比图表失败: {str(e)}", exc_info=True)

