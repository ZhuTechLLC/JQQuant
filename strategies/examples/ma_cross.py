"""
均线交叉策略示例
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Optional
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from strategies.base_strategy import BaseStrategy
from utils.indicators import sma, ma_cross
import logging

logger = logging.getLogger(__name__)

class MACrossStrategy(BaseStrategy):
    """双均线交叉策略"""
    
    def __init__(self, short_window: int = 5, long_window: int = 20):
        """
        初始化策略
        
        Args:
            short_window: 短期均线周期
            long_window: 长期均线周期
        """
        super().__init__(name="MA Cross Strategy")
        self.short_window = short_window
        self.long_window = long_window
        # 存储每只股票的上一期均线状态，用于判断交叉
        self.prev_ma_state = {}  # {security: {'short_ma': float, 'long_ma': float}}
    
    def initialize(self, context: Dict):
        """初始化"""
        super().initialize(context)
        logger.info(f"均线交叉策略初始化: 短期={self.short_window}, 长期={self.long_window}")
    
    def handle_data(self, data: pd.DataFrame, date: datetime):
        """
        处理数据
        
        策略逻辑：
        - 当短期均线上穿长期均线时买入（金叉）
        - 当短期均线下穿长期均线时卖出（死叉）
        """
        portfolio = self.context['portfolio']
        data_provider = self.context['data_provider']
        order_manager = self.context['order_manager']
        
        # 获取可交易的股票列表
        securities = self.context.get('securities', [])
        
        if not securities:
            return
        
        # 对每只股票计算均线并判断买卖信号
        for security in securities:
            try:
                # 获取足够的历史数据来计算均线
                # 需要至少 long_window 天的数据，再加上一些缓冲
                lookback_days = max(self.long_window + 10, 60)  # 至少60天，确保有足够数据
                start_date = date - timedelta(days=lookback_days)
                
                price_data = data_provider.get_price_data(
                    securities=security,
                    start_date=start_date,
                    end_date=date,
                    frequency='daily'
                )
                
                if price_data.empty:
                    continue
                
                # 提取价格序列
                if 'security' in price_data.columns:
                    # 多股票数据格式：筛选该股票的数据
                    sec_data = price_data[price_data['security'] == security].copy()
                    if sec_data.empty:
                        continue
                    # 按日期排序
                    sec_data = sec_data.sort_index()
                    prices = sec_data['close'].values if 'close' in sec_data.columns else None
                else:
                    # 单股票数据格式
                    sec_data = price_data.sort_index()
                    prices = sec_data['close'].values if 'close' in sec_data.columns else None
                
                if prices is None or len(prices) < self.long_window:
                    logger.debug(f"{security} 历史数据不足，需要至少 {self.long_window} 天，实际 {len(prices)} 天")
                    continue
                
                # 计算均线（使用indicators模块）
                short_ma = sma(prices, self.short_window)
                long_ma = sma(prices, self.long_window)
                
                # 获取上一期的均线状态
                prev_state = self.prev_ma_state.get(security, None)
                
                # 获取持仓
                position = portfolio.get_position(security)
                has_position = position is not None and position.amount > 0
                
                # 判断均线交叉信号（使用indicators模块）
                buy_signal = False
                sell_signal = False
                
                if prev_state is not None:
                    prev_short_ma = prev_state['short_ma']
                    prev_long_ma = prev_state['long_ma']
                    
                    # 使用ma_cross函数判断交叉信号
                    golden_cross, death_cross = ma_cross(
                        short_ma, long_ma, 
                        prev_short_ma, prev_long_ma
                    )
                    
                    if golden_cross:
                        buy_signal = True
                        logger.info(f"{security} 金叉信号: 短期MA={short_ma:.2f}, 长期MA={long_ma:.2f}")
                    
                    if death_cross:
                        sell_signal = True
                        logger.info(f"{security} 死叉信号: 短期MA={short_ma:.2f}, 长期MA={long_ma:.2f}")
                
                # 执行交易
                from core.order_manager import OrderType
                
                if buy_signal and not has_position:
                    # 计算买入数量（使用可用资金的80%）
                    current_price = prices[-1]
                    available_cash = portfolio.cash * 0.8
                    amount = int(available_cash / current_price / 100) * 100  # 按100股取整
                    
                    if amount > 0:
                        order_manager.create_order(security, amount, order_type=OrderType.MARKET)
                        logger.info(f"买入信号: {security} @ {current_price:.2f}, 数量={amount}")
                
                elif sell_signal and has_position:
                    # 卖出全部持仓
                    sell_amount = -position.amount
                    order_manager.create_order(security, sell_amount, order_type=OrderType.MARKET)
                    logger.info(f"卖出信号: {security}, 数量={abs(sell_amount)}")
                
                # 更新均线状态
                self.prev_ma_state[security] = {
                    'short_ma': short_ma,
                    'long_ma': long_ma
                }
                    
            except Exception as e:
                logger.error(f"处理 {security} 时出错: {str(e)}", exc_info=True)
    
    def get_parameters(self) -> Dict:
        """获取参数"""
        return {
            'short_window': self.short_window,
            'long_window': self.long_window
        }

