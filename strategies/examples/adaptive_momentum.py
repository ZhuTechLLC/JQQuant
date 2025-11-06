# -*- coding: utf-8 -*-
"""
自适应动量策略 - 基于市场环境自动识别和策略适配
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from strategies.base_strategy import BaseStrategy
from utils.market_regime_detector import MarketRegimeDetector, MarketRegime
from core.order_manager import OrderType
import logging

logger = logging.getLogger(__name__)

class AdaptiveMomentumStrategy(BaseStrategy):
    """
    自适应动量策略
    
    核心特性：
    1. 多因素市场环境识别（趋势、风险、风格）
    2. 根据市场阶段自动调整策略参数
    3. 不同市场环境下的策略适配
    4. 动量选股 + 动态仓位管理
    """
    
    def __init__(
        self,
        benchmark: str = "000300.XSHG",
        roc_10_min: float = 0.02,
        roc_20_min: float = 0.03,
        rsi_min: float = 25,
        rsi_max: float = 95,
        volume_min: float = 0.8,
        max_positions: int = 7,
        position_size: float = 0.15,
        stop_loss: float = 0.10,
        take_profit: float = 0.50
    ):
        """
        初始化策略
        
        Args:
            benchmark: 基准指数代码
            roc_10_min: 10日ROC最小值
            roc_20_min: 20日ROC最小值
            rsi_min: RSI最小值
            rsi_max: RSI最大值
            volume_min: 成交量最小值（相对均量）
            max_positions: 最大持仓数
            position_size: 单只股票仓位比例
            stop_loss: 止损比例
            take_profit: 止盈比例
        """
        super().__init__(name="Adaptive Momentum Strategy")
        self.benchmark = benchmark
        
        # 默认参数（会根据市场环境动态调整）
        self.roc_10_min = roc_10_min
        self.roc_20_min = roc_20_min
        self.rsi_min = rsi_min
        self.rsi_max = rsi_max
        self.volume_min = volume_min
        self.max_positions = max_positions
        self.position_size = position_size
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        
        # 市场环境识别器
        self.regime_detector: Optional[MarketRegimeDetector] = None
        self.current_regime = MarketRegime.NEUTRAL_SIDEWAYS
        
        # 持仓管理
        self.positions: Dict[str, Dict] = {}  # {security: {entry_price, entry_date, ...}}
        
        # 历史数据缓存（用于计算指标）
        self._price_history: Dict[str, pd.DataFrame] = {}
    
    def initialize(self, context: Dict):
        """初始化策略"""
        super().initialize(context)
        
        # 初始化市场环境识别器
        self.regime_detector = MarketRegimeDetector(
            data_provider=context['data_provider'],
            benchmark=self.benchmark
        )
        
        logger.info(f"自适应动量策略初始化完成")
        logger.info(f"基准指数: {self.benchmark}")
        logger.info(f"默认参数: ROC10>={self.roc_10_min}, ROC20>={self.roc_20_min}, 最大持仓={self.max_positions}")
    
    def before_trading_start(self, date: datetime):
        """交易开始前更新市场环境"""
        if self.regime_detector is None:
            return
        
        try:
            # 更新市场环境识别
            new_regime = self.regime_detector.update(date)
            
            # 如果市场环境发生变化，调整策略参数
            if new_regime != self.current_regime:
                logger.info(f"市场环境变化: {self.current_regime.value} -> {new_regime.value}")
                self.current_regime = new_regime
                self._adjust_strategy_parameters()
            
            # 获取策略建议
            recommendation = self.regime_detector.get_strategy_recommendation()
            logger.debug(f"当前市场阶段: {new_regime.value}, 建议: {recommendation['description']}")
            
        except Exception as e:
            logger.error(f"更新市场环境失败: {str(e)}")
    
    def _adjust_strategy_parameters(self):
        """根据市场环境调整策略参数"""
        if self.regime_detector is None:
            return
        
        recommendation = self.regime_detector.get_strategy_recommendation()
        
        # 更新参数
        self.roc_10_min = recommendation.get('roc_10_min', self.roc_10_min)
        self.roc_20_min = recommendation.get('roc_20_min', self.roc_20_min)
        self.max_positions = recommendation.get('max_positions', self.max_positions)
        self.position_size = recommendation.get('position_size', self.position_size) * 0.15  # 转换为单只股票仓位
        self.stop_loss = recommendation.get('stop_loss', self.stop_loss)
        self.take_profit = recommendation.get('take_profit', self.take_profit)
        
        logger.info(f"策略参数已调整: ROC10>={self.roc_10_min}, ROC20>={self.roc_20_min}, "
                   f"最大持仓={self.max_positions}, 止损={self.stop_loss}, 止盈={self.take_profit}")
    
    def handle_data(self, data: pd.DataFrame, date: datetime):
        """
        处理数据
        
        策略逻辑：
        1. 根据市场环境筛选股票
        2. 计算技术指标（ROC、RSI、成交量等）
        3. 根据信号买入/卖出
        4. 执行止损止盈
        """
        portfolio = self.context['portfolio']
        data_provider = self.context['data_provider']
        order_manager = self.context['order_manager']
        securities = self.context.get('securities', [])
        
        if not securities:
            return
        
        # 恐慌熊市：清仓观望
        if self.current_regime == MarketRegime.PANIC_BEAR_MARKET:
            for security in list(self.positions.keys()):
                position = portfolio.get_position(security)
                if position and position.amount > 0:
                    order_manager.create_order(
                        security,
                        -position.amount,
                        order_type=OrderType.MARKET
                    )
                    if security in self.positions:
                        del self.positions[security]
            return
        
        # 1. 执行止损止盈
        self._check_stop_loss_take_profit(portfolio, order_manager, data, date)
        
        # 2. 筛选候选股票
        candidates = self._screen_stocks(securities, data_provider, date)
        
        # 3. 买入信号
        if len(portfolio.positions) < self.max_positions:
            # 按得分排序，买入前N只
            candidates.sort(key=lambda x: x['score'], reverse=True)
            buy_count = min(self.max_positions - len(portfolio.positions), len(candidates))
            
            for candidate in candidates[:buy_count]:
                security = candidate['security']
                position = portfolio.get_position(security)
                
                # 如果已持仓，跳过
                if position and position.amount > 0:
                    continue
                
                # 计算买入数量
                current_price = candidate['price']
                if current_price is None or current_price <= 0:
                    continue
                
                # 使用固定仓位比例
                target_value = portfolio.get_total_value() * self.position_size
                amount = int(target_value / current_price / 100) * 100  # 按手买入
                
                if amount > 0:
                    order_manager.create_order(
                        security,
                        amount,
                        order_type=OrderType.MARKET
                    )
                    # 记录持仓信息
                    self.positions[security] = {
                        'entry_price': current_price,
                        'entry_date': date,
                        'stop_loss_price': current_price * (1 - self.stop_loss),
                        'take_profit_price': current_price * (1 + self.take_profit)
                    }
                    logger.info(f"买入信号: {security} @ {current_price:.2f}, 数量={amount}")
    
    def _screen_stocks(
        self,
        securities: List[str],
        data_provider,
        date: datetime
    ) -> List[Dict]:
        """
        筛选股票
        
        Returns:
            List[Dict]: 候选股票列表，包含score、security等信息
        """
        candidates = []
        
        # 获取历史数据窗口（需要足够的历史数据计算指标）
        start_date = date - timedelta(days=60)
        
        for security in securities:
            try:
                # 获取历史价格数据
                price_data = data_provider.get_price_data(
                    securities=security,
                    start_date=start_date,
                    end_date=date,
                    frequency='daily'
                )
                
                if price_data.empty or len(price_data) < 20:
                    continue
                
                # 提取价格序列
                if 'security' in price_data.columns:
                    sec_data = price_data[price_data['security'] == security]
                    if sec_data.empty:
                        continue
                    prices = sec_data['close'].values if 'close' in sec_data.columns else None
                    volumes = sec_data['volume'].values if 'volume' in sec_data.columns else None
                else:
                    prices = price_data['close'].values if 'close' in price_data.columns else None
                    volumes = price_data['volume'].values if 'volume' in price_data.columns else None
                
                if prices is None or len(prices) < 20:
                    continue
                
                current_price = prices[-1]
                
                # 计算技术指标
                # ROC (Rate of Change)
                roc_10 = (prices[-1] / prices[-11] - 1) if len(prices) >= 11 else 0
                roc_20 = (prices[-1] / prices[-21] - 1) if len(prices) >= 21 else 0
                
                # RSI
                rsi = self._calculate_rsi(prices, period=14)
                
                # 均线
                sma_20 = np.mean(prices[-20:])
                sma_50 = np.mean(prices[-50:]) if len(prices) >= 50 else sma_20
                
                # 成交量
                volume_ratio = 1.0
                if volumes is not None and len(volumes) >= 50:
                    volume_sma = np.mean(volumes[-50:])
                    current_volume = volumes[-1]
                    volume_ratio = current_volume / volume_sma if volume_sma > 0 else 1.0
                
                # 筛选条件
                if roc_10 < self.roc_10_min:
                    continue
                if roc_20 < self.roc_20_min:
                    continue
                if volume_ratio < self.volume_min:
                    continue
                if rsi < self.rsi_min or rsi > self.rsi_max:
                    continue
                
                # 根据市场环境调整均线条件
                if self.current_regime in [MarketRegime.FULL_BULL_MARKET, 
                                          MarketRegime.GROWTH_BULL_MARKET,
                                          MarketRegime.HIGH_GROWTH_ACTIVE]:
                    # 牛市：只需在SMA50以上
                    if current_price < sma_50:
                        continue
                elif self.current_regime == MarketRegime.BEAR_MARKET_BOTTOM:
                    # 熊市末期：可以买入超跌股
                    if current_price < sma_50 * 0.95:
                        continue
                else:
                    # 其他情况：需要突破均线
                    if current_price < sma_50:
                        continue
                
                # 打分（根据市场环境调整权重）
                if self.current_regime in [MarketRegime.FULL_BULL_MARKET,
                                          MarketRegime.HIGH_GROWTH_ACTIVE]:
                    # 牛市：更重视短期动量
                    score = 0.6 * roc_10 + 0.3 * roc_20 + 0.1 * volume_ratio
                elif self.current_regime == MarketRegime.BEAR_MARKET_BOTTOM:
                    # 熊市末期：重视超跌反弹
                    score = 0.4 * roc_10 + 0.4 * roc_20 + 0.2 * volume_ratio
                else:
                    # 默认：平衡各种因子
                    score = 0.5 * roc_20 + 0.35 * roc_10 + 0.15 * volume_ratio
                
                candidates.append({
                    'security': security,
                    'score': score,
                    'price': current_price,
                    'roc_10': roc_10,
                    'roc_20': roc_20,
                    'rsi': rsi
                })
                
            except Exception as e:
                logger.debug(f"筛选 {security} 时出错: {str(e)}")
                continue
        
        return candidates
    
    def _calculate_rsi(self, prices: np.ndarray, period: int = 14) -> float:
        """计算RSI指标"""
        if len(prices) < period + 1:
            return 50.0  # 默认中性值
        
        deltas = np.diff(prices)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        avg_gain = np.mean(gains[-period:])
        avg_loss = np.mean(losses[-period:])
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    def _check_stop_loss_take_profit(
        self,
        portfolio,
        order_manager,
        data: pd.DataFrame,
        date: datetime
    ):
        """检查止损止盈"""
        for security, pos_info in list(self.positions.items()):
            position = portfolio.get_position(security)
            if not position or position.amount <= 0:
                if security in self.positions:
                    del self.positions[security]
                continue
            
            # 获取当前价格
            current_price = None
            if 'security' in data.columns:
                sec_data = data[data['security'] == security]
                if not sec_data.empty and 'close' in sec_data.columns:
                    current_price = sec_data['close'].iloc[0]
            else:
                if 'close' in data.columns:
                    current_price = data['close'].iloc[0] if len(data) == 1 else None
            
            if current_price is None:
                continue
            
            entry_price = pos_info['entry_price']
            stop_loss_price = pos_info['stop_loss_price']
            take_profit_price = pos_info['take_profit_price']
            
            # 止损
            if current_price <= stop_loss_price:
                order_manager.create_order(
                    security,
                    -position.amount,
                    order_type=OrderType.MARKET
                )
                logger.info(f"止损: {security} @ {current_price:.2f} (入场价: {entry_price:.2f})")
                del self.positions[security]
            
            # 止盈
            elif current_price >= take_profit_price:
                order_manager.create_order(
                    security,
                    -position.amount,
                    order_type=OrderType.MARKET
                )
                logger.info(f"止盈: {security} @ {current_price:.2f} (入场价: {entry_price:.2f})")
                del self.positions[security]
    
    def get_parameters(self) -> Dict:
        """获取策略参数"""
        return {
            'benchmark': self.benchmark,
            'roc_10_min': self.roc_10_min,
            'roc_20_min': self.roc_20_min,
            'rsi_min': self.rsi_min,
            'rsi_max': self.rsi_max,
            'volume_min': self.volume_min,
            'max_positions': self.max_positions,
            'position_size': self.position_size,
            'stop_loss': self.stop_loss,
            'take_profit': self.take_profit,
            'current_regime': self.current_regime.value if self.current_regime else None
        }


