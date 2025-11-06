# -*- coding: utf-8 -*-
"""
市场环境识别器 - 简化版（适配聚宽）
基于多因素市场环境识别框架
"""
from enum import Enum
from typing import Dict, Optional
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

class MarketRegime(Enum):
    """市场阶段枚举"""
    # 牛市阶段
    FULL_BULL_MARKET = "FullBullMarket"              # 全面牛市
    STRUCTURAL_BULL_MARKET = "StructuralBullMarket" # 结构性牛市/后期
    GROWTH_BULL_MARKET = "GrowthBullMarket"         # 成长股活跃期
    
    # 熊市阶段
    PANIC_BEAR_MARKET = "PanicBearMarket"           # 熊市恐慌阶段
    PERSISTENT_BEAR_MARKET = "PersistentBearMarket" # 持续熊市
    BEAR_MARKET_BOTTOM = "BearMarketBottom"         # 熊市末期筑底
    
    # 震荡市
    NEUTRAL_SIDEWAYS = "NeutralSideways"            # 中性震荡市
    WAITING_BREAKOUT = "WaitingBreakout"           # 等待突破
    TRANSITION_PHASE = "TransitionPhase"           # 牛熊转换盘整期
    
    # 特殊阶段
    INDUSTRY_ROTATION = "IndustryRotation"         # 行业轮动市
    HIGH_GROWTH_ACTIVE = "HighGrowthActive"         # 高增长板块活跃期

class TrendState(Enum):
    """趋势状态"""
    BULLISH = "Bullish"
    BEARISH = "Bearish"
    SIDEWAYS = "Sideways"

class RiskMode(Enum):
    """风险模式"""
    NORMAL = "Normal"
    ELEVATED = "Elevated"
    PANIC = "Panic"

class MarketRegimeDetector:
    """
    市场环境识别器 - 简化版
    
    基于以下因素识别市场环境：
    1. 趋势判定层：基准指数与均线关系
    2. 风险判定层：波动率指标
    3. 风格轮动层：成长vs价值
    """
    
    def __init__(self, data_provider=None, benchmark: str = "000300.XSHG"):
        """
        初始化市场环境识别器
        
        Args:
            data_provider: 数据提供者
            benchmark: 基准指数代码（默认沪深300）
        """
        self.data_provider = data_provider
        self.benchmark = benchmark
        self.current_regime = MarketRegime.NEUTRAL_SIDEWAYS
        self.trend_state = TrendState.SIDEWAYS
        self.risk_mode = RiskMode.NORMAL
        
        # 历史数据缓存
        self._benchmark_data = None
        self._last_update_date = None
    
    def update(self, date: datetime, benchmark_data: Optional[pd.DataFrame] = None) -> MarketRegime:
        """
        更新市场环境识别
        
        Args:
            date: 当前日期
            benchmark_data: 基准指数数据（可选）
        
        Returns:
            MarketRegime: 当前市场阶段
        """
        try:
            # 获取基准指数数据
            if benchmark_data is None and self.data_provider:
                # 获取最近60天的数据用于计算指标
                start_date = date - timedelta(days=60)
                benchmark_data = self.data_provider.get_price_data(
                    securities=self.benchmark,
                    start_date=start_date,
                    end_date=date,
                    frequency='daily'
                )
            
            if benchmark_data is None or benchmark_data.empty:
                return self.current_regime
            
            # 提取收盘价
            if 'security' in benchmark_data.columns:
                # 多股票数据
                bench_data = benchmark_data[benchmark_data['security'] == self.benchmark]
                if bench_data.empty:
                    return self.current_regime
                prices = bench_data['close'].values if 'close' in bench_data.columns else None
            else:
                # 单股票数据
                prices = benchmark_data['close'].values if 'close' in benchmark_data.columns else None
            
            if prices is None or len(prices) < 20:
                return self.current_regime
            
            # 1. 趋势判定层
            current_price = prices[-1]
            ma20 = np.mean(prices[-20:])
            ma50 = np.mean(prices[-50:]) if len(prices) >= 50 else ma20
            
            # 判断趋势方向
            if current_price > ma50 * 1.05:  # 价格明显高于50日均线
                self.trend_state = TrendState.BULLISH
            elif current_price < ma50 * 0.95:  # 价格明显低于50日均线
                self.trend_state = TrendState.BEARISH
            else:
                self.trend_state = TrendState.SIDEWAYS
            
            # 计算波动率（简化版，使用价格标准差）
            returns = np.diff(prices) / prices[:-1]
            volatility = np.std(returns[-20:]) * np.sqrt(252)  # 年化波动率
            
            # 2. 风险判定层
            if volatility > 0.35:  # 高波动
                self.risk_mode = RiskMode.PANIC
            elif volatility > 0.25:  # 中等波动
                self.risk_mode = RiskMode.ELEVATED
            else:
                self.risk_mode = RiskMode.NORMAL
            
            # 3. 综合判断市场阶段
            self.current_regime = self._determine_regime()
            
            self._last_update_date = date
            return self.current_regime
            
        except Exception as e:
            logger.error(f"市场环境识别失败: {str(e)}")
            return self.current_regime
    
    def _determine_regime(self) -> MarketRegime:
        """根据趋势和风险状态确定市场阶段"""
        # 牛市阶段
        if self.trend_state == TrendState.BULLISH:
            if self.risk_mode == RiskMode.NORMAL:
                return MarketRegime.FULL_BULL_MARKET
            elif self.risk_mode == RiskMode.ELEVATED:
                return MarketRegime.STRUCTURAL_BULL_MARKET
            else:
                return MarketRegime.GROWTH_BULL_MARKET
        
        # 熊市阶段
        elif self.trend_state == TrendState.BEARISH:
            if self.risk_mode == RiskMode.PANIC:
                return MarketRegime.PANIC_BEAR_MARKET
            elif self.risk_mode == RiskMode.ELEVATED:
                return MarketRegime.PERSISTENT_BEAR_MARKET
            else:
                return MarketRegime.BEAR_MARKET_BOTTOM
        
        # 震荡市
        else:
            if self.risk_mode == RiskMode.PANIC:
                return MarketRegime.TRANSITION_PHASE
            elif self.risk_mode == RiskMode.ELEVATED:
                return MarketRegime.WAITING_BREAKOUT
            else:
                return MarketRegime.NEUTRAL_SIDEWAYS
    
    def get_strategy_recommendation(self) -> Dict:
        """
        根据当前市场阶段返回策略建议
        
        Returns:
            Dict: 策略建议字典
        """
        recommendations = {
            MarketRegime.FULL_BULL_MARKET: {
                "strategy_type": "TrendFollowing",
                "position_size": 1.0,
                "description": "全面牛市：趋势跟随策略，满仓做多成长股",
                "roc_10_min": 0.02,
                "roc_20_min": 0.03,
                "max_positions": 7,
                "stop_loss": 0.12,
                "take_profit": 0.60
            },
            MarketRegime.STRUCTURAL_BULL_MARKET: {
                "strategy_type": "DefensiveRotation",
                "position_size": 0.8,
                "description": "结构性牛市：降低仓位，防御轮动",
                "roc_10_min": 0.03,
                "roc_20_min": 0.05,
                "max_positions": 5,
                "stop_loss": 0.08,
                "take_profit": 0.40
            },
            MarketRegime.GROWTH_BULL_MARKET: {
                "strategy_type": "Momentum",
                "position_size": 0.9,
                "description": "成长股活跃期：动量策略",
                "roc_10_min": 0.02,
                "roc_20_min": 0.04,
                "max_positions": 7,
                "stop_loss": 0.10,
                "take_profit": 0.55
            },
            MarketRegime.PANIC_BEAR_MARKET: {
                "strategy_type": "Defensive",
                "position_size": 0.1,
                "description": "熊市恐慌阶段：大幅减仓，持有现金或防御资产",
                "roc_10_min": 0.05,
                "roc_20_min": 0.08,
                "max_positions": 2,
                "stop_loss": 0.05,
                "take_profit": 0.20
            },
            MarketRegime.PERSISTENT_BEAR_MARKET: {
                "strategy_type": "Defensive",
                "position_size": 0.2,
                "description": "持续熊市：大幅减仓",
                "roc_10_min": 0.05,
                "roc_20_min": 0.08,
                "max_positions": 2,
                "stop_loss": 0.05,
                "take_profit": 0.20
            },
            MarketRegime.BEAR_MARKET_BOTTOM: {
                "strategy_type": "BottomFishing",
                "position_size": 0.5,
                "description": "熊市末期：分批抄底",
                "roc_10_min": 0.025,
                "roc_20_min": 0.04,
                "max_positions": 5,
                "stop_loss": 0.08,
                "take_profit": 0.35
            },
            MarketRegime.NEUTRAL_SIDEWAYS: {
                "strategy_type": "MeanReversion",
                "position_size": 0.6,
                "description": "震荡市：均值回归策略",
                "roc_10_min": 0.03,
                "roc_20_min": 0.05,
                "max_positions": 5,
                "stop_loss": 0.08,
                "take_profit": 0.30
            },
            MarketRegime.WAITING_BREAKOUT: {
                "strategy_type": "Breakout",
                "position_size": 0.7,
                "description": "等待突破：突破策略",
                "roc_10_min": 0.03,
                "roc_20_min": 0.05,
                "max_positions": 5,
                "stop_loss": 0.08,
                "take_profit": 0.35
            },
            MarketRegime.TRANSITION_PHASE: {
                "strategy_type": "Defensive",
                "position_size": 0.3,
                "description": "牛熊转换盘整期：谨慎观望",
                "roc_10_min": 0.04,
                "roc_20_min": 0.06,
                "max_positions": 3,
                "stop_loss": 0.06,
                "take_profit": 0.25
            },
            MarketRegime.INDUSTRY_ROTATION: {
                "strategy_type": "Rotation",
                "position_size": 0.8,
                "description": "行业轮动：灵活切换",
                "roc_10_min": 0.025,
                "roc_20_min": 0.04,
                "max_positions": 6,
                "stop_loss": 0.09,
                "take_profit": 0.40
            },
            MarketRegime.HIGH_GROWTH_ACTIVE: {
                "strategy_type": "Momentum",
                "position_size": 0.9,
                "description": "高增长板块活跃期：强势追涨，严格止盈",
                "roc_10_min": 0.015,
                "roc_20_min": 0.025,
                "max_positions": 8,
                "stop_loss": 0.12,
                "take_profit": 0.70
            }
        }
        
        return recommendations.get(self.current_regime, recommendations[MarketRegime.NEUTRAL_SIDEWAYS])

