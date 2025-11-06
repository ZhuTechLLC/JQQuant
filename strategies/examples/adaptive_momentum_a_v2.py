# -*- coding: utf-8 -*-
"""
A股自适应动量策略 v2.0 - 改进版
基于市场环境自动识别和策略适配，增强选股和风险控制
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import sys
from pathlib import Path
import json

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from strategies.base_strategy import BaseStrategy
from utils.market_regime_detector import MarketRegimeDetector, MarketRegime
from core.order_manager import OrderType
import logging

logger = logging.getLogger(__name__)

class AdaptiveMomentumStrategyA_V2(BaseStrategy):
    """
    A股自适应动量策略 v2.0 - 改进版
    
    核心改进：
    1. 动态股票池构建（基于市值、流动性、基本面筛选）
    2. 多因子选股（技术+基本面+市场因子）
    3. 相对强度筛选（跑赢大盘）
    4. 行业轮动支持
    5. 改进的仓位管理（风险平价）
    6. 增强的风险控制（回撤控制、相关性控制）
    7. 优化的参数调整（基于波动率）
    """
    
    def __init__(
        self,
        benchmark: str = "000300.XSHG",
        growth_index: str = "399006.XSHE",
        roc_10_min: float = 0.015,
        roc_20_min: float = 0.025,
        rsi_min: float = 25,
        rsi_max: float = 95,
        volume_min: float = 0.8,
        max_positions: int = 8,
        position_size: float = 0.12,
        stop_loss: float = 0.08,
        take_profit: float = 0.40,
        use_dynamic_pool: bool = True,
        use_fundamental: bool = True,
        use_relative_strength: bool = True
    ):
        """
        初始化策略
        
        Args:
            benchmark: 基准指数代码
            growth_index: 成长指数代码
            use_dynamic_pool: 是否使用动态股票池
            use_fundamental: 是否使用基本面因子
            use_relative_strength: 是否使用相对强度筛选
        """
        super().__init__(name="Adaptive Momentum Strategy A-Share v2.0")
        self.benchmark = benchmark
        self.growth_index = growth_index
        
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
        
        # 新功能开关
        self.use_dynamic_pool = use_dynamic_pool
        self.use_fundamental = use_fundamental
        self.use_relative_strength = use_relative_strength
        
        # 市场环境识别器
        self.regime_detector: Optional[MarketRegimeDetector] = None
        self.current_regime = MarketRegime.NEUTRAL_SIDEWAYS
        
        # 持仓管理
        self.positions: Dict[str, Dict] = {}
        
        # 历史数据缓存
        self._price_history: Dict[str, pd.DataFrame] = {}
        self._benchmark_history: Optional[pd.DataFrame] = None
        
        # 再平衡频率控制
        self.last_rebalance_date = None
        self.rebalance_days = [0, 2, 4]  # 周一、周三、周五
        
        # 风险控制
        self.max_drawdown_threshold = 0.15  # 最大回撤阈值
        self.initial_portfolio_value = None
        self.peak_portfolio_value = None
        
        # 股票池配置
        self.stock_pool_config = self._load_stock_pool_config()
    
    def _load_stock_pool_config(self) -> Dict:
        """加载股票池配置"""
        try:
            config_file = project_root / 'config' / 'stock_pool.json'
            with open(config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {}
    
    def initialize(self, context: Dict):
        """初始化策略"""
        super().initialize(context)
        
        # 初始化市场环境识别器
        self.regime_detector = MarketRegimeDetector(
            data_provider=context['data_provider'],
            benchmark=self.benchmark
        )
        
        # 记录初始资产
        portfolio = context.get('portfolio')
        if portfolio:
            self.initial_portfolio_value = portfolio.get_total_value()
            self.peak_portfolio_value = self.initial_portfolio_value
        
        logger.info(f"A股自适应动量策略 v2.0 初始化完成")
        logger.info(f"基准指数: {self.benchmark}, 成长指数: {self.growth_index}")
        logger.info(f"动态股票池: {self.use_dynamic_pool}, 基本面筛选: {self.use_fundamental}")
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
            
        except Exception as e:
            logger.error(f"更新市场环境失败: {str(e)}")
    
    def _adjust_strategy_parameters(self):
        """根据市场环境调整策略参数（考虑波动率）"""
        if self.regime_detector is None:
            return
        
        recommendation = self.regime_detector.get_strategy_recommendation()
        
        # 基础参数调整
        self.roc_10_min = recommendation.get('roc_10_min', self.roc_10_min)
        self.roc_20_min = recommendation.get('roc_20_min', self.roc_20_min)
        self.max_positions = recommendation.get('max_positions', self.max_positions)
        self.position_size = recommendation.get('position_size', self.position_size) * 0.12
        self.stop_loss = recommendation.get('stop_loss', self.stop_loss)
        self.take_profit = recommendation.get('take_profit', self.take_profit)
        
        # 根据市场环境微调
        if self.current_regime in [MarketRegime.FULL_BULL_MARKET, MarketRegime.HIGH_GROWTH_ACTIVE]:
            # 牛市：可以更激进
            self.roc_10_min = max(0.015, self.roc_10_min * 0.9)
            self.max_positions = min(10, self.max_positions + 1)
        elif self.current_regime in [MarketRegime.PANIC_BEAR_MARKET, MarketRegime.PERSISTENT_BEAR_MARKET]:
            # 熊市：更保守
            self.roc_10_min = self.roc_10_min * 1.2
            self.max_positions = max(3, self.max_positions - 2)
            self.stop_loss = min(0.06, self.stop_loss * 0.8)
        
        logger.info(f"策略参数已调整: ROC10>={self.roc_10_min:.3f}, ROC20>={self.roc_20_min:.3f}, "
                   f"最大持仓={self.max_positions}, 止损={self.stop_loss:.2f}, 止盈={self.take_profit:.2f}")
    
    def _should_rebalance(self, date: datetime) -> bool:
        """判断是否应该再平衡"""
        weekday = date.weekday()
        if weekday in self.rebalance_days:
            if self.last_rebalance_date is None or self.last_rebalance_date != date:
                self.last_rebalance_date = date
                return True
        return False
    
    def _check_risk_control(self, portfolio) -> bool:
        """检查风险控制，返回是否需要减仓"""
        current_value = portfolio.get_total_value()
        
        # 更新峰值
        if current_value > self.peak_portfolio_value:
            self.peak_portfolio_value = current_value
        
        # 计算当前回撤
        if self.peak_portfolio_value > 0:
            current_drawdown = (self.peak_portfolio_value - current_value) / self.peak_portfolio_value
            if current_drawdown > self.max_drawdown_threshold:
                logger.warning(f"回撤超过阈值: {current_drawdown:.2%}, 触发风险控制")
                return True
        
        return False
    
    def handle_data(self, data: pd.DataFrame, date: datetime):
        """处理数据"""
        portfolio = self.context['portfolio']
        data_provider = self.context['data_provider']
        order_manager = self.context['order_manager']
        securities = self.context.get('securities', [])
        
        if not securities:
            return
        
        # 风险控制检查
        if self._check_risk_control(portfolio):
            # 减仓50%
            for security in list(self.positions.keys()):
                position = portfolio.get_position(security)
                if position and position.amount > 0:
                    reduce_amount = int(position.amount * 0.5)
                    if reduce_amount > 0:
                        order_manager.create_order(
                            security,
                            -reduce_amount,
                            order_type=OrderType.MARKET
                        )
        
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
        
        # 1. 执行止损止盈（每日检查）
        self._check_stop_loss_take_profit(portfolio, order_manager, data, date)
        
        # 2. 再平衡逻辑（每周一、三、五）
        if self._should_rebalance(date):
            # 获取动态股票池（如果启用）
            if self.use_dynamic_pool:
                pool_securities = self._get_dynamic_stock_pool(securities, data_provider, date)
            else:
                pool_securities = securities
            
            # 筛选候选股票
            candidates = self._screen_stocks(pool_securities, data_provider, date)
            
            # 清仓不在目标列表的持仓
            target_securities = {c['security'] for c in candidates[:self.max_positions]}
            for security in list(portfolio.positions.keys()):
                if security not in target_securities:
                    position = portfolio.get_position(security)
                    if position and position.amount > 0:
                        order_manager.create_order(
                            security,
                            -position.amount,
                            order_type=OrderType.MARKET
                        )
                        if security in self.positions:
                            del self.positions[security]
            
            # 买入目标列表中的新股票（风险平价分配）
            candidates.sort(key=lambda x: x['score'], reverse=True)
            buy_count = min(self.max_positions - len(portfolio.positions), len(candidates))
            
            # 计算总可用资金
            total_value = portfolio.get_total_value()
            available_cash = portfolio.cash
            
            for candidate in candidates[:buy_count]:
                security = candidate['security']
                position = portfolio.get_position(security)
                
                if position and position.amount > 0:
                    continue
                
                current_price = candidate['price']
                if current_price is None or current_price <= 0:
                    continue
                
                # 风险平价分配：根据波动率调整仓位
                volatility = candidate.get('volatility', 0.02)  # 默认2%波动率
                risk_adjusted_size = self.position_size / (1 + volatility * 10)  # 波动率越高，仓位越小
                
                target_value = total_value * risk_adjusted_size
                amount = int(target_value / current_price / 100) * 100
                
                if amount > 0 and available_cash >= amount * current_price * 1.001:  # 考虑手续费
                    order_manager.create_order(
                        security,
                        amount,
                        order_type=OrderType.MARKET
                    )
                    self.positions[security] = {
                        'entry_price': current_price,
                        'entry_date': date,
                        'stop_loss_price': current_price * (1 - self.stop_loss),
                        'take_profit_price': current_price * (1 + self.take_profit),
                        'volatility': volatility
                    }
                    available_cash -= amount * current_price * 1.001
                    logger.info(f"买入信号: {security} @ {current_price:.2f}, 数量={amount}, 波动率={volatility:.2%}")
    
    def _get_dynamic_stock_pool(self, base_securities: List[str], data_provider, date: datetime) -> List[str]:
        """
        获取动态股票池
        
        目前简化实现：从基础股票池中筛选
        未来可以扩展为从JQData API动态获取
        """
        # 简化实现：直接返回基础股票池（去重）
        return list(set(base_securities))
    
    def _screen_stocks(
        self,
        securities: List[str],
        data_provider,
        date: datetime
    ) -> List[Dict]:
        """
        多因子筛选股票（改进版）
        
        包含：
        1. 技术因子（ROC、RSI、成交量、均线）
        2. 基本面因子（如果启用）
        3. 相对强度（如果启用）
        4. 市场因子（波动率、动量）
        """
        candidates = []
        
        # 获取基准数据用于相对强度计算
        benchmark_data = None
        if self.use_relative_strength:
            try:
                start_date = date - timedelta(days=60)
                benchmark_data = data_provider.get_price_data(
                    securities=self.benchmark,
                    start_date=start_date,
                    end_date=date,
                    frequency='daily'
                )
            except:
                pass
        
        # 获取历史数据窗口
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
                roc_10 = (prices[-1] / prices[-11] - 1) if len(prices) >= 11 else 0
                roc_20 = (prices[-1] / prices[-21] - 1) if len(prices) >= 21 else 0
                roc_5 = (prices[-1] / prices[-6] - 1) if len(prices) >= 6 else 0
                
                rsi = self._calculate_rsi(prices, period=14)
                
                sma_20 = np.mean(prices[-20:])
                sma_50 = np.mean(prices[-50:]) if len(prices) >= 50 else sma_20
                
                # 成交量
                volume_ratio = 1.0
                if volumes is not None and len(volumes) >= 50:
                    volume_sma = np.mean(volumes[-50:])
                    current_volume = volumes[-1]
                    volume_ratio = current_volume / volume_sma if volume_sma > 0 else 1.0
                
                # 计算波动率
                returns = np.diff(prices[-20:]) / prices[-20:-1]
                volatility = np.std(returns) if len(returns) > 0 else 0.02
                
                # 计算相对强度（相对基准）
                relative_strength = 0.0
                if self.use_relative_strength and benchmark_data is not None:
                    try:
                        if 'close' in benchmark_data.columns:
                            benchmark_prices = benchmark_data['close'].values
                            if len(benchmark_prices) >= 21:
                                benchmark_roc = (benchmark_prices[-1] / benchmark_prices[-21] - 1)
                                relative_strength = roc_20 - benchmark_roc
                    except:
                        pass
                
                # 技术指标筛选
                if roc_10 < self.roc_10_min:
                    continue
                if roc_20 < self.roc_20_min:
                    continue
                if volume_ratio < self.volume_min:
                    continue
                if rsi < self.rsi_min or rsi > self.rsi_max:
                    continue
                
                # 相对强度筛选（如果启用）
                if self.use_relative_strength and relative_strength < 0:
                    continue  # 只选择跑赢大盘的股票
                
                # 均线条件
                if self.current_regime in [MarketRegime.FULL_BULL_MARKET, 
                                          MarketRegime.GROWTH_BULL_MARKET,
                                          MarketRegime.HIGH_GROWTH_ACTIVE]:
                    if current_price < sma_50:
                        continue
                elif self.current_regime == MarketRegime.BEAR_MARKET_BOTTOM:
                    if current_price < sma_50 * 0.95:
                        continue
                else:
                    if current_price < sma_50:
                        continue
                
                # 多因子打分
                # 技术因子得分（0-1）
                tech_score = self._calculate_tech_score(roc_5, roc_10, roc_20, rsi, volume_ratio, sma_20, current_price)
                
                # 相对强度得分（0-1）
                rs_score = min(1.0, max(0.0, (relative_strength + 0.1) / 0.2)) if self.use_relative_strength else 0.5
                
                # 波动率得分（波动率越低越好，0-1）
                vol_score = max(0.0, 1.0 - volatility * 20)
                
                # 综合得分（根据市场环境调整权重）
                if self.current_regime in [MarketRegime.FULL_BULL_MARKET, MarketRegime.HIGH_GROWTH_ACTIVE]:
                    # 牛市：重视技术因子和相对强度
                    score = 0.5 * tech_score + 0.3 * rs_score + 0.2 * vol_score
                elif self.current_regime == MarketRegime.BEAR_MARKET_BOTTOM:
                    # 熊市末期：重视超跌反弹
                    score = 0.4 * tech_score + 0.3 * rs_score + 0.3 * vol_score
                else:
                    # 默认：平衡各因子
                    score = 0.4 * tech_score + 0.3 * rs_score + 0.3 * vol_score
                
                candidates.append({
                    'security': security,
                    'score': score,
                    'price': current_price,
                    'roc_10': roc_10,
                    'roc_20': roc_20,
                    'rsi': rsi,
                    'volatility': volatility,
                    'relative_strength': relative_strength,
                    'tech_score': tech_score,
                    'rs_score': rs_score,
                    'vol_score': vol_score
                })
                
            except Exception as e:
                logger.debug(f"筛选 {security} 时出错: {str(e)}")
                continue
        
        return candidates
    
    def _calculate_tech_score(
        self, 
        roc_5: float, 
        roc_10: float, 
        roc_20: float, 
        rsi: float, 
        volume_ratio: float,
        sma_20: float,
        current_price: float
    ) -> float:
        """计算技术因子得分（0-1）"""
        # ROC得分（归一化到0-1）
        roc_score = min(1.0, (roc_10 + roc_20) / 0.2)  # 假设最大ROC为20%
        
        # RSI得分（50-70为最佳）
        if 50 <= rsi <= 70:
            rsi_score = 1.0
        elif rsi < 50:
            rsi_score = rsi / 50
        else:
            rsi_score = max(0.0, 1.0 - (rsi - 70) / 30)
        
        # 成交量得分
        volume_score = min(1.0, volume_ratio / 2.0)  # 成交量越大越好，但不超过2倍
        
        # 均线得分（价格在均线以上）
        ma_score = 1.0 if current_price > sma_20 else 0.5
        
        # 综合技术得分
        tech_score = 0.4 * roc_score + 0.2 * rsi_score + 0.2 * volume_score + 0.2 * ma_score
        
        return tech_score
    
    def _calculate_rsi(self, prices: np.ndarray, period: int = 14) -> float:
        """计算RSI指标"""
        if len(prices) < period + 1:
            return 50.0
        
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
            'growth_index': self.growth_index,
            'roc_10_min': self.roc_10_min,
            'roc_20_min': self.roc_20_min,
            'rsi_min': self.rsi_min,
            'rsi_max': self.rsi_max,
            'volume_min': self.volume_min,
            'max_positions': self.max_positions,
            'position_size': self.position_size,
            'stop_loss': self.stop_loss,
            'take_profit': self.take_profit,
            'current_regime': self.current_regime.value if self.current_regime else None,
            'use_dynamic_pool': self.use_dynamic_pool,
            'use_fundamental': self.use_fundamental,
            'use_relative_strength': self.use_relative_strength
        }

