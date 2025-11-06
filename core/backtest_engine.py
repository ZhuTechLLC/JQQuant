"""
回测引擎核心模块
"""
import pandas as pd
from typing import Dict, List, Optional, Callable, Union
from datetime import datetime, date, timedelta
import logging
from pathlib import Path

from .data_provider import DataProvider
from .portfolio import Portfolio
from .order_manager import OrderManager
from strategies.base_strategy import BaseStrategy

logger = logging.getLogger(__name__)

class BacktestEngine:
    """回测引擎"""
    
    def __init__(
        self,
        data_provider: DataProvider,
        initial_cash: float = 1000000,
        commission_rate: float = 0.0003,
        slippage: float = 0.001
    ):
        """
        初始化回测引擎
        
        Args:
            data_provider: 数据提供者
            initial_cash: 初始资金
            commission_rate: 手续费率
            slippage: 滑点
        """
        self.data_provider = data_provider
        self.portfolio = Portfolio(initial_cash=initial_cash)
        self.order_manager = OrderManager(
            commission_rate=commission_rate,
            slippage=slippage
        )
        self.strategy: Optional[BaseStrategy] = None
        self.results = {}
    
    def set_strategy(self, strategy: BaseStrategy):
        """
        设置策略
        
        Args:
            strategy: 策略实例
        """
        self.strategy = strategy
        # 初始化策略上下文
        context = {
            'portfolio': self.portfolio,
            'data_provider': self.data_provider,
            'order_manager': self.order_manager,
            'securities': []  # 可以从配置或数据中获取
        }
        strategy.initialize(context)
    
    def run(
        self,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        securities: List[str],
        frequency: str = 'daily'
    ) -> Dict:
        """
        运行回测
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            securities: 股票代码列表
            frequency: 频率
        
        Returns:
            Dict: 回测结果
        """
        if self.strategy is None:
            raise Exception("未设置策略，请先调用set_strategy()")
        
        logger.info(f"开始回测: {start_date} to {end_date}")
        
        # 获取所有数据
        all_data = self.data_provider.get_price_data(
            securities=securities,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency
        )
        
        if all_data.empty:
            raise Exception("未获取到数据")
        
        # 更新策略上下文中的股票列表
        self.strategy.context['securities'] = securities
        
        # 按日期遍历
        # 聚宽返回的数据格式：索引为日期，可能有security列区分不同股票
        if 'security' in all_data.columns:
            # 多只股票数据，需要按日期和股票代码组织
            dates = sorted(all_data.index.unique()) if isinstance(all_data.index, pd.DatetimeIndex) else []
        else:
            # 单只股票数据
            dates = sorted(all_data.index.unique()) if isinstance(all_data.index, pd.DatetimeIndex) else []
        
        for current_date in dates:
            try:
                # 获取当日数据
                if 'security' in all_data.columns:
                    # 多只股票：筛选当日数据
                    daily_data = all_data.loc[all_data.index == current_date] if current_date in all_data.index else pd.DataFrame()
                else:
                    # 单只股票：直接获取
                    daily_data = all_data.loc[current_date] if current_date in all_data.index else pd.Series()
                
                if isinstance(daily_data, pd.Series):
                    if daily_data.empty:
                        continue
                elif isinstance(daily_data, pd.DataFrame):
                    if daily_data.empty:
                        continue
                
                # 交易开始前
                self.strategy.before_trading_start(current_date)
                
                # 处理数据（策略逻辑）
                self.strategy.handle_data(daily_data, current_date)
                
                # 处理订单
                self._process_orders(current_date, daily_data)
                
                # 更新持仓价格
                prices = {}
                for sec in securities:
                    try:
                        if 'security' in all_data.columns:
                            # 多只股票：从DataFrame中筛选该股票的数据
                            sec_data = daily_data[daily_data['security'] == sec] if isinstance(daily_data, pd.DataFrame) else None
                            if sec_data is not None and not sec_data.empty:
                                price = sec_data['close'].iloc[0] if 'close' in sec_data.columns else None
                            else:
                                price = None
                        else:
                            # 单只股票：直接从Series获取
                            if isinstance(daily_data, pd.Series):
                                price = daily_data.get('close')
                            elif isinstance(daily_data, pd.DataFrame):
                                price = daily_data['close'].iloc[0] if 'close' in daily_data.columns else None
                            else:
                                price = None
                        
                        if price is not None and not pd.isna(price):
                            prices[sec] = float(price)
                    except Exception as e:
                        logger.debug(f"获取 {sec} 价格失败: {str(e)}")
                        pass
                
                self.portfolio.update_prices(prices)
                
                # 交易结束后
                self.strategy.after_trading_end(current_date)
                
                # 记录组合状态
                self.portfolio.record(current_date)
                
            except Exception as e:
                logger.error(f"处理日期 {current_date} 时出错: {str(e)}")
                continue
        
        # 生成回测结果
        self.results = self._generate_results(start_date, end_date)
        logger.info("回测完成")
        
        return self.results
    
    def _process_orders(self, date: datetime, daily_data):
        """处理订单"""
        from .order_manager import OrderStatus
        # 获取待处理订单
        pending_orders = [o for o in self.order_manager.orders if o.status == OrderStatus.PENDING]
        
        for order in pending_orders:
            try:
                # 获取当前价格
                # daily_data可能是Series（单股票）或DataFrame（多股票）
                current_price = None
                
                if isinstance(daily_data, pd.Series):
                    # 单股票Series
                    current_price = daily_data.get('close')
                elif isinstance(daily_data, pd.DataFrame):
                    # DataFrame：可能是多股票或单股票
                    if 'security' in daily_data.columns:
                        # 多股票：查找对应股票的数据
                        sec_data = daily_data[daily_data['security'] == order.security]
                        if not sec_data.empty and 'close' in sec_data.columns:
                            current_price = sec_data['close'].iloc[0]
                    else:
                        # 单股票DataFrame
                        if 'close' in daily_data.columns:
                            current_price = daily_data['close'].iloc[0]
                
                if current_price is None or pd.isna(current_price):
                    logger.debug(f"无法获取 {order.security} 在 {date} 的价格")
                    continue
                
                # 处理订单
                if self.order_manager.process_order(order, float(current_price)):
                    # 设置成交时间
                    if order.fill_time is None:
                        order.fill_time = date
                    
                    # 更新投资组合
                    portfolio = self.portfolio
                    commission = self.order_manager.get_commission(order)
                    
                    if order.fill_amount > 0:  # 买入
                        cost = order.fill_amount * order.fill_price + commission
                        if portfolio.cash >= cost:
                            portfolio.cash -= cost
                            portfolio.add_position(
                                order.security,
                                order.fill_amount,
                                order.fill_price
                            )
                    else:  # 卖出
                        position = portfolio.get_position(order.security)
                        if position and position.amount >= abs(order.fill_amount):
                            revenue = abs(order.fill_amount) * order.fill_price - commission
                            portfolio.cash += revenue
                            portfolio.remove_position(
                                order.security,
                                abs(order.fill_amount),
                                order.fill_price
                            )
            
            except Exception as e:
                logger.error(f"处理订单失败: {str(e)}")
    
    def _generate_results(self, start_date, end_date) -> Dict:
        """生成回测结果"""
        summary = self.portfolio.get_summary()
        
        # 计算收益率曲线
        returns = pd.Series(self.portfolio.total_value_history, index=self.portfolio.date_history)
        returns_pct = returns.pct_change().fillna(0)
        
        # 计算指标
        total_return = summary['total_profit_rate']
        annual_return = (1 + total_return) ** (252 / len(returns)) - 1 if len(returns) > 0 else 0
        sharpe_ratio = returns_pct.mean() / returns_pct.std() * (252 ** 0.5) if returns_pct.std() > 0 else 0
        max_drawdown = self._calculate_max_drawdown(returns)
        
        # 获取交易历史
        filled_orders = self.order_manager.get_filled_orders()
        trade_history = []
        for order in filled_orders:
            if order.status.value == 'filled' and order.fill_time:
                trade_type = '买入' if order.fill_amount > 0 else '卖出'
                trade_value = abs(order.fill_amount * order.fill_price)
                commission = self.order_manager.get_commission(order)
                
                # 计算净现金流：
                # 买入：净流出 = -(交易金额 + 佣金)，负值表示现金流出
                # 卖出：净流入 = 交易金额 - 佣金，正值表示现金流入
                if order.fill_amount > 0:  # 买入
                    net_value = -(trade_value + commission)  # 负值表示流出
                else:  # 卖出
                    net_value = trade_value - commission  # 正值表示流入
                
                trade_history.append({
                    'date': order.fill_time.strftime('%Y-%m-%d %H:%M:%S') if isinstance(order.fill_time, datetime) else str(order.fill_time),
                    'security': order.security,
                    'type': trade_type,
                    'amount': abs(order.fill_amount),
                    'price': order.fill_price,
                    'value': trade_value,
                    'commission': commission,
                    'net_value': net_value
                })
        
        # 按日期排序
        trade_history.sort(key=lambda x: x['date'])
        
        return {
            'summary': summary,
            'returns': returns,
            'returns_pct': returns_pct,
            'metrics': {
                'total_return': total_return,
                'annual_return': annual_return,
                'sharpe_ratio': sharpe_ratio,
                'max_drawdown': max_drawdown,
                'total_trades': len(filled_orders)
            },
            'portfolio_history': {
                'dates': self.portfolio.date_history,
                'total_value': self.portfolio.total_value_history,
                'cash': self.portfolio.cash_history
            },
            'trade_history': trade_history
        }
    
    def _calculate_max_drawdown(self, returns: pd.Series) -> float:
        """计算最大回撤"""
        if len(returns) == 0:
            return 0.0
        
        cumulative = (1 + returns.pct_change().fillna(0)).cumprod()
        running_max = cumulative.expanding().max()
        drawdown = (cumulative - running_max) / running_max
        return abs(drawdown.min())

