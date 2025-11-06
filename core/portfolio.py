"""
投资组合管理模块
"""
from typing import Dict, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class Position:
    """持仓"""
    
    def __init__(self, security: str, amount: float, price: float):
        """
        初始化持仓
        
        Args:
            security: 股票代码
            amount: 持仓数量
            price: 成本价
        """
        self.security = security
        self.amount = amount
        self.cost_price = price
        self.current_price = price
    
    def update_price(self, price: float):
        """更新当前价格"""
        self.current_price = price
    
    @property
    def market_value(self) -> float:
        """市值"""
        return self.amount * self.current_price
    
    @property
    def cost_value(self) -> float:
        """成本"""
        return self.amount * self.cost_price
    
    @property
    def profit(self) -> float:
        """盈亏"""
        return self.market_value - self.cost_value
    
    @property
    def profit_rate(self) -> float:
        """盈亏比例"""
        if self.cost_value == 0:
            return 0.0
        return self.profit / self.cost_value

class Portfolio:
    """投资组合"""
    
    def __init__(self, initial_cash: float = 1000000):
        """
        初始化投资组合
        
        Args:
            initial_cash: 初始资金
        """
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.positions: Dict[str, Position] = {}
        self.total_value_history = []
        self.cash_history = []
        self.date_history = []
    
    def get_position(self, security: str) -> Optional[Position]:
        """获取持仓"""
        return self.positions.get(security)
    
    def add_position(self, security: str, amount: float, price: float):
        """
        添加持仓
        
        Args:
            security: 股票代码
            amount: 数量
            price: 价格
        """
        if security in self.positions:
            # 更新持仓
            pos = self.positions[security]
            total_cost = pos.cost_value + amount * price
            total_amount = pos.amount + amount
            pos.amount = total_amount
            pos.cost_price = total_cost / total_amount if total_amount > 0 else price
        else:
            # 新建持仓
            self.positions[security] = Position(security, amount, price)
    
    def remove_position(self, security: str, amount: float, price: float):
        """
        减少持仓
        
        Args:
            security: 股票代码
            amount: 数量
            price: 价格
        """
        if security not in self.positions:
            return
        
        pos = self.positions[security]
        if amount >= pos.amount:
            # 全部卖出
            del self.positions[security]
        else:
            # 部分卖出
            pos.amount -= amount
    
    def update_prices(self, prices: Dict[str, float]):
        """
        更新所有持仓的价格
        
        Args:
            prices: {股票代码: 价格}
        """
        for security, price in prices.items():
            if security in self.positions:
                self.positions[security].update_price(price)
    
    def get_total_value(self) -> float:
        """获取总资产"""
        positions_value = sum(pos.market_value for pos in self.positions.values())
        return self.cash + positions_value
    
    def record(self, date: datetime):
        """记录当前状态"""
        self.date_history.append(date)
        self.cash_history.append(self.cash)
        self.total_value_history.append(self.get_total_value())
    
    def get_summary(self) -> Dict:
        """获取组合摘要"""
        return {
            'initial_cash': self.initial_cash,
            'current_cash': self.cash,
            'total_value': self.get_total_value(),
            'total_profit': self.get_total_value() - self.initial_cash,
            'total_profit_rate': (self.get_total_value() - self.initial_cash) / self.initial_cash,
            'positions_count': len(self.positions),
            'positions': {sec: {
                'amount': pos.amount,
                'cost_price': pos.cost_price,
                'current_price': pos.current_price,
                'profit': pos.profit,
                'profit_rate': pos.profit_rate
            } for sec, pos in self.positions.items()}
        }

