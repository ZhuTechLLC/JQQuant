"""
订单管理模块
"""
from typing import Optional, List
from datetime import datetime
from enum import Enum
import logging

logger = logging.getLogger(__name__)

class OrderType(Enum):
    """订单类型"""
    MARKET = "market"  # 市价单
    LIMIT = "limit"    # 限价单

class OrderStatus(Enum):
    """订单状态"""
    PENDING = "pending"      # 待成交
    FILLED = "filled"        # 已成交
    CANCELLED = "cancelled"  # 已取消
    REJECTED = "rejected"    # 已拒绝

class Order:
    """订单"""
    
    def __init__(
        self,
        security: str,
        amount: float,
        order_type: OrderType = OrderType.MARKET,
        price: Optional[float] = None
    ):
        """
        初始化订单
        
        Args:
            security: 股票代码
            amount: 数量（正数为买入，负数为卖出）
            order_type: 订单类型
            price: 价格（限价单需要）
        """
        self.security = security
        self.amount = amount
        self.order_type = order_type
        self.price = price
        self.status = OrderStatus.PENDING
        self.create_time = datetime.now()
        self.fill_time: Optional[datetime] = None
        self.fill_price: Optional[float] = None
        self.fill_amount: float = 0.0
    
    def fill(self, price: float, amount: Optional[float] = None, time: Optional[datetime] = None):
        """
        成交订单
        
        Args:
            price: 成交价格
            amount: 成交数量（默认全部成交）
            time: 成交时间
        """
        self.fill_price = price
        self.fill_amount = amount if amount is not None else self.amount
        self.fill_time = time or datetime.now()
        self.status = OrderStatus.FILLED
        logger.info(f"订单成交: {self.security} {self.fill_amount}@{self.fill_price}")
    
    def cancel(self):
        """取消订单"""
        self.status = OrderStatus.CANCELLED
        logger.info(f"订单取消: {self.security}")

class OrderManager:
    """订单管理器"""
    
    def __init__(self, commission_rate: float = 0.0003, slippage: float = 0.001):
        """
        初始化订单管理器
        
        Args:
            commission_rate: 手续费率
            slippage: 滑点
        """
        self.commission_rate = commission_rate
        self.slippage = slippage
        self.orders: List[Order] = []
        self.filled_orders: List[Order] = []
    
    def create_order(
        self,
        security: str,
        amount: float,
        order_type: Optional[OrderType] = None,
        price: Optional[float] = None
    ) -> Order:
        """
        创建订单
        
        Args:
            security: 股票代码
            amount: 数量
            order_type: 订单类型（默认MARKET）
            price: 价格
        
        Returns:
            Order: 订单对象
        """
        if order_type is None:
            order_type = OrderType.MARKET
        order = Order(security, amount, order_type, price)
        self.orders.append(order)
        logger.info(f"创建订单: {security} {amount} {order_type.value}")
        return order
    
    def process_order(self, order: Order, current_price: float) -> bool:
        """
        处理订单（模拟成交）
        
        Args:
            order: 订单
            current_price: 当前价格
        
        Returns:
            bool: 是否成交
        """
        if order.status != OrderStatus.PENDING:
            return False
        
        # 计算成交价格（考虑滑点）
        if order.order_type == OrderType.MARKET:
            # 市价单：考虑滑点
            if order.amount > 0:  # 买入
                fill_price = current_price * (1 + self.slippage)
            else:  # 卖出
                fill_price = current_price * (1 - self.slippage)
        else:
            # 限价单：检查价格
            if order.price is None:
                order.status = OrderStatus.REJECTED
                return False
            
            if order.amount > 0:  # 买入限价单
                if current_price <= order.price:
                    fill_price = current_price * (1 + self.slippage)
                else:
                    return False  # 价格不够低，不成交
            else:  # 卖出限价单
                if current_price >= order.price:
                    fill_price = current_price * (1 - self.slippage)
                else:
                    return False  # 价格不够高，不成交
        
        # 成交
        order.fill(fill_price)
        self.filled_orders.append(order)
        return True
    
    def get_commission(self, order: Order) -> float:
        """
        计算手续费
        
        Args:
            order: 订单
        
        Returns:
            float: 手续费
        """
        if order.status != OrderStatus.FILLED:
            return 0.0
        return abs(order.fill_amount * order.fill_price * self.commission_rate)
    
    def get_all_orders(self) -> List[Order]:
        """获取所有订单"""
        return self.orders
    
    def get_filled_orders(self) -> List[Order]:
        """获取已成交订单"""
        return self.filled_orders

