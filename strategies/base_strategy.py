"""
策略基类
"""
from abc import ABC, abstractmethod
from typing import Dict, Optional
from datetime import datetime
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class BaseStrategy(ABC):
    """策略基类，所有策略都应继承此类"""
    
    def __init__(self, name: str = "BaseStrategy"):
        """
        初始化策略
        
        Args:
            name: 策略名称
        """
        self.name = name
        self.initialized = False
        self.context: Optional[Dict] = None
    
    def initialize(self, context: Dict):
        """
        初始化策略（在回测开始时调用一次）
        
        Args:
            context: 策略上下文，包含数据提供者、投资组合等信息
        """
        self.context = context
        self.initialized = True
        logger.info(f"策略 {self.name} 初始化完成")
    
    @abstractmethod
    def handle_data(self, data: pd.DataFrame, date: datetime):
        """
        处理数据（每个交易日调用）
        
        Args:
            data: 当前可用的市场数据
            date: 当前日期
        """
        pass
    
    def before_trading_start(self, date: datetime):
        """
        交易开始前调用（可选）
        
        Args:
            date: 当前日期
        """
        pass
    
    def after_trading_end(self, date: datetime):
        """
        交易结束后调用（可选）
        
        Args:
            date: 当前日期
        """
        pass
    
    def get_parameters(self) -> Dict:
        """
        获取策略参数（用于优化和记录）
        
        Returns:
            Dict: 参数字典
        """
        return {}
    
    def set_parameters(self, params: Dict):
        """
        设置策略参数
        
        Args:
            params: 参数字典
        """
        pass

