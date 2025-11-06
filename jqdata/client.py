"""
聚宽API客户端
"""
import jqdatasdk as jq
import pandas as pd
from typing import List, Optional, Union
from datetime import datetime, date
import logging
from .auth import authenticate

logger = logging.getLogger(__name__)

class JQDataClient:
    """聚宽数据API客户端"""
    
    def __init__(self, username: Optional[str] = None, password: Optional[str] = None):
        """
        初始化客户端
        
        Args:
            username: 聚宽用户名
            password: 聚宽密码
        """
        self._authenticated = False
        if username and password:
            self.authenticate(username, password)
    
    def authenticate(self, username: str, password: str) -> bool:
        """认证"""
        self._authenticated = authenticate(username, password)
        return self._authenticated
    
    def is_authenticated(self) -> bool:
        """检查是否已认证"""
        return self._authenticated
    
    def get_price(
        self,
        securities: Union[str, List[str]],
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        frequency: str = 'daily',
        fields: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        获取价格数据
        
        Args:
            securities: 股票代码或代码列表
            start_date: 开始日期
            end_date: 结束日期
            frequency: 频率 ('daily', '1m', '5m', '15m', '30m', '60m')
            fields: 字段列表，默认 ['open', 'close', 'high', 'low', 'volume']
        
        Returns:
            DataFrame: 价格数据
        """
        if not self._authenticated:
            raise Exception("未认证，请先调用authenticate()")
        
        if fields is None:
            fields = ['open', 'close', 'high', 'low', 'volume']
        
        try:
            # 聚宽API: get_price的参数是security(单只)或count，不是securities
            # 对于多只股票，需要分别获取或使用其他方法
            if isinstance(securities, str):
                # 单只股票
                data = jq.get_price(
                    security=securities,
                    start_date=start_date,
                    end_date=end_date,
                    frequency=frequency,
                    fields=fields
                )
            else:
                # 多只股票：聚宽API需要分别获取或使用get_bars
                # 这里使用循环获取每只股票的数据，然后合并
                all_data = []
                for sec in securities:
                    sec_data = jq.get_price(
                        security=sec,
                        start_date=start_date,
                        end_date=end_date,
                        frequency=frequency,
                        fields=fields
                    )
                    # 添加股票代码列以便区分
                    sec_data['security'] = sec
                    all_data.append(sec_data)
                
                # 合并数据
                if all_data:
                    data = pd.concat(all_data, ignore_index=False)
                    # 如果索引是日期，保持日期索引；否则重置索引
                    if isinstance(all_data[0].index, pd.DatetimeIndex):
                        data = data.sort_index()
                else:
                    data = pd.DataFrame()
            
            logger.info(f"获取价格数据成功: {securities}, {start_date} to {end_date}")
            return data
        except Exception as e:
            error_msg = str(e)
            logger.error(f"获取价格数据失败: {error_msg}")
            
            # 检查是否是账号权限限制错误
            if "账号权限仅能获取" in error_msg or "权限仅能获取" in error_msg:
                # 提取允许的日期范围
                import re
                date_pattern = r'(\d{4}-\d{2}-\d{2})'
                dates = re.findall(date_pattern, error_msg)
                if len(dates) >= 2:
                    allowed_start = dates[0]
                    allowed_end = dates[1]
                    raise ValueError(
                        f"账号权限限制：\n"
                        f"  您请求的日期范围: {start_date} 至 {end_date}\n"
                        f"  账号允许的日期范围: {allowed_start} 至 {allowed_end}\n"
                        f"  请调整日期参数后重试。\n"
                        f"  示例命令: python main.py --strategy ma_cross --start {allowed_start} --end {allowed_end} --securities {' '.join(securities if isinstance(securities, list) else [securities])}"
                    )
            
            raise
    
    def get_all_securities(self, types: List[str] = ['stock'], date: Optional[str] = None) -> pd.DataFrame:
        """
        获取所有证券信息
        
        Args:
            types: 证券类型列表 ['stock', 'fund', 'index', 'futures', 'etf', 'lof', 'fja', 'fjb']
            date: 日期，默认为当前日期
        
        Returns:
            DataFrame: 证券信息
        """
        if not self._authenticated:
            raise Exception("未认证，请先调用authenticate()")
        
        try:
            data = jq.get_all_securities(types=types, date=date)
            logger.info(f"获取证券信息成功: {types}")
            return data
        except Exception as e:
            logger.error(f"获取证券信息失败: {str(e)}")
            raise
    
    def get_index_stocks(self, index_symbol: str, date: Optional[str] = None) -> List[str]:
        """
        获取指数成分股
        
        Args:
            index_symbol: 指数代码，如 '000300.XSHG' (沪深300)
            date: 日期
        
        Returns:
            List[str]: 股票代码列表
        """
        if not self._authenticated:
            raise Exception("未认证，请先调用authenticate()")
        
        try:
            stocks = jq.get_index_stocks(index_symbol, date=date)
            logger.info(f"获取指数成分股成功: {index_symbol}, 共{len(stocks)}只股票")
            return stocks
        except Exception as e:
            logger.error(f"获取指数成分股失败: {str(e)}")
            raise
    
    def get_fundamentals(
        self,
        query,
        date: Optional[Union[str, date, datetime]] = None,
        statDate: Optional[str] = None
    ) -> pd.DataFrame:
        """
        获取财务数据
        
        Args:
            query: 查询对象
            date: 日期
            statDate: 统计日期
        
        Returns:
            DataFrame: 财务数据
        """
        if not self._authenticated:
            raise Exception("未认证，请先调用authenticate()")
        
        try:
            data = jq.get_fundamentals(query, date=date, statDate=statDate)
            logger.info("获取财务数据成功")
            return data
        except Exception as e:
            logger.error(f"获取财务数据失败: {str(e)}")
            raise

