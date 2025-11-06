# -*- coding: utf-8 -*-
"""
数据提供者模块
"""
import pandas as pd
from typing import List, Optional, Union
from datetime import datetime, date
import logging
from pathlib import Path
import pickle
import hashlib
import json

logger = logging.getLogger(__name__)

class DataProvider:
    """数据提供者，负责获取和管理市场数据"""
    
    def __init__(self, jq_client=None, cache_dir: Optional[Path] = None, use_disk_cache: bool = True):
        """
        初始化数据提供者
        
        Args:
            jq_client: 聚宽客户端实例
            cache_dir: 缓存目录
            use_disk_cache: 是否使用磁盘缓存
        """
        self.jq_client = jq_client
        self.cache_dir = cache_dir or Path(__file__).parent.parent / "data"
        self.cache_dir.mkdir(exist_ok=True)
        self.use_disk_cache = use_disk_cache
        self._cache = {}  # 内存缓存
    
    def get_price_data(
        self,
        securities: Union[str, List[str]],
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        frequency: str = 'daily',
        use_cache: bool = True
    ) -> pd.DataFrame:
        """
        获取价格数据（带缓存）
        
        Args:
            securities: 股票代码或代码列表
            start_date: 开始日期
            end_date: 结束日期
            frequency: 频率
            use_cache: 是否使用缓存
        
        Returns:
            DataFrame: 价格数据，索引为日期，列为股票代码，值为价格数据
        """
        # 生成缓存键
        if isinstance(securities, str):
            securities = [securities]
        securities = sorted(securities)
        cache_key = f"{'_'.join(securities)}_{start_date}_{end_date}_{frequency}"
        
        # 检查内存缓存
        if use_cache and cache_key in self._cache:
            logger.debug(f"使用内存缓存数据: {cache_key}")
            return self._cache[cache_key]
        
        # 检查磁盘缓存
        if use_cache and self.use_disk_cache:
            cached_data = self._load_from_disk_cache(cache_key)
            if cached_data is not None:
                logger.debug(f"使用磁盘缓存数据: {cache_key}")
                self._cache[cache_key] = cached_data  # 同时加载到内存
                return cached_data
        
        # 从聚宽获取数据
        if self.jq_client is None:
            raise Exception("未设置聚宽客户端，无法获取数据")
        
        logger.info(f"从聚宽API获取数据: {securities}, {start_date} to {end_date}")
        data = self.jq_client.get_price(
            securities=securities,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency
        )
        
        # 缓存数据
        if use_cache:
            self._cache[cache_key] = data
            if self.use_disk_cache:
                self._save_to_disk_cache(cache_key, data)
        
        return data
    
    def _get_cache_file_path(self, cache_key: str) -> Path:
        """获取缓存文件路径"""
        # 使用hash避免文件名过长
        cache_hash = hashlib.md5(cache_key.encode()).hexdigest()
        return self.cache_dir / f"cache_{cache_hash}.pkl"
    
    def _save_to_disk_cache(self, cache_key: str, data: pd.DataFrame):
        """保存数据到磁盘缓存"""
        try:
            cache_file = self._get_cache_file_path(cache_key)
            # 保存数据
            data.to_pickle(cache_file)
            # 保存元数据
            metadata_file = cache_file.with_suffix('.json')
            metadata = {
                'cache_key': cache_key,
                'timestamp': datetime.now().isoformat(),
                'shape': list(data.shape),
                'columns': list(data.columns) if isinstance(data.columns, pd.Index) else []
            }
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            logger.debug(f"数据已保存到磁盘缓存: {cache_file}")
        except Exception as e:
            logger.warning(f"保存磁盘缓存失败: {str(e)}")
    
    def _load_from_disk_cache(self, cache_key: str) -> Optional[pd.DataFrame]:
        """从磁盘缓存加载数据"""
        try:
            cache_file = self._get_cache_file_path(cache_key)
            if not cache_file.exists():
                return None
            
            # 检查元数据
            metadata_file = cache_file.with_suffix('.json')
            if metadata_file.exists():
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
                # 验证缓存键是否匹配
                if metadata.get('cache_key') != cache_key:
                    logger.debug(f"缓存键不匹配，删除旧缓存文件")
                    cache_file.unlink(missing_ok=True)
                    metadata_file.unlink(missing_ok=True)
                    return None
            
            # 加载数据
            data = pd.read_pickle(cache_file)
            return data
        except Exception as e:
            logger.warning(f"加载磁盘缓存失败: {str(e)}")
            return None
    
    def get_current_price(self, security: str, date: datetime) -> Optional[float]:
        """
        获取指定日期的价格
        
        Args:
            security: 股票代码
            date: 日期
        
        Returns:
            float: 收盘价
        """
        try:
            data = self.get_price_data(
                securities=security,
                start_date=date,
                end_date=date,
                frequency='daily'
            )
            if not data.empty:
                return data.loc[date, (security, 'close')] if isinstance(data.columns, pd.MultiIndex) else data.loc[date, 'close']
        except Exception as e:
            logger.error(f"获取价格失败 {security} @ {date}: {str(e)}")
        return None
    
    def clear_cache(self, clear_disk: bool = False):
        """
        清空缓存
        
        Args:
            clear_disk: 是否同时清空磁盘缓存
        """
        self._cache.clear()
        logger.info("内存缓存已清空")
        
        if clear_disk and self.use_disk_cache:
            try:
                # 删除所有缓存文件
                cache_files = list(self.cache_dir.glob("cache_*.pkl"))
                metadata_files = list(self.cache_dir.glob("cache_*.json"))
                for f in cache_files + metadata_files:
                    f.unlink(missing_ok=True)
                logger.info(f"磁盘缓存已清空，删除了 {len(cache_files)} 个缓存文件")
            except Exception as e:
                logger.warning(f"清空磁盘缓存失败: {str(e)}")
    
    def get_cache_info(self) -> dict:
        """
        获取缓存信息
        
        Returns:
            缓存统计信息
        """
        info = {
            'memory_cache_size': len(self._cache),
            'disk_cache_enabled': self.use_disk_cache
        }
        
        if self.use_disk_cache:
            try:
                cache_files = list(self.cache_dir.glob("cache_*.pkl"))
                total_size = sum(f.stat().st_size for f in cache_files)
                info['disk_cache_files'] = len(cache_files)
                info['disk_cache_size_mb'] = total_size / (1024 * 1024)
            except Exception as e:
                logger.warning(f"获取磁盘缓存信息失败: {str(e)}")
                info['disk_cache_files'] = 0
                info['disk_cache_size_mb'] = 0
        
        return info

