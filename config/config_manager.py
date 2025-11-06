"""
配置管理器 - 统一管理所有API账号密码和配置
"""
import json
from pathlib import Path
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class ConfigManager:
    """配置管理器"""
    
    def __init__(self, config_dir: Optional[Path] = None):
        """
        初始化配置管理器
        
        Args:
            config_dir: 配置目录，默认为当前文件所在目录
        """
        if config_dir is None:
            config_dir = Path(__file__).parent
        self.config_dir = config_dir
        self._cache: Dict[str, Dict] = {}
    
    def load_config(self, config_name: str) -> Dict[str, Any]:
        """
        加载配置文件
        
        Args:
            config_name: 配置文件名（不含路径，如 'jqdata_config.json'）
        
        Returns:
            Dict: 配置字典
        """
        # 检查缓存
        if config_name in self._cache:
            return self._cache[config_name]
        
        config_path = self.config_dir / config_name
        
        if not config_path.exists():
            logger.warning(f"配置文件不存在: {config_path}")
            return {}
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                # 缓存配置
                self._cache[config_name] = config
                logger.info(f"加载配置成功: {config_name}")
                return config
        except Exception as e:
            logger.error(f"加载配置失败 {config_name}: {str(e)}")
            return {}
    
    def save_config(self, config_name: str, config: Dict[str, Any], update_cache: bool = True):
        """
        保存配置文件
        
        Args:
            config_name: 配置文件名
            config: 配置字典
            update_cache: 是否更新缓存
        """
        config_path = self.config_dir / config_name
        
        try:
            # 确保目录存在
            config_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            
            if update_cache:
                self._cache[config_name] = config
            
            logger.info(f"保存配置成功: {config_name}")
        except Exception as e:
            logger.error(f"保存配置失败 {config_name}: {str(e)}")
            raise
    
    def get_jqdata_config(self) -> Dict[str, Any]:
        """获取聚宽配置"""
        return self.load_config('jqdata_config.json')
    
    def update_jqdata_config(self, username: Optional[str] = None, password: Optional[str] = None, **kwargs):
        """
        更新聚宽配置
        
        Args:
            username: 用户名
            password: 密码
            **kwargs: 其他配置项
        """
        config = self.get_jqdata_config()
        
        if username is not None:
            config['username'] = username
        if password is not None:
            config['password'] = password
        
        # 更新其他配置
        for key, value in kwargs.items():
            config[key] = value
        
        self.save_config('jqdata_config.json', config)
    
    def get_config(self, service_name: str) -> Dict[str, Any]:
        """
        获取指定服务的配置（通用方法）
        
        Args:
            service_name: 服务名称，如 'tushare', 'trade' 等
        
        Returns:
            Dict: 配置字典
        """
        config_name = f"{service_name}_config.json"
        return self.load_config(config_name)
    
    def save_config_for_service(self, service_name: str, config: Dict[str, Any]):
        """
        保存指定服务的配置（通用方法）
        
        Args:
            service_name: 服务名称
            config: 配置字典
        """
        config_name = f"{service_name}_config.json"
        self.save_config(config_name, config)
    
    def clear_cache(self):
        """清空缓存"""
        self._cache.clear()
        logger.info("配置缓存已清空")

# 全局配置管理器实例
_config_manager = None

def get_config_manager() -> ConfigManager:
    """获取全局配置管理器实例"""
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigManager()
    return _config_manager

