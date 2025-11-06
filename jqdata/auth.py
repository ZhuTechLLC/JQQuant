"""
聚宽认证模块
"""
import jqdatasdk
from typing import Optional
import logging
from pathlib import Path
import json

logger = logging.getLogger(__name__)

def authenticate(username: Optional[str] = None, password: Optional[str] = None) -> bool:
    """
    聚宽账户认证
    
    Args:
        username: 聚宽用户名，如果为None则从配置文件读取
        password: 聚宽密码，如果为None则从配置文件读取
    
    Returns:
        bool: 认证是否成功
    """
    try:
        # 如果未提供用户名和密码，从配置文件读取
        if username is None or password is None:
            from config.config_manager import get_config_manager
            config_manager = get_config_manager()
            config = config_manager.get_jqdata_config()
            username = username or config.get('username')
            password = password or config.get('password')
        
        if not username or not password:
            logger.error("未提供聚宽账号信息")
            return False
        
        # 登录聚宽
        # jqdatasdk可能会关闭stdout/stderr，我们需要处理这种情况
        import sys
        
        # 保存原始的stdout/stderr引用
        original_stdout = getattr(sys, '__stdout__', None)
        original_stderr = getattr(sys, '__stderr__', None)
        
        # 尝试认证，即使出现I/O错误也可能已经成功
        auth_success = False
        auth_exception = None
        
        try:
            jqdatasdk.auth(username, password)
            auth_success = True
        except (ValueError, IOError, OSError) as e:
            # "I/O operation on closed file" 或其他I/O错误，但认证可能已经成功
            auth_exception = e
            if "closed file" in str(e) or "I/O" in str(e):
                # 检查是否已经认证成功
                try:
                    # 尝试获取一个简单的数据来验证认证状态
                    test_data = jqdatasdk.get_all_securities(types=['stock'], date=None)
                    if test_data is not None and len(test_data) > 0:
                        auth_success = True
                except Exception as verify_error:
                    # 如果无法验证，记录错误但继续尝试
                    logger.debug(f"验证认证状态时出错: {str(verify_error)}")
        
        # 恢复stdout/stderr（如果被关闭）
        try:
            if hasattr(sys, '__stdout__') and original_stdout:
                try:
                    sys.stdout.write('')
                except (ValueError, AttributeError, IOError):
                    sys.stdout = sys.__stdout__
        except:
            pass
        
        try:
            if hasattr(sys, '__stderr__') and original_stderr:
                try:
                    sys.stderr.write('')
                except (ValueError, AttributeError, IOError):
                    sys.stderr = sys.__stderr__
        except:
            pass
        
        if auth_success:
            if auth_exception:
                logger.info(f"聚宽认证成功（检测到I/O错误但认证已生效）: {username}")
            else:
                logger.info(f"聚宽认证成功: {username}")
            return True
        else:
            if auth_exception:
                logger.error(f"聚宽认证失败: {str(auth_exception)}")
            return False
        
    except Exception as e:
        logger.error(f"聚宽认证失败: {str(e)}")
        
        # 尝试恢复stdout/stderr
        try:
            import sys
            if hasattr(sys, '__stdout__'):
                sys.stdout = sys.__stdout__
            if hasattr(sys, '__stderr__'):
                sys.stderr = sys.__stderr__
        except:
            pass
        
        return False

def logout():
    """登出聚宽"""
    try:
        jqdatasdk.logout()
        logger.info("已登出聚宽")
    except Exception as e:
        logger.error(f"登出失败: {str(e)}")

