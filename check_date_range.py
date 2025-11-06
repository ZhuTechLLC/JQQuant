# -*- coding: utf-8 -*-
"""
检查聚宽账号允许的日期范围
"""
import sys
import io
from pathlib import Path

# 设置标准输出为UTF-8编码（Windows）
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

sys.path.insert(0, str(Path(__file__).parent))

from jqdata.client import JQDataClient
from config.config_manager import get_config_manager
from datetime import datetime, timedelta

def check_account_date_range():
    """检查账号允许的日期范围"""
    print("=" * 60)
    print("检查聚宽账号日期范围权限")
    print("=" * 60)
    
    # 初始化客户端
    config_manager = get_config_manager()
    config = config_manager.get_jqdata_config()
    jq_client = JQDataClient()
    
    if not config.get('username') or not config.get('password'):
        print("错误: 未找到聚宽账号配置")
        return
    
    if not jq_client.authenticate(config['username'], config['password']):
        print("错误: 聚宽认证失败")
        return
    
    print(f"账号: {config['username']}")
    print("正在测试日期范围...")
    print()
    
    # 尝试不同的日期范围来找到允许的范围
    test_dates = [
        ('2024-07-29', '2024-08-05'),  # 根据错误信息
        ('2024-01-01', '2024-12-31'),
        ('2023-01-01', '2023-12-31'),
        ('2025-01-01', '2025-12-31'),
    ]
    
    working_range = None
    
    for start, end in test_dates:
        try:
            print(f"测试: {start} 至 {end}...", end=' ')
            data = jq_client.get_price(
                securities='000001.XSHE',
                start_date=start,
                end_date=end,
                frequency='daily'
            )
            if not data.empty:
                print(f"✓ 成功 (获取到 {len(data)} 条数据)")
                if working_range is None:
                    working_range = (start, end)
            else:
                print("✗ 无数据")
        except Exception as e:
            error_msg = str(e)
            if "账号权限仅能获取" in error_msg or "权限仅能获取" in error_msg:
                import re
                date_pattern = r'(\d{4}-\d{2}-\d{2})'
                dates = re.findall(date_pattern, error_msg)
                if len(dates) >= 2:
                    print(f"✗ 权限限制: 允许范围 {dates[0]} 至 {dates[1]}")
                    if working_range is None:
                        working_range = (dates[0], dates[1])
                else:
                    print(f"✗ 错误: {error_msg[:50]}")
            else:
                print(f"✗ 错误: {error_msg[:50]}")
    
    print()
    print("=" * 60)
    if working_range:
        print(f"建议使用的日期范围: {working_range[0]} 至 {working_range[1]}")
        print()
        print("示例命令:")
        print(f'python main.py --strategy ma_cross --start {working_range[0]} --end {working_range[1]} --securities 000001.XSHE')
    else:
        print("无法确定允许的日期范围，请查看上面的错误信息")
    print("=" * 60)
    
    # 登出
    try:
        from jqdata.auth import logout
        logout()
    except:
        pass

if __name__ == '__main__':
    check_account_date_range()

