# -*- coding: utf-8 -*-
"""
从回测结果JSON文件生成HTML报告
"""
import sys
import io
import json
from pathlib import Path
from datetime import datetime

# 设置标准输出为UTF-8编码（Windows）
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

sys.path.insert(0, str(Path(__file__).parent))

from utils.report_generator import generate_html_report
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_report_from_json(json_file: Path):
    """从JSON文件生成HTML报告"""
    try:
        # 读取JSON文件
        with open(json_file, 'r', encoding='utf-8') as f:
            results_dict = json.load(f)
        
        # 检查是否有portfolio_history数据
        if 'portfolio_history' not in results_dict or not results_dict['portfolio_history']:
            logger.warning("JSON文件中缺少portfolio_history数据，无法生成完整报告")
            logger.info("请运行新的回测以获取完整数据")
            return
        
        # 转换数据格式
        portfolio_history = results_dict['portfolio_history']
        dates = portfolio_history.get('dates', [])
        total_value = portfolio_history.get('total_value', [])
        
        # 计算returns_pct
        returns_pct = pd.Series()
        if dates and total_value:
            dates_series = pd.to_datetime(dates)
            total_value_series = pd.Series(total_value, index=dates_series)
            if len(total_value_series) > 1:
                returns_pct = total_value_series.pct_change().fillna(0)
        
        # 构建results字典
        results = {
            'summary': results_dict.get('summary', {}),
            'metrics': results_dict.get('metrics', {}),
            'portfolio_history': portfolio_history,
            'returns_pct': returns_pct
        }
        
        # 生成报告
        strategy_name = results_dict.get('strategy', 'ma_cross')
        html_file = json_file.parent / f"{strategy_name}_v1.0_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        
        # 提取策略参数（如果有）
        strategy_params = {}
        if 'short_window' in results_dict:
            strategy_params['short_window'] = results_dict['short_window']
        if 'long_window' in results_dict:
            strategy_params['long_window'] = results_dict['long_window']
        
        generate_html_report(
            results=results,
            strategy_name=strategy_name,
            strategy_version="1.0.0",
            strategy_params=strategy_params if strategy_params else None,
            save_path=html_file
        )
        
        logger.info(f"HTML报告已生成: {html_file}")
        
        # 自动更新报告列表
        try:
            from update_report_list import update_report_list
            update_report_list()
            logger.info("报告列表已自动更新")
        except Exception as e:
            logger.warning(f"更新报告列表失败: {str(e)}")
        
        return html_file
        
    except Exception as e:
        logger.error(f"生成报告失败: {str(e)}", exc_info=True)
        return None

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='从JSON文件生成HTML报告')
    parser.add_argument('json_file', type=str, nargs='?', 
                       help='JSON文件路径（可选，默认使用最新的回测结果）')
    
    args = parser.parse_args()
    
    if args.json_file:
        json_file = Path(args.json_file)
    else:
        # 查找最新的回测结果文件
        results_dir = Path(__file__).parent / 'results'
        json_files = list(results_dir.glob('backtest_*.json'))
        if not json_files:
            logger.error("未找到回测结果JSON文件")
            sys.exit(1)
        json_file = max(json_files, key=lambda p: p.stat().st_mtime)
        logger.info(f"使用最新的回测结果: {json_file}")
    
    if not json_file.exists():
        logger.error(f"文件不存在: {json_file}")
        sys.exit(1)
    
    generate_report_from_json(json_file)

