# -*- coding: utf-8 -*-
"""
更新报告列表页面，自动扫描所有可用的回测报告
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

def get_strategy_name_cn(strategy_name: str) -> str:
    """获取策略中文名称"""
    strategy_names = {
        'ma_cross': '均线交叉策略',
        'adaptive_momentum': '自适应动量策略'
    }
    return strategy_names.get(strategy_name, strategy_name)

def update_report_list():
    """更新报告列表页面"""
    results_dir = Path(__file__).parent / 'results'
    
    # 扫描所有JSON文件
    json_files = sorted(results_dir.glob('backtest_*.json'), key=lambda p: p.stat().st_mtime, reverse=True)
    
    # 扫描所有HTML报告文件
    html_files = sorted(results_dir.glob('*_report*.html'), key=lambda p: p.stat().st_mtime, reverse=True)
    
    # 解析JSON文件信息
    json_reports = []
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            strategy_name = data.get('strategy', 'unknown')
            start_date = data.get('start_date', '')
            end_date = data.get('end_date', '')
            has_history = 'portfolio_history' in data and data['portfolio_history']
            
            json_reports.append({
                'file': json_file.name,
                'strategy': strategy_name,
                'strategy_cn': get_strategy_name_cn(strategy_name),
                'start_date': start_date,
                'end_date': end_date,
                'has_history': has_history,
                'summary': data.get('summary', {}),
                'metrics': data.get('metrics', {})
            })
        except Exception as e:
            print(f"读取 {json_file} 失败: {e}")
    
    # 解析HTML文件信息
    html_reports = []
    for html_file in html_files:
        if html_file.name == 'view_report.html' or html_file.name == 'report_template.html':
            continue
        
        # 从文件名提取信息
        # 文件名格式: {strategy}_v1.0_report_{timestamp}.html
        name_parts = html_file.stem.split('_')
        strategy_name = None
        
        # 尝试匹配策略名称
        if 'adaptive' in html_file.name.lower() and 'momentum' in html_file.name.lower():
            strategy_name = 'adaptive_momentum'
        elif 'ma' in html_file.name.lower() and 'cross' in html_file.name.lower():
            strategy_name = 'ma_cross'
        elif len(name_parts) >= 1:
            # 如果包含下划线，取第一部分
            potential_name = name_parts[0]
            if potential_name in ['ma', 'adaptive']:
                # 尝试从JSON文件中查找匹配的策略名称
                for json_report in json_reports:
                    if json_report['file'].startswith(f'backtest_{potential_name}'):
                        strategy_name = json_report['strategy']
                        break
                if not strategy_name:
                    strategy_name = potential_name
        
        if strategy_name:
            html_reports.append({
                'file': html_file.name,
                'strategy': strategy_name,
                'strategy_cn': get_strategy_name_cn(strategy_name)
            })
        else:
            # 如果无法识别，仍然添加到列表
            html_reports.append({
                'file': html_file.name,
                'strategy': 'unknown',
                'strategy_cn': '未知策略'
            })
    
    # 生成HTML内容
    html_content = '''<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>查看回测报告</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Microsoft YaHei', sans-serif;
            max-width: 1000px;
            margin: 50px auto;
            padding: 20px;
            background: #f5f7fa;
        }
        .container {
            background: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        h1 {
            color: #667eea;
            margin-bottom: 10px;
        }
        .subtitle {
            color: #666;
            margin-bottom: 30px;
            font-size: 0.9em;
        }
        .section {
            margin-bottom: 40px;
        }
        .section-title {
            color: #764ba2;
            font-size: 1.3em;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 2px solid #e0e0e0;
        }
        .file-list {
            list-style: none;
            padding: 0;
        }
        .file-item {
            padding: 20px;
            margin: 15px 0;
            background: #f8f9fa;
            border-radius: 8px;
            border-left: 4px solid #667eea;
            transition: transform 0.2s, box-shadow 0.2s;
        }
        .file-item:hover {
            transform: translateX(5px);
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }
        .file-item.no-history {
            border-left-color: #ffa500;
            opacity: 0.8;
        }
        .file-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 10px;
        }
        .file-item a {
            color: #667eea;
            text-decoration: none;
            font-weight: bold;
            font-size: 1.1em;
        }
        .file-item a:hover {
            text-decoration: underline;
        }
        .file-info {
            color: #666;
            font-size: 0.9em;
            margin-top: 8px;
            line-height: 1.6;
        }
        .file-metrics {
            display: flex;
            gap: 20px;
            margin-top: 10px;
            flex-wrap: wrap;
        }
        .metric-badge {
            background: #e8e8e8;
            padding: 4px 10px;
            border-radius: 4px;
            font-size: 0.85em;
        }
        .metric-badge.positive {
            background: #d4edda;
            color: #155724;
        }
        .metric-badge.negative {
            background: #f8d7da;
            color: #721c24;
        }
        .warning-badge {
            background: #fff3cd;
            color: #856404;
            padding: 4px 10px;
            border-radius: 4px;
            font-size: 0.85em;
            display: inline-block;
            margin-left: 10px;
        }
        .empty-state {
            text-align: center;
            padding: 40px;
            color: #999;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 回测报告列表</h1>
        <p class="subtitle">自动扫描并列出所有可用的回测报告</p>
        
        <div class="section">
            <h2 class="section-title">📄 JSON数据文件（可通过模板查看）</h2>
            <ul class="file-list" id="jsonList">
'''
    
    # 添加JSON报告列表
    if json_reports:
        for report in json_reports:
            profit_rate = report['summary'].get('total_profit_rate', 0) * 100
            total_return = report['metrics'].get('total_return', 0) * 100
            annual_return = report['metrics'].get('annual_return', 0) * 100
            sharpe_ratio = report['metrics'].get('sharpe_ratio', 0)
            total_trades = report['metrics'].get('total_trades', 0)
            
            profit_class = 'positive' if profit_rate >= 0 else 'negative'
            return_class = 'positive' if total_return >= 0 else 'negative'
            
            history_warning = '' if report['has_history'] else '<span class="warning-badge">⚠️ 缺少历史数据</span>'
            
            html_content += f'''                <li class="file-item{' no-history' if not report['has_history'] else ''}">
                    <div class="file-header">
                        <a href="report_template.html?file={report['file']}">{report['file']}</a>
                        {history_warning}
                    </div>
                    <div class="file-info">
                        <strong>{report['strategy_cn']}</strong> | {report['start_date']} 至 {report['end_date']}
                        <div class="file-metrics">
                            <span class="metric-badge {profit_class}">总收益: {profit_rate:.2f}%</span>
                            <span class="metric-badge {return_class}">年化收益: {annual_return:.2f}%</span>
                            <span class="metric-badge">夏普比率: {sharpe_ratio:.2f}</span>
                            <span class="metric-badge">交易次数: {total_trades}</span>
                        </div>
                    </div>
                </li>
'''
    else:
        html_content += '''                <li class="empty-state">暂无JSON报告文件</li>
'''
    
    html_content += '''            </ul>
        </div>
        
        <div class="section">
            <h2 class="section-title">📊 HTML完整报告（可直接打开）</h2>
            <ul class="file-list" id="htmlList">
'''
    
    # 添加HTML报告列表
    if html_reports:
        for report in html_reports:
            html_content += f'''                <li class="file-item">
                    <div class="file-header">
                        <a href="{report['file']}" target="_blank">{report['file']}</a>
                    </div>
                    <div class="file-info">
                        <strong>{report['strategy_cn']}</strong> | 完整HTML报告
                    </div>
                </li>
'''
    else:
        html_content += '''                <li class="empty-state">暂无HTML报告文件</li>
'''
    
    html_content += '''            </ul>
        </div>
        
        <div style="margin-top: 40px; padding: 20px; background: #e8f4f8; border-radius: 8px; color: #666;">
            <strong>💡 使用提示：</strong>
            <ul style="margin-top: 10px; padding-left: 20px;">
                <li><strong>JSON文件：</strong>点击后会在模板页面中加载数据，需要完整的portfolio_history数据才能显示图表</li>
                <li><strong>HTML报告：</strong>可直接在浏览器中打开，包含完整的图表和分析</li>
                <li><strong>缺少历史数据：</strong>如果JSON文件缺少portfolio_history，请运行新的回测以获取完整数据</li>
            </ul>
        </div>
        
        <p style="margin-top: 30px; text-align: center; color: #999; font-size: 0.85em;">
            最后更新: ''' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '''
        </p>
    </div>
</body>
</html>
'''
    
    # 保存文件
    view_report_file = results_dir / 'view_report.html'
    with open(view_report_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"✅ 报告列表已更新: {view_report_file}")
    print(f"   发现 {len(json_reports)} 个JSON文件，{len(html_reports)} 个HTML报告")
    
    return view_report_file

if __name__ == '__main__':
    update_report_list()

