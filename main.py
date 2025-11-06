# -*- coding: utf-8 -*-
"""
JQQuant 主入口文件
"""
import argparse
import sys
import io
import logging
from pathlib import Path
from datetime import datetime

# 设置标准输出为UTF-8编码（Windows）
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent))

from config.settings import (
    PROJECT_ROOT, LOGS_DIR, RESULTS_DIR,
    DEFAULT_BACKTEST_CONFIG, LOG_CONFIG
)
from jqdata.client import JQDataClient
from core.data_provider import DataProvider
from core.backtest_engine import BacktestEngine
from strategies.examples.ma_cross import MACrossStrategy
from strategies.examples.adaptive_momentum import AdaptiveMomentumStrategy
from strategies.examples.adaptive_momentum_a import AdaptiveMomentumStrategyA
from strategies.examples.adaptive_momentum_a_v2 import AdaptiveMomentumStrategyA_V2
import pandas as pd

# 配置日志
logging.basicConfig(
    level=getattr(logging, LOG_CONFIG['level']),
    format=LOG_CONFIG['format'],
    handlers=[
        logging.FileHandler(LOG_CONFIG['file'], encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_strategy(strategy_name: str, **kwargs):
    """
    加载策略
    
    Args:
        strategy_name: 策略名称
        **kwargs: 策略参数
    
    Returns:
        BaseStrategy: 策略实例
    """
    if strategy_name == 'ma_cross':
        short_window = kwargs.get('short_window', 5)
        long_window = kwargs.get('long_window', 20)
        return MACrossStrategy(short_window=short_window, long_window=long_window)
    elif strategy_name == 'adaptive_momentum':
        benchmark = kwargs.get('benchmark', '000300.XSHG')
        roc_10_min = kwargs.get('roc_10_min', 0.02)
        roc_20_min = kwargs.get('roc_20_min', 0.03)
        max_positions = kwargs.get('max_positions', 7)
        position_size = kwargs.get('position_size', 0.15)
        stop_loss = kwargs.get('stop_loss', 0.10)
        take_profit = kwargs.get('take_profit', 0.50)
        return AdaptiveMomentumStrategy(
            benchmark=benchmark,
            roc_10_min=roc_10_min,
            roc_20_min=roc_20_min,
            max_positions=max_positions,
            position_size=position_size,
            stop_loss=stop_loss,
            take_profit=take_profit
        )
    elif strategy_name == 'adaptive_momentum_a':
        benchmark = kwargs.get('benchmark', '000300.XSHG')
        growth_index = kwargs.get('growth_index', '399006.XSHE')
        roc_10_min = kwargs.get('roc_10_min', 0.02)
        roc_20_min = kwargs.get('roc_20_min', 0.03)
        max_positions = kwargs.get('max_positions', 7)
        position_size = kwargs.get('position_size', 0.15)
        stop_loss = kwargs.get('stop_loss', 0.10)
        take_profit = kwargs.get('take_profit', 0.50)
        return AdaptiveMomentumStrategyA(
            benchmark=benchmark,
            growth_index=growth_index,
            roc_10_min=roc_10_min,
            roc_20_min=roc_20_min,
            max_positions=max_positions,
            position_size=position_size,
            stop_loss=stop_loss,
            take_profit=take_profit
        )
    elif strategy_name == 'adaptive_momentum_a_v2':
        benchmark = kwargs.get('benchmark', '000300.XSHG')
        growth_index = kwargs.get('growth_index', '399006.XSHE')
        roc_10_min = kwargs.get('roc_10_min', 0.015)
        roc_20_min = kwargs.get('roc_20_min', 0.025)
        max_positions = kwargs.get('max_positions', 8)
        position_size = kwargs.get('position_size', 0.12)
        stop_loss = kwargs.get('stop_loss', 0.08)
        take_profit = kwargs.get('take_profit', 0.40)
        use_dynamic_pool = kwargs.get('use_dynamic_pool', True)
        use_fundamental = kwargs.get('use_fundamental', False)
        use_relative_strength = kwargs.get('use_relative_strength', True)
        return AdaptiveMomentumStrategyA_V2(
            benchmark=benchmark,
            growth_index=growth_index,
            roc_10_min=roc_10_min,
            roc_20_min=roc_20_min,
            max_positions=max_positions,
            position_size=position_size,
            stop_loss=stop_loss,
            take_profit=take_profit,
            use_dynamic_pool=use_dynamic_pool,
            use_fundamental=use_fundamental,
            use_relative_strength=use_relative_strength
        )
    else:
        raise ValueError(f"未知的策略: {strategy_name}")

def run_backtest(
    strategy_name: str,
    start_date: str,
    end_date: str,
    securities: list,
    initial_cash: float = 1000000,
    commission_rate: float = 0.0003,
    slippage: float = 0.001,
    strategy_params: dict = None
):
    """
    运行回测
    
    Args:
        strategy_name: 策略名称
        start_date: 开始日期
        end_date: 结束日期
        securities: 股票代码列表
        initial_cash: 初始资金
        commission_rate: 手续费率
        slippage: 滑点
        strategy_params: 策略参数
    """
    logger.info("=" * 60)
    logger.info("JQQuant 回测系统")
    logger.info("=" * 60)
    
    # 1. 初始化聚宽客户端
    logger.info("初始化聚宽客户端...")
    jq_client = JQDataClient()
    
    # 从配置管理器加载配置
    from config.config_manager import get_config_manager
    config_manager = get_config_manager()
    config = config_manager.get_jqdata_config()
    
    if config.get('username') and config.get('password'):
        if not jq_client.authenticate(config.get('username'), config.get('password')):
            logger.error("聚宽认证失败，请检查配置文件")
            return None
    else:
        logger.warning("未找到聚宽账号信息，请检查配置文件")
        return None
    
    # 2. 初始化数据提供者
    logger.info("初始化数据提供者...")
    data_provider = DataProvider(jq_client=jq_client)
    
    # 3. 初始化回测引擎
    logger.info("初始化回测引擎...")
    engine = BacktestEngine(
        data_provider=data_provider,
        initial_cash=initial_cash,
        commission_rate=commission_rate,
        slippage=slippage
    )
    
    # 4. 加载策略
    logger.info(f"加载策略: {strategy_name}")
    strategy_params = strategy_params or {}
    strategy = load_strategy(strategy_name, **strategy_params)
    engine.set_strategy(strategy)
    
    # 5. 运行回测
    logger.info(f"开始回测: {start_date} 至 {end_date}")
    logger.info(f"股票列表: {securities}")
    
    try:
        results = engine.run(
            start_date=start_date,
            end_date=end_date,
            securities=securities,
            frequency='daily'
        )
    except ValueError as e:
        # 处理账号权限限制错误
        if "账号权限限制" in str(e):
            logger.error("=" * 60)
            logger.error("日期范围超出账号权限")
            logger.error("=" * 60)
            logger.error(str(e))
            logger.error("=" * 60)
            logger.info("\n提示：")
            logger.info("1. 请使用账号允许的日期范围进行回测")
            logger.info("2. 如需更长时间的数据范围，可联系聚宽运营咨询采购")
            logger.info("3. 或使用其他数据源（如Tushare等）")
            return None
        else:
            raise
    except Exception as e:
        logger.error(f"回测失败: {str(e)}", exc_info=True)
        return None
    
    # 6. 输出结果
    logger.info("=" * 60)
    logger.info("回测结果")
    logger.info("=" * 60)
    
    summary = results['summary']
    metrics = results['metrics']
    
    logger.info(f"初始资金: {summary['initial_cash']:,.2f}")
    logger.info(f"最终资产: {summary['total_value']:,.2f}")
    logger.info(f"总收益: {summary['total_profit']:,.2f} ({summary['total_profit_rate']*100:.2f}%)")
    logger.info(f"年化收益: {metrics['annual_return']*100:.2f}%")
    logger.info(f"夏普比率: {metrics['sharpe_ratio']:.2f}")
    logger.info(f"最大回撤: {metrics['max_drawdown']*100:.2f}%")
    logger.info(f"总交易次数: {metrics['total_trades']}")
    
    # 保存结果JSON
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    results_file = RESULTS_DIR / f"backtest_{strategy_name}_{timestamp}.json"
    import json
    results_dict = {
        'strategy': strategy_name,
        'start_date': start_date,
        'end_date': end_date,
        'securities': securities,
        'summary': summary,
        'metrics': metrics,
        'portfolio_history': results.get('portfolio_history', {}),
        'returns_pct': results.get('returns_pct', pd.Series()).tolist() if not results.get('returns_pct', pd.Series()).empty else [],
        'trade_history': results.get('trade_history', [])
    }
    with open(results_file, 'w', encoding='utf-8') as f:
        json.dump(results_dict, f, indent=2, ensure_ascii=False, default=str)
    logger.info(f"结果JSON已保存至: {results_file}")
    
    # 生成HTML报告
    try:
        # 对于adaptive_momentum_a策略，使用详细报告生成器
        if strategy_name in ['adaptive_momentum_a', 'adaptive_momentum_a_v2']:
            from utils.comprehensive_report_generator import generate_comprehensive_html_report
            html_report_file = RESULTS_DIR / f"{strategy_name}_v1.0_report_{timestamp}.html"
            
            # 获取策略参数
            strategy_params_dict = {}
            if strategy_params:
                strategy_params_dict = strategy_params
            
            generate_comprehensive_html_report(
                results=results,
                strategy_name=strategy_name,
                strategy_version="1.0.0",
                strategy_params=strategy_params_dict if strategy_params_dict else None,
                start_date=start_date,
                end_date=end_date,
                save_path=html_report_file
            )
            logger.info(f"完整详细HTML报告已保存至: {html_report_file}")
        else:
            from utils.report_generator import generate_html_report
            html_report_file = RESULTS_DIR / f"{strategy_name}_v1.0_report_{timestamp}.html"
            
            # 获取策略参数
            strategy_params_dict = {}
            if strategy_params:
                strategy_params_dict = strategy_params
            
            generate_html_report(
                results=results,
                strategy_name=strategy_name,
                strategy_version="1.0.0",
                strategy_params=strategy_params_dict if strategy_params_dict else None,
                save_path=html_report_file
            )
            logger.info(f"HTML报告已保存至: {html_report_file}")
        
        # 自动更新报告列表
        try:
            from update_report_list import update_report_list
            update_report_list()
            logger.info("报告列表已自动更新")
        except Exception as e:
            logger.warning(f"更新报告列表失败: {str(e)}")
    except Exception as e:
        logger.warning(f"生成HTML报告失败: {str(e)}", exc_info=True)
    
    return results

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='JQQuant 量化回测系统')
    parser.add_argument('--strategy', '-s', type=str, required=True, help='策略名称')
    parser.add_argument('--start', type=str, required=True, help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, required=True, help='结束日期 (YYYY-MM-DD)')
    parser.add_argument('--securities', '-stocks', type=str, nargs='+', required=True, help='股票代码列表')
    parser.add_argument('--cash', type=float, default=1000000, help='初始资金')
    parser.add_argument('--commission', type=float, default=0.0003, help='手续费率')
    parser.add_argument('--slippage', type=float, default=0.001, help='滑点')
    parser.add_argument('--short-window', type=int, default=5, help='短期均线周期（MA策略）')
    parser.add_argument('--long-window', type=int, default=20, help='长期均线周期（MA策略）')
    parser.add_argument('--benchmark', type=str, default='000300.XSHG', help='基准指数（自适应动量策略）')
    parser.add_argument('--roc-10-min', type=float, default=0.02, help='ROC10最小值（自适应动量策略）')
    parser.add_argument('--roc-20-min', type=float, default=0.03, help='ROC20最小值（自适应动量策略）')
    parser.add_argument('--max-positions', type=int, default=7, help='最大持仓数（自适应动量策略）')
    parser.add_argument('--position-size', type=float, default=0.15, help='单只股票仓位（自适应动量策略）')
    parser.add_argument('--stop-loss', type=float, default=0.10, help='止损比例（自适应动量策略）')
    parser.add_argument('--take-profit', type=float, default=0.50, help='止盈比例（自适应动量策略）')
    
    args = parser.parse_args()
    
    # 策略参数
    strategy_params = {}
    if args.strategy == 'ma_cross':
        strategy_params = {
            'short_window': args.short_window,
            'long_window': args.long_window
        }
    elif args.strategy == 'adaptive_momentum':
        strategy_params = {
            'benchmark': args.benchmark,
            'roc_10_min': args.roc_10_min,
            'roc_20_min': args.roc_20_min,
            'max_positions': args.max_positions,
            'position_size': args.position_size,
            'stop_loss': args.stop_loss,
            'take_profit': args.take_profit
        }
    
    # 运行回测
    run_backtest(
        strategy_name=args.strategy,
        start_date=args.start,
        end_date=args.end,
        securities=args.securities,
        initial_cash=args.cash,
        commission_rate=args.commission,
        slippage=args.slippage,
        strategy_params=strategy_params
    )

if __name__ == '__main__':
    main()

