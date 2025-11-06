# -*- coding: utf-8 -*-
"""
技术指标计算工具模块
提供常用的技术分析指标计算函数
"""
import numpy as np
import pandas as pd
from typing import Union, Optional, Tuple


def sma(prices: Union[np.ndarray, pd.Series], period: int) -> Union[float, np.ndarray]:
    """
    计算简单移动平均线 (Simple Moving Average)
    
    Args:
        prices: 价格序列
        period: 周期
    
    Returns:
        如果输入是数组，返回最后一个值（float）
        如果输入是Series，返回Series
    """
    if isinstance(prices, pd.Series):
        return prices.rolling(window=period).mean()
    else:
        if len(prices) < period:
            return np.nan
        return np.mean(prices[-period:])


def ema(prices: Union[np.ndarray, pd.Series], period: int) -> Union[float, np.ndarray]:
    """
    计算指数移动平均线 (Exponential Moving Average)
    
    Args:
        prices: 价格序列
        period: 周期
    
    Returns:
        如果输入是数组，返回最后一个值（float）
        如果输入是Series，返回Series
    """
    if isinstance(prices, pd.Series):
        return prices.ewm(span=period, adjust=False).mean()
    else:
        if len(prices) < period:
            return np.nan
        # 使用pandas的ewm计算
        series = pd.Series(prices)
        ema_values = series.ewm(span=period, adjust=False).mean()
        return ema_values.iloc[-1]


def rsi(prices: Union[np.ndarray, pd.Series], period: int = 14) -> Union[float, np.ndarray]:
    """
    计算相对强弱指标 (Relative Strength Index)
    
    Args:
        prices: 价格序列
        period: 周期，默认14
    
    Returns:
        RSI值，范围0-100
    """
    if isinstance(prices, pd.Series):
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi_values = 100 - (100 / (1 + rs))
        return rsi_values
    else:
        if len(prices) < period + 1:
            return 50.0  # 默认中性值
        
        deltas = np.diff(prices)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        avg_gain = np.mean(gains[-period:])
        avg_loss = np.mean(losses[-period:])
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi_value = 100 - (100 / (1 + rs))
        return rsi_value


def macd(prices: Union[np.ndarray, pd.Series], 
         fast: int = 12, 
         slow: int = 26, 
         signal: int = 9) -> Union[Tuple[float, float, float], Tuple[pd.Series, pd.Series, pd.Series]]:
    """
    计算MACD指标 (Moving Average Convergence Divergence)
    
    Args:
        prices: 价格序列
        fast: 快线周期，默认12
        slow: 慢线周期，默认26
        signal: 信号线周期，默认9
    
    Returns:
        (MACD线, 信号线, 柱状图) 或 (MACD Series, Signal Series, Histogram Series)
    """
    if isinstance(prices, pd.Series):
        ema_fast = prices.ewm(span=fast, adjust=False).mean()
        ema_slow = prices.ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram
    else:
        if len(prices) < slow:
            return np.nan, np.nan, np.nan
        
        series = pd.Series(prices)
        ema_fast = series.ewm(span=fast, adjust=False).mean()
        ema_slow = series.ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast.iloc[-1] - ema_slow.iloc[-1]
        
        if len(prices) < slow + signal:
            return macd_line, np.nan, np.nan
        
        macd_series = ema_fast - ema_slow
        signal_line = macd_series.ewm(span=signal, adjust=False).mean().iloc[-1]
        histogram = macd_line - signal_line
        
        return macd_line, signal_line, histogram


def bollinger_bands(prices: Union[np.ndarray, pd.Series], 
                    period: int = 20, 
                    std_dev: float = 2.0) -> Union[Tuple[float, float, float], Tuple[pd.Series, pd.Series, pd.Series]]:
    """
    计算布林带 (Bollinger Bands)
    
    Args:
        prices: 价格序列
        period: 周期，默认20
        std_dev: 标准差倍数，默认2.0
    
    Returns:
        (上轨, 中轨, 下轨)
    """
    if isinstance(prices, pd.Series):
        middle = prices.rolling(window=period).mean()
        std = prices.rolling(window=period).std()
        upper = middle + (std * std_dev)
        lower = middle - (std * std_dev)
        return upper, middle, lower
    else:
        if len(prices) < period:
            return np.nan, np.nan, np.nan
        
        middle = np.mean(prices[-period:])
        std = np.std(prices[-period:])
        upper = middle + (std * std_dev)
        lower = middle - (std * std_dev)
        
        return upper, middle, lower


def roc(prices: Union[np.ndarray, pd.Series], period: int = 10) -> Union[float, np.ndarray]:
    """
    计算变动率指标 (Rate of Change)
    
    Args:
        prices: 价格序列
        period: 周期，默认10
    
    Returns:
        ROC值（百分比）
    """
    if isinstance(prices, pd.Series):
        return ((prices - prices.shift(period)) / prices.shift(period)) * 100
    else:
        if len(prices) < period + 1:
            return 0.0
        return ((prices[-1] / prices[-period-1]) - 1) * 100


def atr(high: Union[np.ndarray, pd.Series],
        low: Union[np.ndarray, pd.Series],
        close: Union[np.ndarray, pd.Series],
        period: int = 14) -> Union[float, np.ndarray]:
    """
    计算平均真实波幅 (Average True Range)
    
    Args:
        high: 最高价序列
        low: 最低价序列
        close: 收盘价序列
        period: 周期，默认14
    
    Returns:
        ATR值
    """
    if isinstance(high, pd.Series):
        high_low = high - low
        high_close = np.abs(high - close.shift())
        low_close = np.abs(low - close.shift())
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        return true_range.rolling(window=period).mean()
    else:
        if len(high) < period + 1:
            return np.nan
        
        high_low = high - low
        high_close = np.abs(high[1:] - close[:-1])
        low_close = np.abs(low[1:] - close[:-1])
        
        true_range = np.maximum(high_low[1:], np.maximum(high_close, low_close))
        return np.mean(true_range[-period:])


def stochastic(high: Union[np.ndarray, pd.Series],
               low: Union[np.ndarray, pd.Series],
               close: Union[np.ndarray, pd.Series],
               k_period: int = 14,
               d_period: int = 3) -> Union[Tuple[float, float], Tuple[pd.Series, pd.Series]]:
    """
    计算随机指标 (Stochastic Oscillator)
    
    Args:
        high: 最高价序列
        low: 最低价序列
        close: 收盘价序列
        k_period: %K周期，默认14
        d_period: %D周期，默认3
    
    Returns:
        (%K, %D)
    """
    if isinstance(high, pd.Series):
        lowest_low = low.rolling(window=k_period).min()
        highest_high = high.rolling(window=k_period).max()
        k_percent = 100 * ((close - lowest_low) / (highest_high - lowest_low))
        d_percent = k_percent.rolling(window=d_period).mean()
        return k_percent, d_percent
    else:
        if len(high) < k_period:
            return np.nan, np.nan
        
        lowest_low = np.min(low[-k_period:])
        highest_high = np.max(high[-k_period:])
        
        if highest_high == lowest_low:
            k_percent = 50.0
        else:
            k_percent = 100 * ((close[-1] - lowest_low) / (highest_high - lowest_low))
        
        if len(high) < k_period + d_period - 1:
            return k_percent, np.nan
        
        # 计算%D（%K的移动平均）
        k_values = []
        for i in range(d_period):
            idx = len(high) - d_period + i
            if idx >= k_period - 1:
                ll = np.min(low[idx - k_period + 1:idx + 1])
                hh = np.max(high[idx - k_period + 1:idx + 1])
                if hh == ll:
                    k_val = 50.0
                else:
                    k_val = 100 * ((close[idx] - ll) / (hh - ll))
                k_values.append(k_val)
        
        d_percent = np.mean(k_values) if k_values else np.nan
        
        return k_percent, d_percent


def ma_cross(short_ma: float, long_ma: float, 
              prev_short_ma: Optional[float] = None, 
              prev_long_ma: Optional[float] = None) -> Tuple[bool, bool]:
    """
    判断均线交叉信号
    
    Args:
        short_ma: 当前短期均线值
        long_ma: 当前长期均线值
        prev_short_ma: 上一期短期均线值
        prev_long_ma: 上一期长期均线值
    
    Returns:
        (金叉信号, 死叉信号)
    """
    golden_cross = False
    death_cross = False
    
    if prev_short_ma is not None and prev_long_ma is not None:
        # 金叉：短期均线上穿长期均线
        if short_ma > long_ma and prev_short_ma <= prev_long_ma:
            golden_cross = True
        # 死叉：短期均线下穿长期均线
        elif short_ma < long_ma and prev_short_ma >= prev_long_ma:
            death_cross = True
    
    return golden_cross, death_cross


def volume_ratio(volumes: Union[np.ndarray, pd.Series], period: int = 50) -> float:
    """
    计算成交量比率（当前成交量相对于平均成交量的倍数）
    
    Args:
        volumes: 成交量序列
        period: 计算平均成交量的周期，默认50
    
    Returns:
        成交量比率
    """
    if isinstance(volumes, pd.Series):
        if len(volumes) < period:
            return 1.0
        avg_volume = volumes.rolling(window=period).mean().iloc[-1]
        current_volume = volumes.iloc[-1]
        return current_volume / avg_volume if avg_volume > 0 else 1.0
    else:
        if len(volumes) < period:
            return 1.0
        avg_volume = np.mean(volumes[-period:])
        current_volume = volumes[-1]
        return current_volume / avg_volume if avg_volume > 0 else 1.0

