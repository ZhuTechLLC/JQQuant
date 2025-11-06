# Astro集成详细规划

## 概述

将JQQuant的回测结果可视化功能集成到Astro Web应用中，提供现代化的Web界面来查看和管理回测结果。

## 技术栈

- **Astro** - Web框架
- **React** - UI组件（可选）
- **Plotly.js** - 图表库
- **TypeScript** - 类型安全
- **Tailwind CSS** - 样式（可选）

## 项目结构

```
web/
├── src/
│   ├── pages/
│   │   ├── index.astro           # 首页 - 回测列表
│   │   ├── backtest/
│   │   │   ├── index.astro       # 创建新回测
│   │   │   └── [id].astro       # 回测详情
│   │   ├── results/
│   │   │   └── [id].astro        # 结果展示页面
│   │   └── strategies/
│   │       ├── index.astro       # 策略列表
│   │       └── [name].astro       # 策略详情
│   ├── components/
│   │   ├── Chart/
│   │   │   ├── AssetCurve.tsx    # 资产曲线
│   │   │   ├── ReturnsChart.tsx   # 收益率图表
│   │   │   └── DrawdownChart.tsx  # 回撤图表
│   │   ├── Metrics/
│   │   │   ├── MetricCard.astro  # 指标卡片
│   │   │   └── MetricsGrid.astro # 指标网格
│   │   ├── Strategy/
│   │   │   ├── StrategyList.astro # 策略列表
│   │   │   └── StrategyCard.astro
│   │   └── Layout/
│   │       └── Layout.astro      # 布局组件
│   ├── layouts/
│   │   └── Layout.astro          # 主布局
│   ├── api/
│   │   ├── results/
│   │   │   └── [id].json.ts      # 获取回测结果
│   │   ├── strategies/
│   │   │   └── index.json.ts     # 获取策略列表
│   │   └── run-backtest.ts       # 运行回测（未来）
│   ├── utils/
│   │   ├── dataLoader.ts         # 数据加载工具
│   │   └── formatters.ts         # 数据格式化
│   └── styles/
│       └── global.css
├── public/
│   ├── images/
│   └── favicon.ico
├── astro.config.mjs
├── package.json
└── tsconfig.json
```

## 核心功能实现

### 1. 数据API端点

#### 获取回测结果列表
`src/pages/api/results/index.json.ts`

```typescript
import type { APIRoute } from 'astro';
import { readdirSync, readFileSync } from 'fs';
import { join } from 'path';

export const GET: APIRoute = async () => {
  const resultsDir = join(process.cwd(), '..', 'results');
  const files = readdirSync(resultsDir)
    .filter(f => f.endsWith('.json'))
    .map(f => {
      const content = JSON.parse(
        readFileSync(join(resultsDir, f), 'utf-8')
      );
      return {
        id: f.replace('.json', ''),
        strategy: content.strategy,
        start_date: content.start_date,
        end_date: content.end_date,
        total_return: content.metrics.total_return,
        ...content.summary
      };
    })
    .sort((a, b) => new Date(b.end_date) - new Date(a.end_date));

  return new Response(JSON.stringify(files), {
    headers: { 'Content-Type': 'application/json' }
  });
};
```

#### 获取单个回测结果
`src/pages/api/results/[id].json.ts`

```typescript
import type { APIRoute } from 'astro';
import { readFileSync } from 'fs';
import { join } from 'path';

export const GET: APIRoute = async ({ params }) => {
  const { id } = params;
  const filePath = join(process.cwd(), '..', 'results', `${id}.json`);
  
  try {
    const content = readFileSync(filePath, 'utf-8');
    return new Response(content, {
      headers: { 'Content-Type': 'application/json' }
    });
  } catch (error) {
    return new Response(
      JSON.stringify({ error: 'Result not found' }),
      { status: 404 }
    );
  }
};
```

### 2. 图表组件

#### 资产曲线组件
`src/components/Chart/AssetCurve.tsx`

```typescript
import React from 'react';
import Plot from 'react-plotly.js';

interface AssetCurveProps {
  dates: string[];
  totalValue: number[];
  initialCash: number;
}

export default function AssetCurve({ 
  dates, 
  totalValue, 
  initialCash 
}: AssetCurveProps) {
  return (
    <Plot
      data={[
        {
          x: dates,
          y: totalValue,
          type: 'scatter',
          mode: 'lines',
          name: '总资产',
          line: { color: '#667eea', width: 2 }
        },
        {
          x: dates,
          y: Array(dates.length).fill(initialCash),
          type: 'scatter',
          mode: 'lines',
          name: '初始资金',
          line: { color: '#999', dash: 'dash' }
        }
      ]}
      layout={{
        title: '回测资产曲线',
        xaxis: { title: '日期' },
        yaxis: { 
          title: '资产价值 (元)',
          tickformat: ',.0f'
        },
        hovermode: 'x unified',
        template: 'plotly_white'
      }}
      style={{ width: '100%', height: '400px' }}
    />
  );
}
```

#### 收益率图表
`src/components/Chart/ReturnsChart.tsx`

```typescript
import React from 'react';
import Plot from 'react-plotly.js';

interface ReturnsChartProps {
  dates: string[];
  returns: number[];
}

export default function ReturnsChart({ dates, returns }: ReturnsChartProps) {
  const cumulativeReturns = returns.reduce((acc, r) => {
    acc.push((acc[acc.length - 1] || 1) * (1 + r));
    return acc;
  }, [] as number[]).map(r => (r - 1) * 100);

  return (
    <Plot
      data={[{
        x: dates,
        y: cumulativeReturns,
        type: 'scatter',
        mode: 'lines',
        name: '累计收益率',
        fill: 'tozeroy',
        line: { color: '#98c379' }
      }]}
      layout={{
        title: '累计收益率曲线',
        xaxis: { title: '日期' },
        yaxis: { 
          title: '收益率 (%)',
          tickformat: '.2f'
        },
        shapes: [{
          type: 'line',
          x0: dates[0],
          x1: dates[dates.length - 1],
          y0: 0,
          y1: 0,
          line: { color: 'red', dash: 'dash' }
        }]
      }}
      style={{ width: '100%', height: '400px' }}
    />
  );
}
```

### 3. 指标展示组件

`src/components/Metrics/MetricCard.astro`

```astro
---
interface Props {
  title: string;
  value: number | string;
  unit?: string;
  trend?: 'up' | 'down' | 'neutral';
}

const { title, value, unit = '', trend = 'neutral' } = Astro.props;
const trendClass = {
  up: 'text-green-600',
  down: 'text-red-600',
  neutral: 'text-gray-600'
}[trend];
---

<div class="metric-card bg-white p-6 rounded-lg shadow-md">
  <h3 class="text-sm font-medium text-gray-500 mb-2">{title}</h3>
  <p class={`text-3xl font-bold ${trendClass}`}>
    {typeof value === 'number' ? value.toLocaleString() : value}
    {unit && <span class="text-lg ml-1">{unit}</span>}
  </p>
</div>

<style>
  .metric-card {
    transition: transform 0.2s;
  }
  .metric-card:hover {
    transform: translateY(-2px);
  }
</style>
```

### 4. 结果展示页面

`src/pages/results/[id].astro`

```astro
---
import Layout from '../../layouts/Layout.astro';
import AssetCurve from '../../components/Chart/AssetCurve';
import ReturnsChart from '../../components/Chart/ReturnsChart';
import MetricCard from '../../components/Metrics/MetricCard.astro';

const { id } = Astro.params;
const response = await fetch(`${Astro.url.origin}/api/results/${id}.json`);
const results = await response.json();

const metrics = results.metrics;
const summary = results.summary;
const history = results.portfolio_history;
---

<Layout title={`回测结果 - ${results.strategy}`}>
  <div class="container mx-auto px-4 py-8">
    <h1 class="text-3xl font-bold mb-6">
      回测结果: {results.strategy}
    </h1>
    
    <div class="mb-6">
      <p class="text-gray-600">
        回测期间: {results.start_date} 至 {results.end_date}
      </p>
      <p class="text-gray-600">
        股票列表: {results.securities.join(', ')}
      </p>
    </div>

    <!-- 指标卡片 -->
    <div class="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
      <MetricCard 
        title="总收益率" 
        value={(metrics.total_return * 100).toFixed(2)} 
        unit="%"
        trend={metrics.total_return > 0 ? 'up' : 'down'}
      />
      <MetricCard 
        title="年化收益率" 
        value={(metrics.annual_return * 100).toFixed(2)} 
        unit="%"
        trend={metrics.annual_return > 0 ? 'up' : 'down'}
      />
      <MetricCard 
        title="夏普比率" 
        value={metrics.sharpe_ratio.toFixed(2)}
        trend={metrics.sharpe_ratio > 1 ? 'up' : 'neutral'}
      />
      <MetricCard 
        title="最大回撤" 
        value={(metrics.max_drawdown * 100).toFixed(2)} 
        unit="%"
        trend={metrics.max_drawdown < 0.2 ? 'up' : 'down'}
      />
    </div>

    <!-- 图表 -->
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
      <div class="bg-white p-4 rounded-lg shadow-md">
        <AssetCurve 
          client:load
          dates={history.dates}
          totalValue={history.total_value}
          initialCash={summary.initial_cash}
        />
      </div>
      <div class="bg-white p-4 rounded-lg shadow-md">
        <ReturnsChart 
          client:load
          dates={history.dates}
          returns={results.returns_pct}
        />
      </div>
    </div>

    <!-- 持仓详情 -->
    <div class="bg-white p-6 rounded-lg shadow-md">
      <h2 class="text-xl font-bold mb-4">持仓详情</h2>
      <div class="overflow-x-auto">
        <table class="min-w-full">
          <thead>
            <tr class="bg-gray-100">
              <th class="px-4 py-2">股票代码</th>
              <th class="px-4 py-2">持仓数量</th>
              <th class="px-4 py-2">成本价</th>
              <th class="px-4 py-2">当前价</th>
              <th class="px-4 py-2">盈亏</th>
              <th class="px-4 py-2">盈亏率</th>
            </tr>
          </thead>
          <tbody>
            {Object.entries(summary.positions).map(([code, pos]: [string, any]) => (
              <tr>
                <td class="px-4 py-2">{code}</td>
                <td class="px-4 py-2">{pos.amount}</td>
                <td class="px-4 py-2">{pos.cost_price.toFixed(2)}</td>
                <td class="px-4 py-2">{pos.current_price.toFixed(2)}</td>
                <td class={`px-4 py-2 ${pos.profit >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                  {pos.profit.toFixed(2)}
                </td>
                <td class={`px-4 py-2 ${pos.profit_rate >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                  {(pos.profit_rate * 100).toFixed(2)}%
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  </div>
</Layout>
```

## 实施步骤

### 阶段1：基础设置（1-2天）
1. 初始化Astro项目
2. 配置TypeScript和React
3. 设置基本布局和路由

### 阶段2：数据API（1天）
1. 创建API端点读取回测结果
2. 实现数据加载和格式化工具

### 阶段3：图表组件（2-3天）
1. 集成Plotly.js
2. 创建资产曲线、收益率、回撤图表
3. 实现交互功能

### 阶段4：页面开发（2-3天）
1. 结果列表页面
2. 结果详情页面
3. 策略管理页面

### 阶段5：优化和部署（1-2天）
1. 性能优化
2. 响应式设计
3. 部署配置

## 部署方案

### 开发环境
```bash
cd web
npm install
npm run dev
```

### 生产部署
```bash
npm run build
npm run preview
```

### 集成到主项目
可以将Astro应用集成到Python项目中，通过Python启动Web服务器，或者单独部署。

## 未来扩展

1. **实时回测** - 通过WebSocket实时显示回测进度
2. **策略编辑器** - 在线编辑和测试策略
3. **参数优化** - 可视化参数优化过程
4. **实盘监控** - 实盘交易监控界面
5. **多策略对比** - 多个策略的性能对比分析

## 注意事项

1. **数据安全** - 确保敏感信息不会暴露在前端
2. **性能优化** - 大量数据时需要考虑分页和懒加载
3. **浏览器兼容** - 确保主要浏览器支持
4. **移动端适配** - 响应式设计支持移动设备

