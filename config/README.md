# 配置文件说明

本目录用于存储所有API账号、密码和配置信息。

## 文件说明

### jqdata_config.json
聚宽（JoinQuant）API配置
- `username`: 聚宽用户名/手机号
- `password`: 聚宽密码
- `api_endpoint`: API端点地址
- `timeout`: 请求超时时间（秒）
- `retry_times`: 重试次数

### 其他API配置（待添加）
后续如有其他API账号密码，请在此目录下创建对应的配置文件，例如：
- `tushare_config.json` - Tushare数据API
- `trade_config.json` - 交易API配置
- `database_config.json` - 数据库配置

## 安全提示

⚠️ **重要**: 本目录包含敏感信息，请勿将配置文件提交到Git仓库。

已在 `.gitignore` 中排除 `config/jqdata_config.json`，但请确保：
1. 不要将配置文件上传到公共仓库
2. 定期更新密码
3. 使用环境变量或密钥管理工具（生产环境推荐）

## 使用方式

### 方式1：使用配置管理器（推荐）

```python
from config.config_manager import get_config_manager

# 获取配置管理器
config_manager = get_config_manager()

# 获取聚宽配置
jq_config = config_manager.get_jqdata_config()
username = jq_config['username']
password = jq_config['password']

# 获取其他服务配置
tushare_config = config_manager.get_config('tushare')  # 加载 tushare_config.json
trade_config = config_manager.get_config('trade')       # 加载 trade_config.json

# 更新配置
config_manager.update_jqdata_config(username='新用户名', password='新密码')

# 保存新服务的配置
config_manager.save_config_for_service('tushare', {
    'token': 'your_token',
    'api_endpoint': 'http://api.tushare.pro'
})
```

### 方式2：直接读取JSON文件

```python
import json
from pathlib import Path

config_path = Path(__file__).parent / "config" / "jqdata_config.json"
with open(config_path, 'r', encoding='utf-8') as f:
    config = json.load(f)
    username = config['username']
    password = config['password']
```

## 添加新的API配置

当需要添加新的API账号密码时：

1. 在 `config/` 目录下创建新的配置文件，例如 `tushare_config.json`
2. 使用配置管理器加载：

```python
from config.config_manager import get_config_manager

config_manager = get_config_manager()
tushare_config = config_manager.get_config('tushare')  # 自动加载 tushare_config.json
```

或者直接保存：

```python
config_manager.save_config_for_service('tushare', {
    'token': 'your_token',
    'api_endpoint': 'http://api.tushare.pro'
})
```

