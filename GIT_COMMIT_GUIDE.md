# Git 提交指南

## 安全注意事项

✅ **已确保敏感信息被排除**：
- `config/jqdata_config.json` 已在 `.gitignore` 中排除
- 所有包含账号密码的配置文件都不会被提交

## Git 提交步骤

### 1. 安装 Git（如果未安装）

**Windows 安装方法：**
1. 访问 https://git-scm.com/download/win
2. 下载并安装 Git for Windows
3. 安装完成后重启命令行

**验证安装：**
```bash
git --version
```

### 2. 初始化 Git 仓库（如果尚未初始化）

```bash
cd c:\JQQuant
git init
```

### 3. 配置 Git 用户信息（首次使用）

```bash
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"
```

### 4. 检查要提交的文件（确保敏感信息已排除）

```bash
# 查看将被提交的文件
git status

# 确认 config/jqdata_config.json 不在列表中
git status --ignored
```

### 5. 添加文件到暂存区

```bash
# 添加所有文件（.gitignore 会自动排除敏感文件）
git add .

# 或者选择性添加
git add *.py
git add config/*.py
git add config/stock_pool.json
git add config/example_other_configs.json
git add .gitignore
git add README.md
git add requirements.txt
# ... 其他非敏感文件
```

### 6. 提交更改

```bash
git commit -m "feat: 完成A股自适应动量策略v2开发

- 实现市场环境自动识别框架
- 添加动态参数调整机制
- 完成回测引擎和报告生成器
- 添加交易公司简介和板块分析章节
- 修复交易频率判断和净现金流计算
- 添加高增长板块投资建议"
```

### 7. 添加远程仓库（如果需要推送到远程）

```bash
# 添加远程仓库（替换为您的仓库地址）
git remote add origin https://github.com/yourusername/JQQuant.git

# 或者使用 SSH
git remote add origin git@github.com:yourusername/JQQuant.git
```

### 8. 推送到远程仓库

```bash
# 首次推送
git push -u origin main

# 或如果默认分支是 master
git push -u origin master
```

## 重要提醒

⚠️ **提交前请再次确认：**
1. `config/jqdata_config.json` 不会被提交（已在 .gitignore 中）
2. 没有硬编码的账号密码在代码中
3. 所有敏感信息都已排除

## 当前 .gitignore 配置

已排除的文件/目录：
- `config/jqdata_config.json` - 包含账号密码
- `results/` - 回测结果文件
- `data/` - 数据文件
- `logs/` - 日志文件
- `__pycache__/` - Python 缓存
- `*.json` - JSON 文件（除了 config 目录下的非敏感文件）
- `venv/`, `env/` - 虚拟环境

## 如果已经提交了敏感文件

如果之前不小心提交了敏感文件，需要：

```bash
# 1. 从 Git 历史中移除敏感文件（但保留本地文件）
git rm --cached config/jqdata_config.json

# 2. 提交这个更改
git commit -m "chore: 移除敏感配置文件"

# 3. 如果已经推送到远程，需要强制推送（谨慎使用）
# git push --force
```

