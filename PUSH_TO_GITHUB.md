# 推送到 GitHub 完整指南

## 仓库信息
- **仓库地址**: https://github.com/ZhuTechLLC/JQQuant.git
- **用户名**: ZhuTechLLC
- **仓库状态**: 当前为空仓库

## 快速开始

### 方法一：使用自动化脚本（推荐）

```powershell
cd c:\JQQuant
powershell -ExecutionPolicy Bypass -File setup_and_push.ps1
```

脚本会自动完成：
1. ✅ 检查 Git 是否安装
2. ✅ 验证敏感文件排除
3. ✅ 初始化 Git 仓库
4. ✅ 配置用户信息
5. ✅ 添加远程仓库
6. ✅ 提交代码
7. ✅ 推送到 GitHub

### 方法二：手动操作

#### 1. 安装 Git（如果未安装）

**Windows 安装：**
- 访问：https://git-scm.com/download/win
- 下载并安装 Git for Windows
- 或使用 winget：`winget install --id Git.Git -e --source winget`

**验证安装：**
```bash
git --version
```

#### 2. 初始化仓库并配置

```bash
cd c:\JQQuant

# 初始化仓库
git init

# 配置用户信息
git config user.name "ZhuTechLLC"
git config user.email "your.email@example.com"

# 添加远程仓库
git remote add origin https://github.com/ZhuTechLLC/JQQuant.git
```

#### 3. 提交代码

```bash
# 添加文件（敏感文件已自动排除）
git add .

# 确认敏感文件不在暂存区
git status

# 提交
git commit -m "feat: Complete A-share adaptive momentum strategy v2 development"
```

#### 4. 推送到 GitHub

```bash
# 推送到 main 分支
git push -u origin main
```

## 认证方式

### 方式一：使用 Personal Access Token（推荐）

GitHub 已不再支持密码认证，需要使用 Personal Access Token：

1. **生成 Token：**
   - 访问：https://github.com/settings/tokens
   - 点击 "Generate new token" → "Generate new token (classic)"
   - 设置名称：`JQQuant Access`
   - 选择权限：勾选 `repo`（完整仓库访问权限）
   - 点击 "Generate token"
   - **重要**：复制生成的 token（只显示一次）

2. **使用 Token：**
   ```bash
   git push -u origin main
   # Username: ZhuTechLLC
   # Password: <粘贴你的 token>
   ```

### 方式二：配置凭据管理器（Windows）

```bash
# 配置 Windows 凭据管理器
git config --global credential.helper wincred

# 首次推送时会提示输入用户名和 token，之后会自动保存
git push -u origin main
```

### 方式三：使用 SSH 密钥（最安全）

1. **生成 SSH 密钥：**
   ```bash
   ssh-keygen -t ed25519 -C "your.email@example.com"
   # 按 Enter 使用默认路径
   # 设置密码（可选）
   ```

2. **添加 SSH 密钥到 GitHub：**
   - 复制公钥内容：`cat ~/.ssh/id_ed25519.pub`
   - 访问：https://github.com/settings/keys
   - 点击 "New SSH key"
   - 粘贴公钥并保存

3. **使用 SSH URL：**
   ```bash
   git remote set-url origin git@github.com:ZhuTechLLC/JQQuant.git
   git push -u origin main
   ```

## 安全确认

✅ **已确保敏感信息被排除：**
- `config/jqdata_config.json` 已在 `.gitignore` 中排除
- 所有包含账号密码的配置文件都不会被提交
- 数据文件、日志文件、回测结果等已排除

**提交前检查：**
```bash
# 查看将被提交的文件
git status

# 确认敏感文件不在列表中
git status --ignored

# 查看暂存区文件
git ls-files --cached
```

## 常见问题

### 1. 认证失败

**错误信息：** `remote: Support for password authentication was removed`

**解决方案：** 使用 Personal Access Token 代替密码

### 2. 仓库不存在或无权限

**错误信息：** `remote: Repository not found` 或 `Permission denied`

**解决方案：**
- 确认仓库 URL 正确
- 确认有仓库的写入权限
- 检查用户名是否正确

### 3. 分支名称问题

**错误信息：** `error: src refspec main does not match any`

**解决方案：**
```bash
# 创建并切换到 main 分支
git checkout -b main

# 或使用 master 分支
git push -u origin master
```

### 4. 没有更改可提交

**错误信息：** `nothing to commit, working tree clean`

**解决方案：** 这表示所有文件都已提交，可以直接推送：
```bash
git push -u origin main
```

## 推送后的验证

推送成功后，访问以下 URL 验证：
- https://github.com/ZhuTechLLC/JQQuant

应该能看到：
- ✅ 所有代码文件
- ✅ README.md
- ✅ .gitignore
- ✅ 项目结构
- ❌ **不应该看到** `config/jqdata_config.json`（敏感文件）

## 后续更新

以后有新的更改时：

```bash
# 添加更改
git add .

# 提交
git commit -m "描述你的更改"

# 推送
git push
```

## 需要帮助？

如果遇到问题：
1. 检查 Git 是否已安装：`git --version`
2. 检查远程仓库配置：`git remote -v`
3. 查看 Git 状态：`git status`
4. 查看提交历史：`git log --oneline`

