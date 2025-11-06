# 快速推送指南（使用 Token）

## 已配置 Token

您的 GitHub Personal Access Token 已准备好使用。

## 使用步骤

### 1. 安装 Git（如果未安装）

```powershell
# 方法1：使用 winget
winget install --id Git.Git -e --source winget

# 方法2：手动下载
# 访问：https://git-scm.com/download/win
```

### 2. 运行推送脚本

安装 Git 后，运行以下命令：

```powershell
cd c:\JQQuant
powershell -ExecutionPolicy Bypass -File push_with_token.ps1 -Token "YOUR_GITHUB_TOKEN"
```

或者，将 token 保存到文件（更安全）：

```powershell
# 创建 token 文件（已在 .gitignore 中，不会被提交）
echo "YOUR_GITHUB_TOKEN" > github_token.txt

# 使用存储的 token
powershell -ExecutionPolicy Bypass -File push_with_token.ps1 -UseStoredToken
```

## 脚本功能

`push_with_token.ps1` 会自动完成：

1. ✅ 检查 Git 是否安装
2. ✅ 验证敏感文件已排除（`config/jqdata_config.json` 不会被提交）
3. ✅ 初始化 Git 仓库（如需要）
4. ✅ 配置用户信息（如需要）
5. ✅ 添加远程仓库
6. ✅ 提交所有代码
7. ✅ 使用 token 推送到 GitHub
8. ✅ 推送后清除 token（安全）

## 安全说明

- ✅ Token 文件 `github_token.txt` 已在 `.gitignore` 中，不会被提交
- ✅ 脚本会在推送后自动清除 Git 配置中的 token
- ✅ 敏感配置文件 `config/jqdata_config.json` 不会被提交

## 手动操作（如果脚本不可用）

如果脚本无法运行，可以手动执行：

```bash
# 1. 初始化仓库
git init

# 2. 配置用户
git config user.name "ZhuTechLLC"
git config user.email "your.email@example.com"

# 3. 添加远程仓库（使用 token）
git remote add origin https://YOUR_GITHUB_TOKEN@github.com/ZhuTechLLC/JQQuant.git

# 4. 添加文件
git add .

# 5. 提交
git commit -m "feat: Complete A-share adaptive momentum strategy v2 development"

# 6. 推送
git push -u origin main

# 7. 清除 token（安全）
git remote set-url origin https://github.com/ZhuTechLLC/JQQuant.git
```

## 验证推送

推送成功后，访问：
- https://github.com/ZhuTechLLC/JQQuant

应该能看到所有代码文件，但**不应该**看到：
- ❌ `config/jqdata_config.json`（敏感文件）
- ❌ `github_token.txt`（token 文件）

## 后续更新

以后有新的更改时：

```powershell
# 使用存储的 token
powershell -ExecutionPolicy Bypass -File push_with_token.ps1 -UseStoredToken

# 或直接传入 token
powershell -ExecutionPolicy Bypass -File push_with_token.ps1 -Token "your_token"
```

## 注意事项

1. **Token 安全**：Token 具有完整的仓库访问权限，请妥善保管
2. **不要提交 Token**：确保 `github_token.txt` 在 `.gitignore` 中
3. **Token 过期**：如果 token 过期，需要生成新的 token
4. **权限检查**：确保 token 有 `repo` 权限

