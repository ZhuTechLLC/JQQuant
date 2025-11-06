# Git Commit Script
# This script safely commits code, ensuring sensitive information is not committed

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "JQQuant Project Git Commit Script" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# 检查 Git 是否安装
try {
    $gitVersion = git --version 2>&1
    Write-Host "Git installed: $gitVersion" -ForegroundColor Green
} catch {
    Write-Host "Git not installed or not in PATH" -ForegroundColor Red
    Write-Host ""
    Write-Host "Please install Git first:" -ForegroundColor Yellow
    Write-Host "1. Visit https://git-scm.com/download/win" -ForegroundColor Yellow
    Write-Host "2. Download and install Git for Windows" -ForegroundColor Yellow
    Write-Host "3. Restart terminal and run this script again" -ForegroundColor Yellow
    exit 1
}

# 检查是否在项目根目录
if (-not (Test-Path ".gitignore")) {
    Write-Host "ERROR: .gitignore not found, please run this script in project root" -ForegroundColor Red
    exit 1
}

Write-Host "Project root directory confirmed" -ForegroundColor Green
Write-Host ""

# 检查敏感文件是否会被提交
Write-Host "Checking sensitive files exclusion..." -ForegroundColor Cyan
$sensitiveFiles = @(
    "config\jqdata_config.json"
)

$allExcluded = $true
foreach ($file in $sensitiveFiles) {
    if (Test-Path $file) {
        # 检查文件是否在 .gitignore 中
        $gitignoreContent = Get-Content .gitignore -Raw
        $fileName = Split-Path $file -Leaf
        if ($gitignoreContent -match [regex]::Escape($fileName)) {
            Write-Host "  OK: $file is excluded in .gitignore" -ForegroundColor Green
        } else {
            Write-Host "  WARNING: $file may not be excluded!" -ForegroundColor Red
            $allExcluded = $false
        }
    }
}

if (-not $allExcluded) {
    Write-Host ""
    Write-Host "WARNING: Some sensitive files may not be excluded, please check .gitignore" -ForegroundColor Yellow
    $continue = Read-Host "Continue? (y/n)"
    if ($continue -ne "y") {
        exit 1
    }
}

Write-Host ""

# 初始化 Git 仓库（如果尚未初始化）
if (-not (Test-Path ".git")) {
    Write-Host "Initializing Git repository..." -ForegroundColor Cyan
    git init
    Write-Host "Git repository initialized" -ForegroundColor Green
    Write-Host ""
}

# 检查 Git 用户配置
$userName = git config user.name
$userEmail = git config user.email

if (-not $userName -or -not $userEmail) {
    Write-Host "WARNING: Git user info not configured" -ForegroundColor Yellow
    Write-Host "Please configure Git user info:" -ForegroundColor Yellow
    $userName = Read-Host "Enter username"
    $userEmail = Read-Host "Enter email"
    git config user.name $userName
    git config user.email $userEmail
    Write-Host "Git user info configured" -ForegroundColor Green
    Write-Host ""
}

# 显示当前状态
Write-Host "Current Git status:" -ForegroundColor Cyan
git status --short
Write-Host ""

# 确认要提交的文件
Write-Host "Files to be committed (sensitive files auto-excluded):" -ForegroundColor Cyan
git status --short --ignored | Select-Object -First 20
Write-Host ""

# 确认敏感文件不会被提交
Write-Host "Confirm sensitive files exclusion:" -ForegroundColor Cyan
foreach ($file in $sensitiveFiles) {
    if (Test-Path $file) {
        $status = git check-ignore $file 2>&1
        if ($status) {
            Write-Host "  OK: $file is excluded" -ForegroundColor Green
        } else {
            Write-Host "  WARNING: $file may be committed!" -ForegroundColor Red
        }
    }
}
Write-Host ""

# 询问是否继续
$confirm = Read-Host "Confirm commit? (y/n)"
if ($confirm -ne "y") {
    Write-Host "Commit cancelled" -ForegroundColor Yellow
    exit 0
}

# 添加文件
Write-Host ""
Write-Host "Adding files to staging area..." -ForegroundColor Cyan
git add .
Write-Host "Files added to staging area" -ForegroundColor Green

# 再次确认敏感文件不会被提交
Write-Host ""
Write-Host "Final check - confirm sensitive files not in staging:" -ForegroundColor Cyan
$hasSensitiveFiles = $false
foreach ($file in $sensitiveFiles) {
    if (Test-Path $file) {
        $inIndex = git ls-files --cached $file 2>&1
        if ($inIndex) {
            Write-Host "  ERROR: $file is in staging! Removing..." -ForegroundColor Red
            git rm --cached $file 2>&1 | Out-Null
            $hasSensitiveFiles = $true
        } else {
            Write-Host "  OK: $file not in staging" -ForegroundColor Green
        }
    }
}

if ($hasSensitiveFiles) {
    Write-Host "  Sensitive files removed from staging" -ForegroundColor Green
}

Write-Host ""

# 提交
$commitMessage = @"
feat: Complete A-share adaptive momentum strategy v2 development

Main features:
- Implement market regime detection framework
- Add dynamic parameter adjustment mechanism
- Complete backtest engine and report generator
- Add company introductions and sector analysis sections
- Fix trading frequency calculation and net cash flow calculation
- Add high-growth sector investment recommendations

Technical improvements:
- Optimize stock pool construction logic
- Enhance multi-factor stock selection scoring system
- Improve risk management and position control
- Enhance report generation and visualization
"@

Write-Host "Committing changes..." -ForegroundColor Cyan
git commit -m $commitMessage

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Green
    Write-Host "Commit successful!" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "Commit info:" -ForegroundColor Cyan
    git log -1 --oneline
    Write-Host ""
    Write-Host "Next steps:" -ForegroundColor Yellow
    Write-Host "1. To push to remote: git push -u origin main" -ForegroundColor Yellow
    Write-Host "2. Or add remote first: git remote add origin <repository-url>" -ForegroundColor Yellow
} else {
    Write-Host ""
    Write-Host "Commit failed, please check error messages" -ForegroundColor Red
    exit 1
}

