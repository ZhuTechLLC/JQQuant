# Complete Git Setup and Push Script
# This script sets up Git repository and pushes to GitHub
# Repository: https://github.com/ZhuTechLLC/JQQuant.git

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "JQQuant Git Setup and Push Script" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if Git is installed
$gitInstalled = $false
try {
    $gitVersion = git --version 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Git installed: $gitVersion" -ForegroundColor Green
        $gitInstalled = $true
    }
} catch {
    $gitInstalled = $false
}

if (-not $gitInstalled) {
    Write-Host "ERROR: Git is not installed!" -ForegroundColor Red
    Write-Host ""
    Write-Host "Please install Git first:" -ForegroundColor Yellow
    Write-Host "1. Visit: https://git-scm.com/download/win" -ForegroundColor Yellow
    Write-Host "2. Download and install Git for Windows" -ForegroundColor Yellow
    Write-Host "3. Restart terminal and run this script again" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Or install via winget (if available):" -ForegroundColor Yellow
    Write-Host "  winget install --id Git.Git -e --source winget" -ForegroundColor Yellow
    exit 1
}

# Check if in project root
if (-not (Test-Path ".gitignore")) {
    Write-Host "ERROR: .gitignore not found. Please run in project root directory." -ForegroundColor Red
    exit 1
}

Write-Host "Project root confirmed" -ForegroundColor Green
Write-Host ""

# Verify sensitive files are excluded
Write-Host "Verifying sensitive files exclusion..." -ForegroundColor Cyan
$sensitiveFiles = @("config\jqdata_config.json")
$allExcluded = $true

foreach ($file in $sensitiveFiles) {
    if (Test-Path $file) {
        $gitignoreContent = Get-Content .gitignore -Raw -ErrorAction SilentlyContinue
        $fileName = Split-Path $file -Leaf
        if ($gitignoreContent -and $gitignoreContent -match [regex]::Escape($fileName)) {
            Write-Host "  OK: $file is excluded" -ForegroundColor Green
        } else {
            Write-Host "  WARNING: $file may not be excluded!" -ForegroundColor Red
            $allExcluded = $false
        }
    }
}

if (-not $allExcluded) {
    Write-Host ""
    Write-Host "WARNING: Some sensitive files may not be excluded!" -ForegroundColor Yellow
    $continue = Read-Host "Continue anyway? (y/n)"
    if ($continue -ne "y") {
        exit 1
    }
}

Write-Host ""

# Initialize Git repository if needed
if (-not (Test-Path ".git")) {
    Write-Host "Initializing Git repository..." -ForegroundColor Cyan
    git init
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Failed to initialize Git repository" -ForegroundColor Red
        exit 1
    }
    Write-Host "Git repository initialized" -ForegroundColor Green
    Write-Host ""
}

# Configure Git user if needed
$userName = git config user.name 2>&1
$userEmail = git config user.email 2>&1

if (-not $userName -or $userName -match "error" -or -not $userEmail -or $userEmail -match "error") {
    Write-Host "Git user info not configured" -ForegroundColor Yellow
    Write-Host "Configuring Git user info..." -ForegroundColor Cyan
    
    # Use provided username or prompt
    $defaultUser = "ZhuTechLLC"
    $inputUser = Read-Host "Enter Git username (default: $defaultUser)"
    if ([string]::IsNullOrWhiteSpace($inputUser)) {
        $inputUser = $defaultUser
    }
    
    $inputEmail = Read-Host "Enter Git email"
    if ([string]::IsNullOrWhiteSpace($inputEmail)) {
        Write-Host "ERROR: Email is required" -ForegroundColor Red
        exit 1
    }
    
    git config user.name $inputUser
    git config user.email $inputEmail
    Write-Host "Git user info configured" -ForegroundColor Green
    Write-Host ""
}

# Check current remote
$remoteUrl = git remote get-url origin 2>&1
$hasRemote = $LASTEXITCODE -eq 0

if ($hasRemote) {
    Write-Host "Current remote: $remoteUrl" -ForegroundColor Cyan
    $updateRemote = Read-Host "Update remote URL? (y/n)"
    if ($updateRemote -eq "y") {
        git remote set-url origin "https://github.com/ZhuTechLLC/JQQuant.git"
        Write-Host "Remote URL updated" -ForegroundColor Green
    }
} else {
    Write-Host "Adding remote repository..." -ForegroundColor Cyan
    git remote add origin "https://github.com/ZhuTechLLC/JQQuant.git"
    Write-Host "Remote repository added" -ForegroundColor Green
}

Write-Host ""

# Show current status
Write-Host "Current Git status:" -ForegroundColor Cyan
git status --short
Write-Host ""

# Add files
Write-Host "Adding files to staging area..." -ForegroundColor Cyan
git add .

# Final check for sensitive files
Write-Host ""
Write-Host "Final check - verifying sensitive files not in staging:" -ForegroundColor Cyan
$hasSensitiveFiles = $false
foreach ($file in $sensitiveFiles) {
    if (Test-Path $file) {
        $inIndex = git ls-files --cached $file 2>&1
        if ($inIndex -and -not ($inIndex -match "error")) {
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

# Commit
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

if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "ERROR: Commit failed" -ForegroundColor Red
    Write-Host "This might be because there are no changes to commit." -ForegroundColor Yellow
    Write-Host "Checking if there are any changes..." -ForegroundColor Yellow
    git status
    exit 1
}

Write-Host "Commit successful!" -ForegroundColor Green
Write-Host ""

# Show commit info
Write-Host "Commit info:" -ForegroundColor Cyan
git log -1 --oneline
Write-Host ""

# Push to remote
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Ready to push to GitHub" -ForegroundColor Cyan
Write-Host "Repository: https://github.com/ZhuTechLLC/JQQuant.git" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "IMPORTANT: You will be prompted for credentials:" -ForegroundColor Yellow
Write-Host "  Username: ZhuTechLLC" -ForegroundColor Yellow
Write-Host "  Password: (enter your GitHub password or personal access token)" -ForegroundColor Yellow
Write-Host ""
Write-Host "Note: For security, use a Personal Access Token instead of password:" -ForegroundColor Yellow
Write-Host "  1. Go to: https://github.com/settings/tokens" -ForegroundColor Yellow
Write-Host "  2. Generate a new token with 'repo' scope" -ForegroundColor Yellow
Write-Host "  3. Use the token as your password" -ForegroundColor Yellow
Write-Host ""

$confirm = Read-Host "Ready to push? (y/n)"
if ($confirm -ne "y") {
    Write-Host "Push cancelled. You can push later with: git push -u origin main" -ForegroundColor Yellow
    exit 0
}

# Determine default branch
$defaultBranch = "main"
$branchCheck = git branch --show-current 2>&1
if ($branchCheck -and -not ($branchCheck -match "error")) {
    $defaultBranch = $branchCheck.Trim()
} else {
    # Try to create main branch if it doesn't exist
    git checkout -b main 2>&1 | Out-Null
    $defaultBranch = "main"
}

Write-Host ""
Write-Host "Pushing to origin/$defaultBranch..." -ForegroundColor Cyan
Write-Host "(You will be prompted for credentials)" -ForegroundColor Yellow
Write-Host ""

# Push with credential helper suggestion
git push -u origin $defaultBranch

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Green
    Write-Host "SUCCESS! Code pushed to GitHub" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "Repository URL: https://github.com/ZhuTechLLC/JQQuant" -ForegroundColor Cyan
    Write-Host ""
} else {
    Write-Host ""
    Write-Host "ERROR: Push failed" -ForegroundColor Red
    Write-Host ""
    Write-Host "Common issues:" -ForegroundColor Yellow
    Write-Host "1. Authentication failed - check username/password" -ForegroundColor Yellow
    Write-Host "2. Use Personal Access Token instead of password" -ForegroundColor Yellow
    Write-Host "3. Repository might not exist or you don't have access" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "You can try again later with:" -ForegroundColor Yellow
    Write-Host "  git push -u origin $defaultBranch" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Or configure credential helper:" -ForegroundColor Yellow
    Write-Host "  git config --global credential.helper wincred" -ForegroundColor Yellow
    exit 1
}

