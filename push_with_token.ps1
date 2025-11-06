# Git Push Script with Token Authentication
# This script safely pushes code to GitHub using a Personal Access Token

param(
    [string]$Token = "",
    [switch]$UseStoredToken
)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "JQQuant Git Push with Token" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Refresh PATH environment variable to find Git
Write-Host "Refreshing PATH environment variable..." -ForegroundColor Cyan
$env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")

# Common Git installation paths
$gitPaths = @(
    "C:\Program Files\Git\cmd\git.exe",
    "C:\Program Files (x86)\Git\cmd\git.exe",
    "$env:LOCALAPPDATA\Programs\Git\cmd\git.exe",
    "$env:ProgramFiles\Git\cmd\git.exe"
)

# Try to find Git and add to PATH if found
$gitFound = $false
foreach ($gitPath in $gitPaths) {
    if (Test-Path $gitPath) {
        $gitDir = Split-Path (Split-Path $gitPath) -Parent
        $env:Path = "$gitDir\cmd;$env:Path"
        $gitFound = $true
        break
    }
}

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
    Write-Host "ERROR: Git is not installed or not found in PATH!" -ForegroundColor Red
    Write-Host ""
    Write-Host "Please try one of the following:" -ForegroundColor Yellow
    Write-Host "1. Install Git from: https://git-scm.com/download/win" -ForegroundColor Yellow
    Write-Host "2. Restart your terminal/PowerShell window" -ForegroundColor Yellow
    Write-Host "3. Or run: powershell -ExecutionPolicy Bypass -File refresh_and_push.ps1" -ForegroundColor Yellow
    exit 1
}

# Get token
$githubToken = $Token

if ([string]::IsNullOrWhiteSpace($githubToken) -and $UseStoredToken) {
    # Try to read from file
    $tokenFile = "github_token.txt"
    if (Test-Path $tokenFile) {
        $githubToken = Get-Content $tokenFile -Raw -ErrorAction SilentlyContinue
        $githubToken = $githubToken.Trim()
        if ($githubToken) {
            Write-Host "Using stored token from file" -ForegroundColor Green
        }
    }
}

if ([string]::IsNullOrWhiteSpace($githubToken)) {
    # Prompt for token securely
    Write-Host "GitHub Personal Access Token required" -ForegroundColor Yellow
    Write-Host "You can:" -ForegroundColor Yellow
    Write-Host "  1. Pass token as parameter: .\push_with_token.ps1 -Token 'your_token'" -ForegroundColor Yellow
    Write-Host "  2. Store token in github_token.txt (will be gitignored)" -ForegroundColor Yellow
    Write-Host "  3. Enter token now (will not be saved)" -ForegroundColor Yellow
    Write-Host ""
    
    $secureToken = Read-Host "Enter GitHub token" -AsSecureString
    $BSTR = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureToken)
    $githubToken = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($BSTR)
    [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($BSTR)
}

if ([string]::IsNullOrWhiteSpace($githubToken)) {
    Write-Host "ERROR: Token is required" -ForegroundColor Red
    exit 1
}

Write-Host "Token configured (length: $($githubToken.Length))" -ForegroundColor Green
Write-Host ""

# Check if in project root
if (-not (Test-Path ".gitignore")) {
    Write-Host "ERROR: .gitignore not found. Please run in project root." -ForegroundColor Red
    exit 1
}

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
    Write-Host "Configuring Git user info automatically..." -ForegroundColor Cyan
    
    # Use default values
    $inputUser = "ZhuTechLLC"
    # Use a generic email (GitHub doesn't require real email for commits)
    $inputEmail = "ZhuTechLLC@users.noreply.github.com"
    
    git config user.name $inputUser
    git config user.email $inputEmail
    Write-Host "Git user info configured: $inputUser <$inputEmail>" -ForegroundColor Green
    Write-Host ""
}

# Configure remote with token
Write-Host "Configuring remote repository..." -ForegroundColor Cyan
$remoteUrl = "https://${githubToken}@github.com/ZhuTechLLC/JQQuant.git"

# Check if remote exists
$existingRemote = git remote get-url origin 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "Updating remote URL..." -ForegroundColor Cyan
    git remote set-url origin $remoteUrl
} else {
    Write-Host "Adding remote repository..." -ForegroundColor Cyan
    git remote add origin $remoteUrl
}

# Clear token from URL in config (for security, store without token)
$cleanUrl = "https://github.com/ZhuTechLLC/JQQuant.git"
git remote set-url origin $cleanUrl

Write-Host "Remote configured" -ForegroundColor Green
Write-Host ""

# Show current status
Write-Host "Current Git status:" -ForegroundColor Cyan
git status --short
Write-Host ""

# Check if there are changes to commit
$statusOutput = git status --porcelain
if ([string]::IsNullOrWhiteSpace($statusOutput)) {
    Write-Host "No changes to commit. Checking if we need to push..." -ForegroundColor Yellow
    
    # Check if we're ahead of remote
    $branchCheck = git branch --show-current 2>&1
    if ($branchCheck -and -not ($branchCheck -match "error")) {
        $branch = $branchCheck.Trim()
    } else {
        $branch = "main"
    }
    
    # Try to push directly
    Write-Host "Attempting to push existing commits..." -ForegroundColor Cyan
} else {
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
        Write-Host "ERROR: Commit failed" -ForegroundColor Red
        exit 1
    }

    Write-Host "Commit successful!" -ForegroundColor Green
    Write-Host ""
}

# Show commit info
Write-Host "Latest commit:" -ForegroundColor Cyan
git log -1 --oneline
Write-Host ""

# Determine branch
$branchCheck = git branch --show-current 2>&1
if ($branchCheck -and -not ($branchCheck -match "error")) {
    $branch = $branchCheck.Trim()
} else {
    # Try to create main branch
    git checkout -b main 2>&1 | Out-Null
    $branch = "main"
}

# Push using token in URL (temporary, for this push only)
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Pushing to GitHub..." -ForegroundColor Cyan
Write-Host "Repository: https://github.com/ZhuTechLLC/JQQuant.git" -ForegroundColor Cyan
Write-Host "Branch: $branch" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Set remote URL with token temporarily
$tokenUrl = "https://${githubToken}@github.com/ZhuTechLLC/JQQuant.git"
git remote set-url origin $tokenUrl

# Push
Write-Host "Pushing to origin/$branch..." -ForegroundColor Cyan
git push -u origin $branch

$pushSuccess = $LASTEXITCODE -eq 0

# Clear token from remote URL (for security)
git remote set-url origin $cleanUrl

if ($pushSuccess) {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Green
    Write-Host "SUCCESS! Code pushed to GitHub" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "Repository URL: https://github.com/ZhuTechLLC/JQQuant" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Note: Token has been cleared from Git config for security." -ForegroundColor Yellow
    Write-Host "For future pushes, you can:" -ForegroundColor Yellow
    Write-Host "  1. Run this script again with -Token parameter" -ForegroundColor Yellow
    Write-Host "  2. Store token in github_token.txt and use -UseStoredToken" -ForegroundColor Yellow
    Write-Host "  3. Use Git credential manager: git config --global credential.helper wincred" -ForegroundColor Yellow
} else {
    Write-Host ""
    Write-Host "ERROR: Push failed" -ForegroundColor Red
    Write-Host ""
    Write-Host "Possible issues:" -ForegroundColor Yellow
    Write-Host "1. Token may be invalid or expired" -ForegroundColor Yellow
    Write-Host "2. Token may not have 'repo' scope" -ForegroundColor Yellow
    Write-Host "3. Repository may not exist or you don't have access" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Token has been cleared from Git config for security." -ForegroundColor Yellow
    exit 1
}

