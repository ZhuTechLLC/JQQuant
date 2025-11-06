# Git Installation Script
# This script helps install Git on Windows

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Git Installation Helper" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if Git is already installed
$gitInstalled = $false
try {
    $gitVersion = git --version 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Git is already installed: $gitVersion" -ForegroundColor Green
        Write-Host ""
        Write-Host "You can now run the push script:" -ForegroundColor Yellow
        Write-Host "  powershell -ExecutionPolicy Bypass -File push_with_token.ps1 -Token `"YOUR_GITHUB_TOKEN`"" -ForegroundColor Cyan
        exit 0
    }
} catch {
    $gitInstalled = $false
}

Write-Host "Git is not installed. Attempting to install..." -ForegroundColor Yellow
Write-Host ""

# Method 1: Try winget
if (Get-Command winget -ErrorAction SilentlyContinue) {
    Write-Host "Method 1: Using winget..." -ForegroundColor Cyan
    Write-Host "Installing Git via winget..." -ForegroundColor Yellow
    
    try {
        winget install --id Git.Git -e --source winget --accept-package-agreements --accept-source-agreements
        if ($LASTEXITCODE -eq 0) {
            Write-Host ""
            Write-Host "========================================" -ForegroundColor Green
            Write-Host "Git installed successfully!" -ForegroundColor Green
            Write-Host "========================================" -ForegroundColor Green
            Write-Host ""
            Write-Host "Please restart your terminal/PowerShell and run the push script again." -ForegroundColor Yellow
            Write-Host ""
            Write-Host "Command to run after restart:" -ForegroundColor Cyan
            Write-Host "  powershell -ExecutionPolicy Bypass -File push_with_token.ps1 -Token `"YOUR_GITHUB_TOKEN`"" -ForegroundColor Cyan
            exit 0
        }
    } catch {
        Write-Host "winget installation failed, trying other methods..." -ForegroundColor Yellow
    }
}

# Method 2: Try chocolatey
if (Get-Command choco -ErrorAction SilentlyContinue) {
    Write-Host ""
    Write-Host "Method 2: Using Chocolatey..." -ForegroundColor Cyan
    Write-Host "Installing Git via Chocolatey..." -ForegroundColor Yellow
    
    try {
        choco install git -y
        if ($LASTEXITCODE -eq 0) {
            Write-Host ""
            Write-Host "========================================" -ForegroundColor Green
            Write-Host "Git installed successfully!" -ForegroundColor Green
            Write-Host "========================================" -ForegroundColor Green
            Write-Host ""
            Write-Host "Please restart your terminal/PowerShell and run the push script again." -ForegroundColor Yellow
            exit 0
        }
    } catch {
        Write-Host "Chocolatey installation failed." -ForegroundColor Yellow
    }
}

# Method 3: Manual download instructions
Write-Host ""
Write-Host "========================================" -ForegroundColor Yellow
Write-Host "Manual Installation Required" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Yellow
Write-Host ""
Write-Host "Automatic installation methods are not available." -ForegroundColor Yellow
Write-Host "Please install Git manually:" -ForegroundColor Yellow
Write-Host ""
Write-Host "1. Download Git for Windows:" -ForegroundColor Cyan
Write-Host "   https://git-scm.com/download/win" -ForegroundColor White
Write-Host ""
Write-Host "2. Run the installer and follow the setup wizard" -ForegroundColor Cyan
Write-Host "   (Use default settings recommended)" -ForegroundColor Gray
Write-Host ""
Write-Host "3. After installation, restart your terminal/PowerShell" -ForegroundColor Cyan
Write-Host ""
Write-Host "4. Then run the push script:" -ForegroundColor Cyan
Write-Host "   powershell -ExecutionPolicy Bypass -File push_with_token.ps1 -Token `"YOUR_GITHUB_TOKEN`"" -ForegroundColor White
Write-Host ""
Write-Host "Opening download page in browser..." -ForegroundColor Yellow

# Try to open the download page
try {
    Start-Process "https://git-scm.com/download/win"
} catch {
    Write-Host "Could not open browser automatically. Please visit the URL above." -ForegroundColor Yellow
}

