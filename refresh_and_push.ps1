# Refresh PATH and Push Script
# This script refreshes the PATH environment variable and then runs the push script

Write-Host "Refreshing PATH environment variable..." -ForegroundColor Cyan

# Refresh PATH from registry
$env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")

# Common Git installation paths
$gitPaths = @(
    "C:\Program Files\Git\cmd\git.exe",
    "C:\Program Files (x86)\Git\cmd\git.exe",
    "$env:LOCALAPPDATA\Programs\Git\cmd\git.exe",
    "$env:ProgramFiles\Git\cmd\git.exe"
)

# Try to find Git
$gitFound = $false
foreach ($gitPath in $gitPaths) {
    if (Test-Path $gitPath) {
        $gitDir = Split-Path (Split-Path $gitPath) -Parent
        $env:Path = "$gitDir\cmd;$env:Path"
        Write-Host "Found Git at: $gitPath" -ForegroundColor Green
        $gitFound = $true
        break
    }
}

# Check if Git is now available
try {
    $gitVersion = git --version 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Git is now available: $gitVersion" -ForegroundColor Green
        Write-Host ""
        
        # Run the push script
        Write-Host "Running push script..." -ForegroundColor Cyan
        Write-Host ""
        
        & powershell -ExecutionPolicy Bypass -File push_with_token.ps1 -Token "YOUR_GITHUB_TOKEN"
    } else {
        throw "Git command failed"
    }
} catch {
    Write-Host ""
    Write-Host "ERROR: Git is still not available" -ForegroundColor Red
    Write-Host ""
    Write-Host "Please try one of the following:" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "1. Restart your terminal/PowerShell window" -ForegroundColor Cyan
    Write-Host "   (This will refresh the PATH environment variable)" -ForegroundColor Gray
    Write-Host ""
    Write-Host "2. Or manually add Git to PATH:" -ForegroundColor Cyan
    Write-Host "   - Find Git installation (usually C:\Program Files\Git)" -ForegroundColor Gray
    Write-Host "   - Add C:\Program Files\Git\cmd to your PATH" -ForegroundColor Gray
    Write-Host ""
    Write-Host "3. Or reinstall Git:" -ForegroundColor Cyan
    Write-Host "   - Run: winget install --id Git.Git -e --source winget" -ForegroundColor Gray
    Write-Host "   - Then restart terminal" -ForegroundColor Gray
    Write-Host ""
    exit 1
}

