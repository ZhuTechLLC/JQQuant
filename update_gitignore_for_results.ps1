# Script to update .gitignore for results folder
# This script helps you decide what to include from results folder

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Results Folder Upload Options" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "Current situation:" -ForegroundColor Yellow
Write-Host "  - results/ folder is excluded in .gitignore" -ForegroundColor Gray
Write-Host "  - Contains HTML reports and JSON backtest data" -ForegroundColor Gray
Write-Host ""

Write-Host "Options:" -ForegroundColor Yellow
Write-Host ""
Write-Host "1. Upload everything in results/ (may be large)" -ForegroundColor Cyan
Write-Host "2. Upload only documentation files (README.md, view_report.html, etc.)" -ForegroundColor Cyan
Write-Host "3. Upload HTML reports but exclude JSON data files" -ForegroundColor Cyan
Write-Host "4. Keep results/ excluded (current setting)" -ForegroundColor Cyan
Write-Host ""

$choice = Read-Host "Enter your choice (1-4)"

switch ($choice) {
    "1" {
        Write-Host ""
        Write-Host "Removing results/ from .gitignore..." -ForegroundColor Cyan
        $gitignore = Get-Content ".gitignore" -Raw
        $gitignore = $gitignore -replace "(?m)^results/$\r?\n", ""
        Set-Content ".gitignore" -Value $gitignore -NoNewline
        Write-Host "✓ results/ will now be included in commits" -ForegroundColor Green
        Write-Host ""
        Write-Host "Next steps:" -ForegroundColor Yellow
        Write-Host "  1. Run: git add results/" -ForegroundColor Cyan
        Write-Host "  2. Run: git commit -m 'Add results folder'" -ForegroundColor Cyan
        Write-Host "  3. Run: git push" -ForegroundColor Cyan
    }
    "2" {
        Write-Host ""
        Write-Host "Updating .gitignore to allow only documentation files..." -ForegroundColor Cyan
        $gitignore = Get-Content ".gitignore" -Raw
        # Remove results/ line
        $gitignore = $gitignore -replace "(?m)^results/$\r?\n", ""
        # Add specific exclusions
        $gitignore += "`n# Results folder - exclude data files, allow documentation`n"
        $gitignore += "results/*.json`n"
        $gitignore += "results/*_report_*.html`n"
        $gitignore += "!results/README.md`n"
        $gitignore += "!results/view_report.html`n"
        $gitignore += "!results/report_template.html`n"
        $gitignore += "!results/strategy_comparison.md`n"
        Set-Content ".gitignore" -Value $gitignore -NoNewline
        Write-Host "✓ Only documentation files will be included" -ForegroundColor Green
        Write-Host ""
        Write-Host "Next steps:" -ForegroundColor Yellow
        Write-Host "  1. Run: git add results/README.md results/view_report.html results/report_template.html results/strategy_comparison.md" -ForegroundColor Cyan
        Write-Host "  2. Run: git commit -m 'Add results documentation files'" -ForegroundColor Cyan
        Write-Host "  3. Run: git push" -ForegroundColor Cyan
    }
    "3" {
        Write-Host ""
        Write-Host "Updating .gitignore to allow HTML reports but exclude JSON..." -ForegroundColor Cyan
        $gitignore = Get-Content ".gitignore" -Raw
        # Remove results/ line
        $gitignore = $gitignore -replace "(?m)^results/$\r?\n", ""
        # Add JSON exclusion
        $gitignore += "`n# Results folder - exclude JSON data files`n"
        $gitignore += "results/*.json`n"
        Set-Content ".gitignore" -Value $gitignore -NoNewline
        Write-Host "✓ HTML reports will be included, JSON files excluded" -ForegroundColor Green
        Write-Host ""
        Write-Host "Next steps:" -ForegroundColor Yellow
        Write-Host "  1. Run: git add results/" -ForegroundColor Cyan
        Write-Host "  2. Run: git commit -m 'Add results HTML reports'" -ForegroundColor Cyan
        Write-Host "  3. Run: git push" -ForegroundColor Cyan
    }
    "4" {
        Write-Host ""
        Write-Host "Keeping results/ excluded (no changes)" -ForegroundColor Yellow
        exit 0
    }
    default {
        Write-Host ""
        Write-Host "Invalid choice. Exiting." -ForegroundColor Red
        exit 1
    }
}

Write-Host ""
Write-Host "Done! You can now add and commit the results folder." -ForegroundColor Green


