# 在浏览器中打开HTML教程
$htmlPath = Join-Path $PSScriptRoot "docs\index.html"
if (Test-Path $htmlPath) {
    Start-Process $htmlPath
    Write-Host "正在浏览器中打开教程..." -ForegroundColor Green
} else {
    Write-Host "错误: 找不到教程文件 $htmlPath" -ForegroundColor Red
}

