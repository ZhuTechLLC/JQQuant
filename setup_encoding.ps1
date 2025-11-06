# -*- coding: utf-8 -*-
# PowerShell脚本：设置UTF-8编码环境

Write-Host "正在配置UTF-8编码环境..." -ForegroundColor Green

# 设置PowerShell输出编码为UTF-8
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::InputEncoding = [System.Text.Encoding]::UTF8

# 设置环境变量
$env:PYTHONIOENCODING = "utf-8"
$env:PYTHONUTF8 = "1"

Write-Host "UTF-8编码环境配置完成！" -ForegroundColor Green
Write-Host ""
Write-Host "当前编码设置:" -ForegroundColor Yellow
Write-Host "  OutputEncoding: $($OutputEncoding.EncodingName)"
Write-Host "  Console.OutputEncoding: $([Console]::OutputEncoding.EncodingName)"
Write-Host "  PYTHONIOENCODING: $env:PYTHONIOENCODING"
Write-Host "  PYTHONUTF8: $env:PYTHONUTF8"
Write-Host ""
Write-Host "提示: 这些设置仅在当前PowerShell会话中有效。" -ForegroundColor Cyan
Write-Host "如需永久设置，请将以下内容添加到PowerShell配置文件中:" -ForegroundColor Cyan
Write-Host "  $PROFILE" -ForegroundColor White

