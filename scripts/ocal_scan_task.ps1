# Task Scheduler wrapper for the ocal diary scan — runs python directly and
# captures ALL output (stdout+stderr) + the exit code into ocal_scan.log so a
# headless failure is diagnosable. Runs from a residential IP (Render is blocked).
$ErrorActionPreference = "Continue"
$repo = "C:\Users\zomer\CLAUDE CODE\Ckan versions"
$py   = "C:\Users\zomer\AppData\Local\Python\pythoncore-3.14-64\python.exe"
$log  = "$repo\ocal_scan.log"
$env:PYTHONIOENCODING = "utf-8"
Set-Location $repo
"" | Out-File -Append -Encoding utf8 $log
"===== $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') (scheduled task) =====" | Out-File -Append -Encoding utf8 $log
& $py -u "$repo\scripts\ocal_scan.py" 25 *>> $log
"exit code: $LASTEXITCODE" | Out-File -Append -Encoding utf8 $log
