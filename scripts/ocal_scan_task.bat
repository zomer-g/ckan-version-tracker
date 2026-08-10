@echo off
REM ---------------------------------------------------------------------------
REM Windows Task Scheduler wrapper for the יומן לעם (Ocal) diary auto-import scan.
REM Must run from a RESIDENTIAL IP: odata.org.il's file downloads 403 datacenter
REM (Render) IPs, so the Render scheduler cannot import — this machine can.
REM Registered as the "OcalDiaryScan" task (every 6h). Logs to ocal_scan.log.
REM ---------------------------------------------------------------------------
set PYTHONIOENCODING=utf-8
cd /d "C:\Users\zomer\CLAUDE CODE\Ckan versions"
echo. >> "C:\Users\zomer\CLAUDE CODE\Ckan versions\ocal_scan.log"
echo ===== %DATE% %TIME% ===== >> "C:\Users\zomer\CLAUDE CODE\Ckan versions\ocal_scan.log"
"C:\Users\zomer\AppData\Local\Python\pythoncore-3.14-64\python.exe" -u scripts\ocal_scan.py 25 >> "C:\Users\zomer\CLAUDE CODE\Ckan versions\ocal_scan.log" 2>&1
