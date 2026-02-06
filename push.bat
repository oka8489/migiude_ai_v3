@echo off
cd /d "%~dp0"
echo Pushing to GitHub...
echo.
git push origin master
echo.
if errorlevel 1 (
    echo Push failed.
) else (
    echo Push succeeded.
)
echo.
echo Press any key to close.
pause >nul
