@echo off
echo ========================================================
echo   PUSHING TO GITHUB: mybullandbear/bull-bear-cloud
echo ========================================================
echo.
echo [1/4] Configuring Git...
git init
git config user.email "admin@bullbear.cloud"
git config user.name "BullBear Admin"
git branch -M main
git remote remove origin
git remote add origin https://github.com/mybullandbear/bull-bear-cloud.git

echo.
echo [2/4] Adding Files...
git add .

echo.
echo [3/4] Committing...
git commit -m "Auto-Deploy: Initial Setup"

echo.
echo [4/4] Pushing to GitHub...
echo.
echo NOTE: A browser window or popup may ask for your GitHub credentials.
echo.
git push -u origin master
git push -u origin main

echo.
echo ========================================================
echo   DONE! Check https://github.com/mybullandbear/bull-bear-cloud
echo ========================================================
pause
