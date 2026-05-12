@echo off
chcp 65001 >nul
setlocal EnableExtensions EnableDelayedExpansion

set "PROJECT_DIR=D:\chan-theory-project"
set "BRANCH=main"
set "DEFAULT_COMMIT_MSG=chore: sync local project"

title chan-theory-project GitHub sync

:MENU
cls
echo ================================================
echo   chan-theory-project GitHub 同步工具
echo ================================================
echo.
echo 项目目录: %PROJECT_DIR%
echo Git 分支: %BRANCH%
echo.
echo 1. 自动上传本地项目到 GitHub
echo 2. 自动从 GitHub 下载最新代码到本地
echo 3. 先 git pull，再 git add、commit、push
echo 4. 退出
echo.
set /p "CHOICE=请输入选项数字后按回车: "

if "%CHOICE%"=="1" goto UPLOAD
if "%CHOICE%"=="2" goto DOWNLOAD
if "%CHOICE%"=="3" goto PULL_THEN_UPLOAD
if "%CHOICE%"=="4" goto EXIT_SCRIPT

echo.
echo 输入无效，请输入 1、2、3 或 4。
pause
goto MENU

:CHECK_PROJECT
echo.
echo [检查] 正在检查项目目录...
if not exist "%PROJECT_DIR%\" (
    echo [失败] 项目目录不存在: %PROJECT_DIR%
    exit /b 1
)

cd /d "%PROJECT_DIR%"
if errorlevel 1 (
    echo [失败] 无法进入项目目录: %PROJECT_DIR%
    exit /b 1
)
echo [成功] 已进入项目目录。

echo.
echo [检查] 正在检查 Git...
git --version >nul 2>&1
if errorlevel 1 (
    echo [失败] 未找到 Git，请先安装 Git 并确认 git 命令可用。
    exit /b 1
)
echo [成功] Git 可用。

if not exist "%PROJECT_DIR%\.git\" (
    echo [失败] 当前目录不是 Git 仓库: %PROJECT_DIR%
    exit /b 1
)

echo.
echo [分支] 正在切换到 %BRANCH% 分支...
git checkout %BRANCH%
if errorlevel 1 (
    echo [失败] 切换到 %BRANCH% 分支失败。
    exit /b 1
)
echo [成功] 当前分支已准备好。
exit /b 0

:ASK_COMMIT_MESSAGE
echo.
set "COMMIT_MSG="
set /p "COMMIT_MSG=请输入 commit message，直接回车使用默认说明: "
if "%COMMIT_MSG%"=="" set "COMMIT_MSG=%DEFAULT_COMMIT_MSG%"
echo [信息] 本次 commit message: %COMMIT_MSG%
exit /b 0

:DO_UPLOAD
call :ASK_COMMIT_MESSAGE
if errorlevel 1 exit /b 1

echo.
echo [添加] 正在执行 git add -A...
git add -A
if errorlevel 1 (
    echo [失败] git add 执行失败。
    exit /b 1
)
echo [成功] 本地修改已加入暂存区。

echo.
echo [提交] 正在提交本地修改...
git diff --cached --quiet
if not errorlevel 1 (
    echo [提示] 没有需要提交的本地修改，跳过 commit。
) else (
    git commit -m "%COMMIT_MSG%"
    if errorlevel 1 (
        echo [失败] git commit 执行失败。
        exit /b 1
    )
    echo [成功] 本地修改已提交。
)

echo.
echo [上传] 正在推送到 GitHub: origin %BRANCH%...
git push origin %BRANCH%
if errorlevel 1 (
    echo [失败] git push 执行失败。请查看上方 Git 提示。
    exit /b 1
)
echo [成功] 已上传到 GitHub。
exit /b 0

:UPLOAD
cls
echo ================================================
echo   1. 自动上传本地项目到 GitHub
echo ================================================
call :CHECK_PROJECT
if errorlevel 1 goto END_WITH_PAUSE

call :DO_UPLOAD
if errorlevel 1 goto END_WITH_PAUSE

goto END_WITH_PAUSE

:DOWNLOAD
cls
echo ================================================
echo   2. 自动从 GitHub 下载最新代码到本地
echo ================================================
call :CHECK_PROJECT
if errorlevel 1 goto END_WITH_PAUSE

echo.
echo [下载] 正在执行 git pull origin %BRANCH%...
git pull origin %BRANCH%
if errorlevel 1 (
    echo [失败] git pull 执行失败。请查看上方 Git 提示。
    goto END_WITH_PAUSE
)
echo [成功] 已从 GitHub 下载最新代码。
goto END_WITH_PAUSE

:PULL_THEN_UPLOAD
cls
echo ================================================
echo   3. 先 git pull，再 git add、commit、push
echo ================================================
call :CHECK_PROJECT
if errorlevel 1 goto END_WITH_PAUSE

echo.
echo [下载] 正在先执行 git pull origin %BRANCH%...
git pull origin %BRANCH%
if errorlevel 1 (
    echo [失败] git pull 执行失败，已停止后续上传步骤。
    goto END_WITH_PAUSE
)
echo [成功] 已先从 GitHub 下载最新代码。

call :DO_UPLOAD
if errorlevel 1 goto END_WITH_PAUSE

goto END_WITH_PAUSE

:END_WITH_PAUSE
echo.
echo ================================================
echo 操作结束。请查看上方提示确认是否成功。
echo ================================================
pause
goto MENU

:EXIT_SCRIPT
echo.
echo 已退出 GitHub 同步工具。
pause
exit /b 0
