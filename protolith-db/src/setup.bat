@echo off

REM Define constants
SET APP_USER=protolithdb
SET APP_DIR=C:\ProgramData\ProtolithDB\db

REM Create a dedicated user
echo Creating a dedicated user for the application...
net user %APP_USER% /add

REM Create necessary directories and set permissions
echo Setting up directories...
mkdir %APP_DIR%
icacls %APP_DIR% /grant %APP_USER%:(OI)(CI)F

REM Additional setup tasks here...

echo Installation and setup completed successfully.