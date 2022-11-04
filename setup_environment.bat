REM echo off
set env_name=venv
python -m venv %env_name%
call %env_name%\Scripts\activate.bat
pip install -r .\requirements.txt --use-feature=2020-resolver
call %env_name%\Scripts\deactivate.bat
pause