echo off
set tsp=%1
set credential=%2
set action=%3

set py_script_name=app.py
set env_name=venv

E:
cd \Telematics-Ingestion

call %env_name%\Scripts\activate.bat
python %py_script_name% %tsp% %credential% %action%
set exit_code=%errorlevel%
call %env_name%\Scripts\deactivate.bat

exit /B %exit_code%