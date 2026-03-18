@echo off

REM Force the console to use UTF-8
chcp 65001 > nul

REM Force Python to use UTF-8 for all input/output
set PYTHONIOENCODING=utf-8

REM Activate conda
call C:\Users\Admin\anaconda3\Scripts\activate.bat tejomaya

REM Go to project root  
cd /d C:\Users\Admin\Desktop\Indu\Tejomaya\Tejomaya_Akshayam

REM Run ETL
python -m agents.run_agents