Note 
python 3.10
ODBC driver 18  

API may error  because  limited request per day.
Excel may error because  openpyxl.

#  HOW Airflow

https://pitch-tile-2ef.notion.site/Install-Airflow-in-Docker-4a3648a5e1054624b922f31cfe48eb4a?pvs=4


# fix openpyxl in cmd

https://pitch-tile-2ef.notion.site/error-Excel-openpyxl-a1d5515daf2643c88ff0735da7aca039?pvs=4

# after clone please

Remove "  from config import DB_USERNAME, DB_PASSWORD, DB_SERVERNAME  " in dags in every pipline  (normally found in line 1)
and replace with real  USERNAME ,PASSWORD, SERVERNAME (normally found in  line 35+++)

provide docker-compose.yaml already that mean skip      init

# to start docker and airflow web  (normally found docker port 8080)
 1st  time may take some time 
##
docker compose up   

 # to pause and docker compose up again do not use down  down=delete
##
docker compose stop




