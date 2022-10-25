#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pyodbc
import csv
import datetime 
import time
import os
import sys
reload(sys) 
sys.setdefaultencoding('cp1252')

server = sys.argv[1]
database = sys.argv[2]
username = sys.argv[3]
password = sys.argv[4]
query =  sys.argv[5]
motivo = sys.argv[6]

cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cnxn.timeout = 30
cursor = cnxn.cursor()
cursor.execute('SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED')

startDuration = time.time()
start = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')

cursor.execute(query)
data = cursor.fetchall()

endDuration = time.time()
end = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S’)
duration = int((endDuration - startDuration) * 1000) #ms

outputfile = database + ".csv"
logfile = database + ".txt"

with open(outputfile, 'w') as fp:
    a = csv.writer(fp, delimiter='|')
    a.writerow([x[0] for x in cursor.description])
    for line in data:
        a.writerow(line)
        
querycount = 'SELECT @@ROWCOUNT'
cursor.execute(querycount)
rowcount = cursor.fetchone()[0]
if rowcount > 10000:
    sys.exit('Máximo de 10.000 linhas por resultado!')
with open(logfile, 'w') as fp:
    fp.write('\r\n’) 
    fp.write('Rows: ' + str(rowcount))
    fp.write('\r\n') 
    fp.write('Duration: ' + str(duration) + ' ms')
    fp.write('\r\n') 
    fp.write('Start time: ' + start)
    fp.write('\r\n') 
    fp.write('End time: ' + end)
    fp.write('\r\n') 
    fp.write('Executed by: ' + sys.argv[7])
    fp.write('\r\n') 
    fp.write('Build#: ' + sys.argv[8])
    fp.write('\r\n') 
    fp.write('\r\nMotivo: ' + motivo)
    fp.write('\r\n') 
    fp.write('\r\nQuery: ' +  query)

# Close and delete cursor
cursor.close()
del cursor
# Close Connection
cnxn.close()
