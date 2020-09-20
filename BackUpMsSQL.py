# Last Updated 20/09/2020
import pyodbc
import datetime
import os
from ftplib import FTP

def putFileOnFtp(filename):
	#domain name or server ip:
	ftp = FTP('35.226.76.232')
	ftp.login(user='myftp', passwd = 'LazySusan201810')
	ftp.set_pasv(True)
	ftp.cwd("/files/Sage_BackUp/")

	#print("File List: ")
	#files = ftp.dir()
	#print(files)

	# Fetch File
	#filename = '20200519_100344_BackUpSage200_Live.dbo.AllocationBalance_FileOutput_1.csv'
	#localfile = open('C:\\Users\\marcel.mulders\\Dropbox\\Scripts\\SageServerCode\\' + filename, 'wb')
	#ftp.retrbinary('RETR ' + filename, localfile.write, 1024)
	#ftp.quit()
	#localfile.close()

	# Put File
	#filename = 'exampleFile.txt'
	ftp.storbinary('STOR '+filename, open("Z:\\TMP_STORAGE_FTP\\MSSQL_BACKUP_CSV\\" + filename, 'rb'))
	ftp.quit()
	


def fetchTable(table_name):
	FileTitle = 'BackUp' + table_name
	now = datetime.datetime.now()
	Year = str(now.year)
	if now.month < 10:
		Month = '0' + str(now.month)
	else:
		Month = str(now.month)
	if now.day < 10:
		Day = '0' + str(now.day)
	else:
		Day = str(now.day)
	if now.hour < 10:
		Hour = '0' + str(now.hour)
	else:
		Hour = str(now.hour)
	if now.minute < 10:
		Minute = '0' + str(now.minute)
	else:
		Minute = str(now.minute)
	if now.second < 10:
		Second = '0' + str(now.second)
	else:
		Second = str(now.second)
		
	filepath = "Z:\\TMP_STORAGE_FTP\\MSSQL_BACKUP_CSV"
	file_count = 1
	filename = Year + Month + Day + '_' + Hour + Minute + Second + '_' + FileTitle + '_FileOutput' + '_' + str(file_count) + '.csv'
	NewFile = open(filepath + '\\' + filename,'a')

	print('')
	now = datetime.datetime.now()
	conn = pyodbc.connect("DSN=SageData2;UID=LSF\marcel.mulders;PWD=LazySusan201810")
	cursor = conn.cursor()

	sql = "SELECT * FROM " + table_name + " WITH (NOLOCK)"
	ms_results = cursor.execute(sql)

	seperator = '<<>>'
	a = 0
	b = 0
	for row in ms_results:
		a = a + 1
		b = b + 1
		if b > 10000:
			NewFile.close()
			file_count = file_count + 1
			filename = Year + Month + Day + '_' + Hour + Minute + Second + '_' + FileTitle + '_FileOutput' + '_' + str(file_count) + '.csv'
			NewFile = open(filepath + '\\' + filename,'a')
			b = 0
		print('Inserting Row: ' + str(a))
		line = ''
		for value in row:
			#print(value)
			line = line + seperator + str(value)
		NewFile.write(line + '<<line_end>>' + '\n')

	cursor.close()
	conn.close()
	NewFile.close()
	
def getTables():
	conn = pyodbc.connect("DSN=SageData2;UID=LSF\marcel.mulders;PWD=LazySusan201810")
	cursor = conn.cursor()
	sql = """
	SELECT
		 CONCAT(TABLE_CATALOG,'.',TABLE_SCHEMA,'.',TABLE_NAME) AS TableName
	FROM Sage200_Live.INFORMATION_SCHEMA.TABLES
	WHERE TABLE_TYPE = 'BASE TABLE'
	ORDER BY TABLE_NAME
	"""
	myArray = []
	ms_results = cursor.execute(sql)
	for row in ms_results:
		#print(row[0])
		myArray.append(row[0])
	cursor.close()
	conn.close()
	return myArray
	
# Program Start
myArray = getTables()
for row in myArray:
	print("Storing "+row)
	fetchTable(row)
	
folder = os.listdir("Z:\\TMP_STORAGE_FTP\\MSSQL_BACKUP_CSV")
	
for file in folder:
	print("Uploaing to FTP "+file)
	putFileOnFtp(file)
	os.remove("Z:\\TMP_STORAGE_FTP\\MSSQL_BACKUP_CSV\\" + file)
