# Last Updated 20/09/2020
#Command Line
#"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql" -u root --password=root -h 80.244.176.131:2020 --execute="SELECT now()"
import pyodbc
import datetime
import os
from ftplib import FTP


def putFileOnFtp(filename):
	#domain name or server ip:
	ftp = FTP('35.226.76.232')
	ftp.login(user='myftp', passwd = 'LazySusan201810')
	ftp.set_pasv(True)
	ftp.cwd("/files/Website_BackUp/")

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
	ftp.storbinary('STOR '+filename, open("Z:\\TMP_STORAGE_FTP\\MYSQL_WEBSITE_BACKUP_CSV\\" + filename, 'rb'))
	ftp.quit()

def fetchTable():
	#dircontents = os.listdir("Z:\\TMP_STORAGE_FTP\\MYSQL_WEBSITE_BACKUP_CSV")

	#for file in dircontents:
	#	print(file.replace(".csv",""))

	conn = pyodbc.connect('DSN=Shopware1',autocommit=True)

	#------- FETCH 1 ---------
	cursor = conn.cursor()
	sql = "USE lazysusan_live"
	cursor.execute(sql)
	sql = "SHOW TABLES"
	MySQL_Results1 = cursor.execute(sql)
	#------ END FETCH 1 --------

	myTablesArray = []

	for myrow in MySQL_Results1:
		mytable = myrow[0]
		myTablesArray.append(mytable)

	for thisRow in myTablesArray:
		mytable = thisRow
		
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
		
		filename = Year + Month + Day + '_' + Hour + Minute + Second + "_BackUp_" + mytable + "_FileOutput.csv"
		
		goforth = True
		
		#for file in dircontents:
		#	file = file.replace(".csv","")
		#	if file == str(mytable):
		#		goforth = False
		
		print("For: " + str(mytable) + " " + str(goforth))
		
		if goforth == True:
			newfile = open("Z:\\TMP_STORAGE_FTP\\MYSQL_WEBSITE_BACKUP_CSV" + "\\" + filename,'a',encoding="utf8")
			
			#------- FETCH 2 ---------
			sql = "SHOW COLUMNS FROM lazysusan_live." + mytable
			MySQL_Results2 = cursor.execute(sql)
			#------ END FETCH 2 --------
			
			header = ""
			for item in MySQL_Results2:
				column = item[0]
				header = header + "|" + column
			header = header[1:len(header)]
			
			newfile.write(header)
			newfile.write("\n")
			#------- FETCH 3 ---------
			sql = "SELECT * FROM lazysusan_live." + mytable
			MySQL_Results3 = cursor.execute(sql)
			#------ END FETCH 3 --------
			
			i = 1
			for line in MySQL_Results3:
				print("   " + str(i))
				i = i + 1
				row = ""
				for item in line:
					item = str(item)
					item = item.replace("|","")
					item = item.replace(",","<comma>")
					item = item.replace("'","<apos>")
					item = item.replace("\r\n","<newline>").replace("\r","<newline>").replace("\n","<newline>")
					row = row + "|" + item
					
				row = row[1:len(row)]
				row = row.replace("None","")
				
				newfile.write(row)
				newfile.write("\n")
			
			newfile.close()

	cursor.close()	
	conn.close()
	
# Program start Fetch CSV files
fetchTable()

folder = os.listdir("Z:\\TMP_STORAGE_FTP\\MYSQL_WEBSITE_BACKUP_CSV")
	
for file in folder:
	print("Uploaing to FTP "+file)
	putFileOnFtp(file)
	os.remove("Z:\\TMP_STORAGE_FTP\\MYSQL_WEBSITE_BACKUP_CSV\\" + file)
