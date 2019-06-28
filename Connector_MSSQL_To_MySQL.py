#Define Imports
import pyodbc
import mysql.connector
import datetime
import os

##############################################################
################  Connector Settings #########################
##############################################################

# Edit all locations where there is a [<<<---] which [cont.]
# indicates file variable that will be used herein [cont.]
# these variables can also be stored in a variable [cont.]
# file and passed into the connector

#Program Variables
	#MSSQL Variables
msdriver = '\'Driver=msdriver;\''
msserver = '############' 			#<<<--- Place server host ID here
msdatabase = '############' 		#<<<--- Place database name here
mstableextension = '############'	#<<<--- Place database table name extension here
msconnectiontrust = 'yes'
mstable = '############'			#<<<--- Place Microsoft SQL Server table name here
msvar1 = '############'				#<<<--- Place first MSSQL variable here, NB: must be the ID
msvar2 = '############'				#<<<--- Place second MSSQL variable here
msvar3 ='############'				#<<<--- Place third MSSQL variable here
#Repeat as many variables as desired - Add variables to the following lines after [130,142,146,162,172,174]

	#MySQL Variables
myhost = '############'				#<<<--- Place host IP here without port
myusername = '############' 		#<<<--- Place username credentials here
mypassword = '############' 		#<<<--- Place password credentials here
mydatabase = '############' 		#<<<--- Place schema here
mytable = '############'			#<<<--- Place MySQL table name here
myvar1 = msvar1						#<<<--- Place first MySQL variable here, NB: By default this is the same as the MSSQL equivalent variable 
myvar2 = msvar2						#<<<--- Place second MySQL variable here, NB: By default this is the same as the MSSQL equivalent variable
myvar3 = msvar3						#<<<--- Place third MySQL variable here, NB: By default this is the same as the MSSQL equivalent variable 
#Repeat as many variables as desired - Add variables to the following lines after [174:150]

	#Error file Variables
filetitle = '############' 			#<<<--- Place error file name string here
filepath = '############' 			#<<<--- Place error file path here

	#Error table Variables -- requires pre-setting in MySQL server
#Can be uncommented out at lines 78 to 85
#Can be uncommented out at lines 207 to 214
		#Table Variables:
			# - Username,
			# - SystemInterfaceCode,
			# - EventString
errortable = '############'			#<<<--- Place error system table here

##############################################################
##############################################################

#Define Connections
conn = pyodbc.connect(msdriver + '\'Server=' + msserver + ';\'' + '\'Database= ' + msdatabase + ';\'' + '\'Trusted_Connection=' + msconnectiontrust + ';\'')
mydb = mysql.connector.connect(host=myhost, user=myusername, passwd=mypassword, database=mydatabase)

#Define Error File
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

filename = Year + Month + Day + '_' + Hour + Minute + Second + '_' + filetitle + '_Error_Log.txt'
NewFile = open(filepath + '\\' + filename,'a')

UniqueID = Year + Month + Day + '_' + Hour + Minute + Second + '_' + str(now.microsecond)

#Make connections
cursor = conn.cursor()
mycursor = mydb.cursor()

#### Program 1 ####
#Create entry log for start of program
#sql = "INSERT INTO " + errortable + "(Username,SystemInterfaceCode,EventString) VALUES ('SYS','" + str(UniqueID) + "','" + filetitle + ".py')"
#try:
#	mycursor.execute(sql)
#except:
#	NewFile.write('Failed To Execute: ' + sql + '\n')
#mydb.commit()

#### Program 2 ####
#Remove old data that is no longer is MS SQL
sql = """
SELECT
	   a.""" + msvar1 + """
FROM OPENQUERY(\"""" + mydatabase + """\",'SELECT * FROM """ + mytable + """') AS a
LEFT JOIN """ + msdatabase + "." + mstableextension + "." + mstable + """ AS b ON a.""" + msvar1 + " = b." + myvar1 + """
WHERE b.""" + myvar1 + " IS NULL"

mssqlresults = cursor.execute(sql)

for row in mssqlresults:
	msvar1Val = str(row[0])
	sql = "DELETE FROM " + mytable + " WHERE " + myvar1 + " = " + msVar1Val
	
	try:
		mycursor.execute(sql)
	except:
		NewFile.write('Failed To Execute: ' + sql + '\n')

mydb.commit()

#### Program 3 ####
#Clean the out of sync data
sql = """
SELECT 
	 a.""" + msvar1 + """
    ,a.""" + msvar2 + """
	,a.""" + msvar3 + """
FROM """ + msdatabase + "." + mstableextension + "." + mstable + """ AS a
LEFT JOIN OPENQUERY(\"""" + mydatabase + """\",'SELECT * FROM """ + mytable + """') AS b ON a.""" + msvar1 + " = b." + myvar1 + """
WHERE a.""" + msvar2 + " <>  b." + myvar2 + """
OR a.""" + msvar3 + " <> b." + myvar3


mssqlresults = cursor.execute(sql)

for row in mssqlresults:
	msvar1Val = str(row[0])
	msvar2Val = str(row[1])
	msvar3Val = str(row[2])

	sql = "UPDATE " + mydatabase + '.' + mytable + """
	SET """ + msvar2 + "= '" + msvar2Val + """'
	,""" + msvar3 + " = " + msvar3Val + """
	WHERE """ + msvar1 + " = " + msvar1Val
	
	try:
		mycursor.execute(sql)
	except:
		NewFile.write('Failed To Execute: ' + sql + '\n')

mydb.commit()

#### Program 4 ####
#Insert records that are not present
sql = """
SELECT 
	 a.""" + msvar1 + """
    ,a.""" + msvar2 + """
	,a.""" + msvar3 + """
FROM """ + msdatabase + "." + mstableextension + "." + mstable + """ AS a
LEFT JOIN OPENQUERY(\"""" + mydatabase + """\",'SELECT * FROM """ + mytable + """') AS b ON a.""" + msvar1 + " = b." + myvar1 + """
WHERE b.""" + myvar1 + " IS NULL"

mssqlresults = cursor.execute(sql)

for row in mssqlresults:
	msvar1Val = str(row[0])
	msvar2Val = str(row[1])
	msvar3Val = str(row[2])
	
	sql = "INSERT INTO " + mydatabase + '.' + mytable + "(" + myvar1 + "," + myvar2 + "," + myvar3 + ") VALUES('" + msvar1Val + "','" + msvar2Val + "','" + msvar3Val + "')"

	try:
		mycursor.execute(sql)
	except:
		NewFile.write('Failed To Execute: ' + sql + '\n')

mydb.commit()

#### Program 5 ####
#Create entry log for start of program
#sql = "INSERT INTO " + errortable + "(Username,EventString,PrimaryTrigger) VALUES ('SYS','" + filetitle + ".py','" + str(UniqueID) + "')"
#try:
#	mycursor.execute(sql)
#except:
#	NewFile.write('Failed To Execute: ' + sql + '\n')
#mydb.commit()

mycursor.close()
mydb.close()

cursor.close()
conn.close()

NewFile.close()

NewFile = open(filepath + '\\' + filename,'r')
contents = NewFile.read()
LengthContents = len(contents)
NewFile.close()

if LengthContents == 0:
	os.remove(filepath + '\\' + filename)
