#!/usr/bin/python

#imports
import json
import datetime
import os
import sys
import os
import threading
import time
from queue import *
import mysql.connector
from mysql.connector import errorcode

# configs
msconfig ={
        "host" : "localhost",
        "user" : "scriptUser",
        "password" : "scriptPassword",
        "database" : "dbname"
}

targetdir="./" #log file target
logFile = "log.log" # logfile name
errorFile = "error.log" # error file name


#vars
activecon = None # store active connection to msql here
action=None # store action function
threadLimit=1 # set max num threads

##connect to mysql
def Connect():
        logToFile("Connecting to mySQL")
        global activecon
        try:
                mysqlconn = mysql.connector.connect(**msconfig)
        except mysql.connector.Error as err:
                if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                        errorToFile("is Username/pass correct?")
                        sys.exit("Something is wrong with your user name or password")
                elif err.errno == errorcode.ER_BAD_DB_ERROR:
                        errorToFile("Cant find database")
                        sys.exit("Database does not exist")
                else:
                        errorToFile("database Error ")
                        sys.exit(err)
        else:
                global activecon
                activecon = mysqlconn
                return activecon


## check for connection
def isConnected():
        if activecon is not None:
                return True
        else:
                return False

# close connection if connected
def Disconnect():
        if isConnected():
                activeconn.Close()

# get a connection and make one if not already connected
def getConnection():
        if isConnected():
                return activecon
        else:
                return Connect()

# perform parse SQL query and return result
def queryVar(query,vars):
        conn = getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query,vars)
        data = cursor.fetchall()
        cursor.close()
        return data

# perform straight SQL query and return result
def query(querystring):
        conn = getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(querystring)

        for(data) in cursor:
                return(data)
        cursor.close()

# qry to get data, prefix to store file, name to save file name as
def queryConvertSave(qry,idprefix,name):
        qryData = queryVar(qry,(idprefix[0],))
        qryDataJson = json.dumps(qryData,default=str) ## add default=str to overcome datetime conversion
        writeToFile(qryDataJson,idprefix[1].lower()+name)

# do stuff for each line in file
def task1(inputData):      
    #do something here
    print("t1:"+inputData)

# do stuff for each line in file
def task2(inputData):      
    #do something here
    print("t2:"+inputData)


#UTILS
#### define core functions
def writeToFile(data,fileName):
        logToFile("Saving data for project "+fileName)
        try:
                with open(os.path.join(cefroot, fileName), "w") as file:
                        file.write(data)
        except OSError as e:
                errorToFile("Unable to write file for "+fileName)

def getDateString():
    ctime = datetime.datetime.now()
    strDate = ctime.strftime("%y-%m-%d-%H:%M_%S%f")
    return strDate

def logToFile(logtext):
    with open(os.path.join(targetdir, logFile), "a+") as file:
        file.write(getDateString()+":"+logtext+"\n")

def errorToFile(errorText):
    with open(os.path.join(targetdir, errorFile), "a+") as file:
        file.write(errorText+"\n")

#help text
if len(sys.argv) > 1:
    if sys.argv[1] == "help" or sys.argv[1] == "-h" :
        rtString = "Help: \n"
        if len(sys.argv) <= 2:
            rtString+="Available Commands:\n"
            rtString+="task1 - does task 1\n"
            rtString+="task2 - does task 2\n"
        else:
            if sys.argv[2] != None:
                if sys.argv[2] == 'task1':
                    rtString+= "do task 1\n"
                    rtString+= "input file csv [a,b,c]"
                elif sys.argv[2] == 'task2':
                    rtString+= "do task 2\n"
                    rtString+= "input file csv [id]"
        print(rtString)
        exit()


## thread batch control
def convertLinesToBatches(sf):
    f = open(sf, encoding='utf-8')
    batch = []
    for idx, val in enumerate(f):
        batch.append(val)
    return batch

#define callable methods
def getActionFunction(stringName):
    if stringName == 'task1':
        return task1
    elif stringName == 'task2':
        return task2
    else:
        print("Invalid Action name ["+stringName+"]")
        exit()

## MAIN ##
logToFile(sys.argv[0] +" init")

# get arguments
if len(sys.argv) < 3:
    strval='This program requires 2 arguments [Action] [infile] \n'
    strval+='For help use help or -h [action]\n'
    print (strval)
    exit()
else:
    sourcefile=sys.argv[2]
    action = getActionFunction(sys.argv[1])

batch = convertLinesToBatches(sourcefile)
logToFile(sys.argv[0] + "Created Batches")


def workerTask(workerInput):
     return action(workerInput)

# worker
def worker():
    while True:
        if q.empty():
            print("Queue empty Killing Thread["+str(threading.get_ident())+"]")
            exit()
        item = q.get()
        logToFile("Thread id ["+str(threading.get_ident())+"] getting Item from Queue")
        workerTask(item)
        print("Thread["+str(threading.get_ident())+"] Completed Task. Getting Item["+str(q.qsize())+"]")
        logToFile("Thread id ["+str(threading.get_ident())+"] Task Done: todo "+str(q.qsize()))
        q.task_done()

# make a queue
q = Queue()

# fill q wilt data
for b in batch:
    logToFile("Adding to Queue"+str(b));
    q.put(b)

# make threads
for i in range(threadLimit):
    logToFile("Making Thread :"+str(i))
    t = threading.Thread(target=worker)
    t.deamon = True
    t.start()

# block til all threads completed
q.join()

# make threads
for i in range(threadLimit):
    logToFile("CPF Making Thread :"+str(i))
    t = threading.Thread(target=worker)
    t.deamon = True
    t.start()

# block til all threads completed
q.join()




## always close con if connected (its good programming :)
if isConnected():
        logToFile("Closing Connection to MySQL")
        activecon.close()

logToFile(sys.argv[0] +" exit")
