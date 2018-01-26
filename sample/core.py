# -*- coding: utf-8 -*-

import time, threading,linecache,multiprocessing

from kafka import KafkaConsumer, KafkaProducer

def checklog():


    #Check the last line read
    strTrackerFile = "LogLineCounter.txt"
    fileTrackerFile = open(strTrackerFile,"r")
    first_line=fileTrackerFile.readline()
    print(first_line)
    intLastReadLine=int(first_line)
    fileTrackerFile.close()

    #Check the file for how many lines there currently  are
    filename = "SampleLog.txt"
    myfile = open(filename)
    intLines = len(myfile.readlines())
    print "There are  " + str(intLines) + " in the file and I have read " + str(intLastReadLine)
    myfile.close


    while intLastReadLine < intLines+1:
        strLine = linecache.getline("SampleLog.txt",intLastReadLine)
        print(strLine)
        print(str(intLastReadLine) + ' vs ' + str(intLines))

        producer = KafkaProducer(bootstrap_servers='0020-UBUNTU-KAFKA:9092')
        producer.send('test_topic', strLine)

        intLastReadLine = intLastReadLine + 1
        fileTrackerFile=open("LogLineCounter.txt","w")
        fileTrackerFile.write(str(intLastReadLine))
        linecache.clearcache()
        producer.close()




    myfile.close()
    fileTrackerFile.close()
    threading.Timer(10, checklog).start()






