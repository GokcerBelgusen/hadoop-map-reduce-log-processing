#!/usr/bin/env bash
#DELETE old java classes
rm -rf apachelogs-classes/
rm apachelogs.jar

#COMPILE the classes
mkdir apachelogs-classes
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop/client-0.20/* -d apachelogs-classes/ hadoop-map-reduce/src/main/java/org/tzc/apachelogs/*.java

#MAKE the jar
jar -cvf apachelogs.jar -C apachelogs-classes/ .

#LIST and DELETE stuff
hadoop fs -ls /user/cloudera/apachelogs/input
hadoop fs -ls /user/cloudera/apachelogs/output
hadoop fs -rm -f -r /user/cloudera/apachelogs/output

#RUN HADOOP
hadoop jar apachelogs.jar org.tzc.apachelogs.ProcessLogs /user/cloudera/apachelogs/input /user/cloudera/apachelogs/output
