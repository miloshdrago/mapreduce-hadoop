# Exercise 1: Spark Trip Lengths

## To compile : copy SparkTripLength.java to a directory (example: /home/r0780142/AS3/Exercice1)
cd /home/r0780142/AS3/Exercice1
javac -cp $(yarn classpath) SparkTripLength.java
jar cf Exercise1.jar *.class

## To be run locally
## the command takes two mandatory parameters :
## parameter 1 : path to input file containing the trips
## parameter 2 : path to output folder for the trip lengths distribution
cd /home/r0780142/AS3/Exercice1
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
export SPARK_INSTALL=/cw/bdap/software/spark-2.4.0-bin-hadoop2.7
export HADOOP_INSTALL=/cw/bdap/software/hadoop-3.1.2
export PATH=$PATH:$HADOOP_INSTALL/bin:$HADOOP_INSTALL/sbin
$SPARK_INSTALL/bin/spark-submit --class "SparkTripLength" --master local[1] Exercise1.jar file:///cw/bdap/assignment3/2010_03.trips.gz file:///home/r0780142/AS3/Exercice1/SparkOut


# Exercise 2: Hadoop Reconstructing trips and Airport trips revenue

## To compile: copy AirportTripsRevenue.java to a directory (example: /home/r0780142/AS3/Exercice2)
cd /home/r0780142/AS3/Exercice2
javac -cp $(yarn classpath) AirportTripsRevenue.java
jar cf Exercise2.jar *.class

## To be run on a cluster (pay attention to the exact HADOOP_CONF_DIR value for the cluster you run it on)
## the command takes three mandatory parameters and two optional parameters :
## parameter 1 : path to input file
## parameter 2 : path to output folder for the reconstructed airport trips
## parameter 3 : path to output folder for the daily revenue of airport trips
## optional parameter 4 : number of reducers for the first job to reconstruct the trips. When not provided the program will calculate it based on the number of node and available memory for containers.
## optional parameter 5 : split size in bytes of the input file, on the clusters should preferably be a multiple of 134217728. When not provided the program will determine itself the best split size and number of mappers
cd /home/r0780142/AS3/Exercice2
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre 
export HADOOP_INSTALL=/cw/bdap/software/hadoop-3.1.2 
export PATH=$PATH:$HADOOP_INSTALL/bin:$HADOOP_INSTALL/sbin 
export HADOOP_CONF_DIR=/localhost/NoCsBack/bdap/cluster
hadoop jar Exercise2.jar AirportTripsRevenue /data/all.segments /user/r0780142/out /user/r0780142/out2
