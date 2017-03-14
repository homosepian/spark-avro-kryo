# README #

This app contains a test for loading custom objects from avro files into Spark RDDs, using Kryo serialization.

### What is this repository for? ###

The code reproduces the following bug:
If the schema file used by the Avro-Reader refers to a non-existing class, the objects will be loaded as org.apache.avro.generic.GenericData$Record. But you  will not get a ClassCastException when fetching an AvroKey<MyCustomClass> and assigning it to a variable of this type:
```java
JavaPairRDD<MyCustomClassAvroKey, NullWritable> records =
                sc.newAPIHadoopFile(file.getAbsolutePath(),
                        MyCustomClassInputFormat.class, MyCustomClassAvroKey.class,
                        NullWritable.class,
                        sc.hadoopConfiguration());

MyCustomClassAvroKey firstAvroKey = records.first()._1;
``` 
You will only get an exception when trying to retrieve the enclosed MyCustomClass (which is really a GenericData$Record):
```java
MyCustomClass customObj = firstAvroKey.datum();
``` 

### How do I get set up? ###

* Install Spark 2.0.2 pre-built for Hadoop 2.7 by [downloading it](http://spark.apache.org/downloads.html) and unzipping it to your local file system

* Start Spark from the command line:
```sh
/path/to/spark-2.0.2-bin-hadoop2.7/sbin$ ./start-master.sh 
/path/to/spark-2.0.2-bin-hadoop2.7/sbin$ ./start-slave.sh spark://<server>:7077
``` 
Replace <server> with the one you see when you open http://localhost:8080/ after running start-master.sh

* Build by running `mvn package`

* Run the test in the [AppTest](https://github.com/homosepian/spark-avro-kryo/blob/master/src/test/java/com/test/AppTest.java) class. If you want to reproduce the bug, replace the schema file in [MyCustomClassInputFormat](https://github.com/homosepian/spark-avro-kryo/blob/master/src/main/java/com/test/serialization/MyCustomClassInputFormat.java) with the erroneous one (see comment in the code).
