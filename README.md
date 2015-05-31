# jcascalog-parquet-tez-example
This example shows that JCascalog using Parquet Avro Scheme runs on Tez.

# Dependencies
To run this example, many dependencies relating to Tez are necessary.

## Tez Supported Cascalog
Tez supported cascalog can be found here: https://github.com/mykidong/cascalog

After building cascalog-core, this dependency jar added to this example, see it in pom.xml:

```xml
<dependency>
	<groupId>cascalog</groupId>
	<artifactId>cascalog-core-tez</artifactId>
	<version>3.0.0-SNAPSHOT</version>
</dependency>
```
See also the built cascalog tez jar in src/test/resources/lib.


## Parquet Avro Scheme
To handle Parquet file with avro schema(https://github.com/mykidong/cascading-parquet-avro-scheme), the following jar added:

```xml
<dependency>
	<groupId>cascading-scheme</groupId>
	<artifactId>cascading-parquet-avro-scheme</artifactId>
	<version>0.1.0-SNAPSHOT</version>
</dependency>
```

## Cascading Tez Dependencies
Tez supported Cascading 3.0 dependencies added, see them in pom.xml


# Run Example
To run example, follow the next steps.

## Create input, output directory in HDFS, and copy local file to HDFS

	hdfs dfs -mkdir -p /test/json;
	hdfs dfs -mkdir -p /test/ive;
	hdfs dfs -mkdir -p /test/ive-2;
	hdfs dfs -put src/main/resources/data/event-sample.data /test/json;
	
## Run JsonToParquetWrite

Build hadoop job jar file:

	mvn -e -DskipTests=true clean install assembly:single -P cascalog;
	
	
Tez could throw Class Not Found Exception, thus copy hadoop job jar file to Tez classpath in HDFS:

	hdfs dfs -rm -R -f /apps/tez/jcascalog-parquet-tez-example-*-hadoop-job.jar;
	hdfs dfs -put target/jcascalog-parquet-tez-example-*-hadoop-job.jar /apps/tez;
	
	
To run JsonToParquetWrite, [defaultFS] is name service url, for instance, 'hdfs://name-service-url', [resourceManagerHost] is resource manager host name, [queueName] is yarn queue name, if this omitted, queue name is 'default'.

	yarn jar target/jcascalog-parquet-tez-example-*-hadoop-job.jar jcascalog.example.JsonToParquetWrite /test/json /test/ive [defaultFS] [resourceManagerHost] [queueName]
	
	

## Run ParquetSpecifiedColumnReadWrite

	mvn -e -DskipTests=true clean install assembly:single -P cascalog;
	hdfs dfs -rm -R -f /apps/tez/jcascalog-parquet-tez-example-*-hadoop-job.jar;
	hdfs dfs -put target/jcascalog-parquet-tez-example-*-hadoop-job.jar /apps/tez;
	yarn jar target/jcascalog-parquet-tez-example-*-hadoop-job.jar jcascalog.example.ParquetSpecifiedColumnReadWrite /test/ive /test/ive-2 [defaultFS] [resourceManagerHost] [queueName]
	


