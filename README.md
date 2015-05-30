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
Tez supported cascading 3.0 dependencies added, see them in pom.xml


# Run Example
TODO: 
