# trino-aws-proxy
Proxy for AWS

## How to try it

### 1. Build

```shell
./mvnw -DskipTests clean install
```

### 2. Run

Start a testing Trino AWS proxy, a Postgres database container, a Minio object store container
and a Hive metastore container.

```shell
./mvnw exec:java -Dexec.mainClass=io.trino.aws.proxy.server.LocalServer -Dexec.classpathScope=test
```

Make note of the last few lines output to the console and copy:

- Metastore Server port
- Endpoint
- Access Key
- Secret Key

### 3. Test AWS CLI

In separate terminal...

```shell
> aws configure
-- enter the access and secret key
-- enter "us-east-1" for region

-- try AWS CLI commands
> aws --endpoint-url <endpoint from above> s3 ls
```

### 4 Test PySpark

In separate terminal...

```shell
-- start a PySpark container and then enter commands as shown
> docker run -it spark:3.5.1-scala2.12-java17-python3-ubuntu /opt/spark/bin/pyspark

# lots of text will output here

# The Spark default context needs to be stopped and re-created to
# point at the Trino AWS proxy:
spark.stop()

spark = SparkSession\
    .builder\
    .appName("test")\
    .config("hive.metastore.uris", "thrift://host.docker.internal:METASTORE-SERVER-PORT-GOES-HERE")\
    .enableHiveSupport()\
    .config("spark.hadoop.fs.s3a.endpoint", "ENDPOINT-GOES-HERE - REPLACE 127.0.0.1 with host.docker.internal")\
    .config("spark.hadoop.fs.s3a.access.key", "ACCESS-KEY-GOES-HERE")\
    .config("spark.hadoop.fs.s3a.secret.key", "SECRET-KEY-GOES-HERE")\
    .config("spark.hadoop.fs.s3a.path.style.access", True)\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", False)\
    .getOrCreate()
    
# try spark sql commands
```
