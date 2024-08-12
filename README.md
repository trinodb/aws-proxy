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

## Module Configurations

### HTTP Credentials Provider

The HTTP credentials provider provides an option to include additional headers on requests sent to the HTTP service (e.g., for authentication).

These can be configured with `credentials-provider.http.headers`. This config entry is formatted as a comma-separated list of header names and values, where each entry is in the format `header-name:header-value`.

For instance, `header1:value1,header2:value2`.
If a header name or value should contain a comma, these can be escaped by doubling them (`,,` translates to a single comma in the literal header name or value, and is not treated as a separator).

E.g.: setting this config property to `"x-api-key: xyz,,123, Authorization: key,,,,123"` results in 2 headers:
- `x-api-key`: with value `xyz,123`
- `Authorization`: with value `key,,123`