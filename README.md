# trino-s3-proxy
Proxy for S3

To run testing server:

```shell
./mvnw package
./mvnw -pl trino-s3-proxy exec:java -Dexec.classpathScope=test -Dexec.mainClass=io.trino.s3.proxy.server.TestingTrinoS3ProxyServer
```
