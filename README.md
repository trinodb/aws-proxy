# trino-s3-proxy
Proxy for S3

To run testing server:

```shell
./mvnw clean package
./mvnw -pl trino-s3-proxy exec:java -Dexec.classpathScope=test -Dexec.mainClass=io.trino.s3.proxy.server.LocalServer <fake access key> <fake secret key> <remote S3 access key> <remote S3 secret key>
```

Make note of the logged `Endpoint`

To test out proxied APIs:

First, set up AWS CLI:

```shell
> aws configure
AWS Access Key ID [None]: enter the fake access key
AWS Secret Access Key [None]: enter the fake secret key
Default region name [None]: <enter>
Default output format [None]: <enter>
```

Now, make calls. E.g.

```shell
aws --endpoint-url http://<endpoint>/api/v1/s3Proxy/s3 s3 ls s3://<s3bucket>/<dir>

aws --endpoint-url http://<endpoint>/api/v1/s3Proxy/s3 s3 cp s3://<s3bucket>/<dir>/<file> .
```
