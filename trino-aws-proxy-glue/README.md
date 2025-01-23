# AWS Glue Emulation

Implementation of the AWS Glue endpoint and model serialization. Can be used as part of the
Trino AWS S3 Proxy or as part of a separate/standalone project.

## As part of Trino AWS S3 Proxy

Including the `trino-aws-proxy-glue` dependency will automatically add the AWS Glue plugin (via the
JDK Service Loader). See [configration and binding](#configuration-and-binding) below for more details.

## As part of a separate/standalone project

The AWS Glue endpoint implementation can be added to any Airlift application by installing
the `TrinoStandaloneGlueModule`.

## Configuration and binding

### Endpoint

The path to the Glue endpoint can be configured via the `aws.proxy.glue.path` configuration property.

### Handler

Bind an instance of `GlueRequestHandler` to handle Glue requests. Use the
`GlueRequestHandlerBinding` utility to do the binding. Your `GlueRequestHandler`
should examine the `ParsedGlueRequest` and handle any Glue requests and
return a Glue response.

E.g.

```java
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;

public class MyGlueRequestHandler
        implements GlueRequestHandler
{
    @Override
    public GlueResponse handle(ParsedGlueRequest request)
    {
        // validate credentials, etc. via: request.requestAuthorization()
        
        return switch (request.glueRequest()) {
            case GetDatabasesRequest getDatabasesRequest -> {
                // handle a GetDatabases request
                
                yield new GetDatabasesResponse.builder()
                        // ...
                        .build();
            }
            
            // ...
            
            default -> throw new WebApplicationException(NOT_FOUND);
        };
    }
}
```

E.g bind your handler

```java
Module module = binder -> {
    glueRequestHandlerBinding(binder)
            .bind(binding -> binding.to(MyGlueRequestHandler.class));
};
```
