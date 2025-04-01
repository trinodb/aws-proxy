# HttpRemoteS3ConnectionProvider Plugin

## Overview

The `HttpRemoteS3ConnectionProvider` plugin provides a way to retrieve remote S3 connection details via HTTP requests. This plugin is configurable and supports caching of the
connection details.

## Configuration

The following table lists the configuration properties available for the `HttpRemoteS3ConnectionProvider`:

| Property                                            | Description                                                     | Default Value |
|-----------------------------------------------------|-----------------------------------------------------------------|---------------|
| `remote-s3-connection-provider.http.endpoint`       | The HTTP endpoint to retrieve the remote S3 connection details. | None          |
| `remote-s3-connection-provider.http.request-fields` | The fields to include in the HTTP request query parameters.     | All fields    |
| `remote-s3-connection-provider.http.cache-size`     | The maximum size of the cache for remote S3 connections.        | 0             |
| `remote-s3-connection-provider.http.cache-ttl`      | The time-to-live for cache entries.                             | 1s            |

## Example Configuration

Here is an example configuration for the `HttpRemoteS3ConnectionProvider`:

```properties
remote-s3-connection-provider.type=http
remote-s3-connection-provider.http.endpoint=https://example.com/api/v1
remote-s3-connection-provider.http.request-fields=BUCKET,EMULATED_ACCESS_KEY
remote-s3-connection-provider.http.cache-size=100
remote-s3-connection-provider.http.cache-ttl=5m
```

## RequestQuery

The `RequestQuery` enum defines the fields that can be included in the HTTP request query parameters. Each field is associated with a `FieldSelector` that determines how the value
for the field is obtained. The available fields are:

- `BUCKET`: The bucket name from the `ParsedS3Request`.
- `KEY`: The key in the bucket from the `ParsedS3Request`.
- `EMULATED_ACCESS_KEY`: The access key from the `SigningMetadata`.
- `IDENTITY`: The identity in JSON format, if available.

## OpenAPI Specification

The following OpenAPI specification defines the API for retrieving remote S3 connection details:

```yaml
openapi: 3.0.3
info:
  title: Remote S3 Connection Service
  description: API for retrieving remote S3 connection details
  version: 1.0.0
servers:
  - url: http://localhost:8080
paths:
  /:
    get:
      summary: Get Remote S3 Connection
      description: Retrieve the remote S3 connection details based on query parameters
      parameters:
        - in: query
          name: bucket
          schema:
            type: string
          required: true
          description: The bucket name
        - in: query
          name: key
          schema:
            type: string
          required: true
          description: The key in the bucket
        - in: query
          name: emulatedAccessKey
          schema:
            type: string
          required: true
          description: The emulated access key
        - in: query
          name: identity
          schema:
            type: string
          required: false
          description: The identity in JSON format
      responses:
        '200':
          description: Successful response with remote S3 connection details
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/RemoteS3Connection'
        '404':
          description: Remote S3 connection not found
        '500':
          description: Internal server error
components:
  schemas:
    RemoteS3Connection:
      type: object
      properties:
        remoteCredential:
          $ref: '#/components/schemas/Credential'
        remoteSessionRole:
          $ref: '#/components/schemas/RemoteSessionRole'
        remoteS3FacadeConfiguration:
          type: object
          additionalProperties:
            type: string
    Credential:
      type: object
      properties:
        accessKey:
          type: string
        secretKey:
          type: string
        session:
          type: string
          nullable: true
    RemoteSessionRole:
      type: object
      properties:
        region:
          type: string
        roleArn:
          type: string
        externalId:
          type: string
          nullable: true
        stsEndpoint:
          type: string
          format: uri
          nullable: true
```
