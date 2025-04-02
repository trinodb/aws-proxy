# HttpCredentialsProvider Plugin

## Overview

The `HttpCredentialsProvider` plugin provides a way to retrieve AWS credentials via HTTP requests. This plugin is configurable and supports caching of the credentials.

## Configuration

The following table lists the configuration properties available for the `HttpCredentialsProvider`:

| Property                               | Description                                    | Default Value |
|----------------------------------------|------------------------------------------------|---------------|
| `credentials-provider.http.endpoint`   | The HTTP endpoint to retrieve the credentials. | None          |
| `credentials-provider.http.headers`    | Additional headers to include in requests.     | None          |
| `credentials-provider.http.cache-size` | The maximum size of the cache for credentials. | 0             |
| `credentials-provider.http.cache-ttl`  | The time-to-live for cache entries.            | 0s            |

## Example Configuration

Here is an example configuration for the `HttpCredentialsProvider`:

```properties
credentials-provider.type=http
credentials-provider.http.endpoint=https://example.com/api/v1/credentials
credentials-provider.http.headers=Authorization:Bearer token,Custom-Header:Value
credentials-provider.http.cache-size=100
credentials-provider.http.cache-ttl=5m
```

## OpenAPI Specification

The following OpenAPI specification defines the API for retrieving AWS credentials:

```yaml
openapi: 3.0.3
info:
  title: AWS Credentials Service
  description: API for retrieving AWS credentials
  version: 1.0.0
servers:
  - url: http://localhost:8080
paths:
  /{emulatedAccessKey}:
    get:
      summary: Get AWS Credentials
      description: Retrieve the AWS credentials based on path and query parameters
      parameters:
        - in: path
          name: emulatedAccessKey
          schema:
            type: string
          required: true
          description: The emulated access key
        - in: query
          name: sessionToken
          schema:
            type: string
          required: false
          description: The session token
      responses:
        '200':
          description: Successful response with AWS credentials
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/IdentityCredential'
        '404':
          description: Credentials not found
        '500':
          description: Internal server error
components:
  schemas:
    IdentityCredential:
      type: object
      properties:
        emulated:
          $ref: '#/components/schemas/Credential'
        identity:
          $ref: '#/components/schemas/Identity'
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
    Identity:
      type: object
      additionalProperties: true
      properties:
        user:
          type: string
        groups:
          type: array
          items:
            type: string
```
