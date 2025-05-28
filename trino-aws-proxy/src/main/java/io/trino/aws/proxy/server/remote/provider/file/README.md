# FileBasedRemoteS3ConnectionProvider Plugin

## Overview

The `FileBasedRemoteS3ConnectionProvider` plugin reads remote S3 connection details from a JSON file. This plugin is configured via a file path and supports a flexible JSON mapping
of access keys to connection details.

## Configuration

The following property is available for the `FileBasedRemoteS3ConnectionProvider`:

| Property                          | Description                                                     | Default Value |
|-----------------------------------|-----------------------------------------------------------------|---------------|
| `remote-s3.connections-file-path` | The path to the JSON file containing the S3 connection mapping. | None          |

## Example Configuration

Below is an example configuration for the `FileBasedRemoteS3ConnectionProvider`:

```properties
remote-s3-connection-provider.type=file
remote-s3.connections-file-path=/path/to/your/connections.json
```

## JSON File Format

The JSON file should map an emulated access key to its corresponding S3 connection details. For example:

```json
{
  "emulated-access-key-1": {
    "remoteCredential": {
      "accessKey": "remote-access-key",
      "secretKey": "remote-secret-key"
    },
    "remoteSessionRole": {
      "region": "us-east-1",
      "roleArn": "arn:aws:iam::123456789012:role/role-name",
      "externalId": "external-id",
      "stsEndpoint": "https://sts.us-east-1.amazonaws.com"
    },
    "remoteS3FacadeConfiguration": {
      "remoteS3.https": true,
      "remoteS3.domain": "s3.amazonaws.com",
      "remoteS3.port": 443,
      "remoteS3.virtual-host-style": false,
      "remoteS3.hostname.template": "${domain}"
    }
  }
}
```

// ...existing content if any...
