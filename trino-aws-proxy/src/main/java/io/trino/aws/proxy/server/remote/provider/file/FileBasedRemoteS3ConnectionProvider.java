/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.aws.proxy.server.remote.provider.file;

import com.google.common.io.Files;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.aws.proxy.spi.credentials.Identity;
import io.trino.aws.proxy.spi.remote.RemoteS3Connection;
import io.trino.aws.proxy.spi.remote.RemoteS3ConnectionProvider;
import io.trino.aws.proxy.spi.rest.ParsedS3Request;
import io.trino.aws.proxy.spi.signing.SigningMetadata;

import java.util.Map;
import java.util.Optional;

/**
 * <p>File-based RemoteS3ConnectionProvider that reads a JSON file containing a mapping from emulated access key to
 * RemoteS3Connection.</p>
 * <pre>{@code
 * {
 *   "emulated-access-key-1": {
 *     "remoteCredential": {
 *       "accessKey": "remote-access-key",
 *       "secretKey": "remote-secret-key"
 *     },
 *     "remoteSessionRole": {
 *       "region": "us-east-1",
 *       "roleArn": "arn:aws:iam::123456789012:role/role-name",
 *       "externalId": "external-id",
 *       "stsEndpoint": "https://sts.us-east-1.amazonaws.com"
 *     },
 *     "remoteS3FacadeConfiguration": {
 *       "remoteS3.https": true,
 *       "remoteS3.domain": "s3.amazonaws.com",
 *       "remoteS3.port": 443,
 *       "remoteS3.virtual-host-style": false,
 *       "remoteS3.hostname.template": "${domain}"
 *     }
 *   }
 * }
 * }</pre>
 */
public class FileBasedRemoteS3ConnectionProvider
        implements RemoteS3ConnectionProvider
{
    private final Map<String, RemoteS3Connection> remoteS3Connections;

    @Inject
    public FileBasedRemoteS3ConnectionProvider(FileBasedRemoteS3ConnectionProviderConfig config, JsonCodec<Map<String, RemoteS3Connection>> jsonCodec)
    {
        try {
            this.remoteS3Connections = jsonCodec.fromJson(Files.toByteArray(config.getConnectionsFile()));
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to read remote S3 connections file", e);
        }
    }

    @Override
    public Optional<RemoteS3Connection> remoteConnection(SigningMetadata signingMetadata, Optional<Identity> identity, ParsedS3Request request)
    {
        return Optional.ofNullable(remoteS3Connections.get(signingMetadata.credential().accessKey()));
    }
}
