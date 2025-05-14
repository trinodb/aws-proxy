package io.trino.aws.proxy.server.remote;

import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.remote.RemoteS3Connection;
import io.trino.aws.proxy.spi.remote.RemoteS3Facade;
import io.trino.aws.proxy.spi.remote.RemoteSessionRole;

import java.util.Optional;

public record DefaultRemoteS3Connection(Credential remoteCredential, Optional<RemoteSessionRole> remoteSessionRole, Optional<DefaultRemoteS3Config> remoteS3Config)
        implements RemoteS3Connection
{
    @Override
    public RemoteS3Facade appliedRemoteS3Facade(RemoteS3Facade defaultRemoteS3Facade)
    {
        return remoteS3Config.map(config -> config.getVirtualHostStyle() ? new VirtualHostStyleRemoteS3Facade(config) : new PathStyleRemoteS3Facade(config))
                .orElse(defaultRemoteS3Facade);
    }
}
