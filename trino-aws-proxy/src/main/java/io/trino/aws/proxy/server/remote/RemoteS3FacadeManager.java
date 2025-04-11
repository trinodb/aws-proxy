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
package io.trino.aws.proxy.server.remote;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.aws.proxy.spi.remote.RemoteS3Facade;
import io.trino.aws.proxy.spi.remote.RemoteS3FacadeFactory;
import jakarta.ws.rs.core.UriBuilder;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;

public class RemoteS3FacadeManager
        implements RemoteS3Facade
{
    private static final Logger log = Logger.get(RemoteS3FacadeManager.class);

    private static final File REMOTE_S3_FACADE_CONFIGURATION = new File("etc/remote-s3-facade.properties");
    private static final String REMOTE_S3_FACED_NAME = "remote-s3-facade.name";

    private final Map<String, RemoteS3FacadeFactory> factories = new ConcurrentHashMap<>();
    private final AtomicReference<RemoteS3Facade> defaultConfiguredRemoteS3Facade = new AtomicReference<>();

    @Inject
    public RemoteS3FacadeManager(Set<RemoteS3FacadeFactory> factories)
    {
        for (RemoteS3FacadeFactory factory : factories) {
            String name = factory.name();
            if (this.factories.putIfAbsent(name, factory) != null) {
                throw new IllegalArgumentException("Remote S3 Facade " + name + " is already registered");
            }
            log.info("Registering RemoteS3Facade factory %s", name);
        }
    }

    public void loadDefaultRemoteS3Facade()
    {
        checkState(REMOTE_S3_FACADE_CONFIGURATION.exists(), "RemoteS3Facade configuration file [%s] does not exist", REMOTE_S3_FACADE_CONFIGURATION.getAbsolutePath());
        Map<String, String> properties;
        try {
            properties = new HashMap<>(loadPropertiesFrom(REMOTE_S3_FACADE_CONFIGURATION.getPath()));
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to load RemoteS3Facade configuration file " + REMOTE_S3_FACADE_CONFIGURATION.getAbsolutePath(), e);
        }
        setDefaultRemoteS3Facade(createRemoteS3Facade(properties));
    }

    public RemoteS3Facade createRemoteS3Facade(Map<String, String> properties)
    {
        properties = new HashMap<>(properties);
        String remoteS3FacadeName = Optional.ofNullable(properties.remove(REMOTE_S3_FACED_NAME)).orElse("default");
        RemoteS3FacadeFactory factory = factories.get(remoteS3FacadeName);
        checkState(factory != null, "RemoteS3Facade factory %s not registered", remoteS3FacadeName);
        return factory.create(properties);
    }

    @VisibleForTesting
    protected void setDefaultRemoteS3Facade(RemoteS3Facade remoteS3Facade)
    {
        checkState(defaultConfiguredRemoteS3Facade.compareAndSet(null, remoteS3Facade), "Default RemoteS3Facade already set");
    }

    @Override
    public URI buildEndpoint(UriBuilder uriBuilder, String path, String bucket, String region)
    {
        return defaultConfiguredRemoteS3Facade.get().buildEndpoint(uriBuilder, path, bucket, region);
    }

    @Override
    public URI remoteUri(String region)
    {
        return defaultConfiguredRemoteS3Facade.get().remoteUri(region);
    }
}
