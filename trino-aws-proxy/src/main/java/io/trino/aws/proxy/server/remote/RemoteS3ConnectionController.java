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

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.credentials.Identity;
import io.trino.aws.proxy.spi.remote.RemoteS3ConnectionProvider;
import io.trino.aws.proxy.spi.remote.RemoteS3Facade;
import io.trino.aws.proxy.spi.remote.RemoteSessionRole;
import io.trino.aws.proxy.spi.rest.ParsedS3Request;
import io.trino.aws.proxy.spi.signing.SigningMetadata;
import jakarta.annotation.PreDestroy;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.endpoints.Endpoint;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.io.Closeable;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class RemoteS3ConnectionController
{
    private static final Logger log = Logger.get(RemoteS3ConnectionController.class);

    private final RemoteS3Facade defaultS3Facade;
    private final RemoteS3ConnectionProvider remoteS3ConnectionProvider;
    private final Map<String, Session> remoteSessions = new ConcurrentHashMap<>();

    private final class Session
            implements Closeable
    {
        private final String sessionName;
        private final StsClient stsClient;
        private final StsAssumeRoleCredentialsProvider credentialsProvider;

        // TODO: use useCount and lastUsage to clean old sessions that haven't been used in a while
        private final AtomicLong useCount = new AtomicLong();

        private Session(String sessionName, StsClient stsClient, StsAssumeRoleCredentialsProvider credentialsProvider)
        {
            this.sessionName = requireNonNull(sessionName, "sessionName is null");
            this.stsClient = requireNonNull(stsClient, "stsClient is null");
            this.credentialsProvider = requireNonNull(credentialsProvider, "credentialsProvider is null");
        }

        @Override
        public void close()
        {
            checkState(useCount.get() == 0, "Session is currently being used");

            Session session = remoteSessions.remove(sessionName);
            requireNonNull(session, "Session was already closed");

            stsClient.close();
        }

        private <T> T withUsage(Function<Credential, T> credentialsConsumer)
        {
            incrementUsage();
            try {
                return credentialsConsumer.apply(currentCredential());
            }
            finally {
                decrementUsage();
            }
        }

        private void incrementUsage()
        {
            useCount.incrementAndGet();
        }

        private void decrementUsage()
        {
            if (useCount.decrementAndGet() < 0) {
                throw new IllegalStateException("Session useCount has gone negative");
            }
        }

        @SuppressWarnings("SwitchStatementWithTooFewBranches")
        private Credential currentCredential()
        {
            checkState(remoteSessions.containsKey(sessionName), "Session is closed");

            AwsCredentials awsCredentials = credentialsProvider.resolveCredentials();

            return switch (awsCredentials) {
                case AwsSessionCredentials sessionCredentials ->
                        new Credential(sessionCredentials.accessKeyId(), sessionCredentials.secretAccessKey(), Optional.of(sessionCredentials.sessionToken()));
                default -> new Credential(awsCredentials.accessKeyId(), awsCredentials.secretAccessKey());
            };
        }
    }

    @Inject
    public RemoteS3ConnectionController(RemoteS3Facade defaultS3Facade, RemoteS3ConnectionProvider remoteS3ConnectionProvider)
    {
        this.defaultS3Facade = requireNonNull(defaultS3Facade, "defaultS3Facade is null");
        this.remoteS3ConnectionProvider = requireNonNull(remoteS3ConnectionProvider, "remoteS3ConnectionProvider is null");
    }

    @PreDestroy
    public void shutdown()
    {
        remoteSessions.values().forEach(Session::close);
    }

    @SuppressWarnings("resource")
    public <T> Optional<T> withRemoteConnection(SigningMetadata signingMetadata, Optional<Identity> identity, ParsedS3Request request,
            BiFunction<Credential, RemoteS3Facade, T> credentialsConsumer)
    {
        return remoteS3ConnectionProvider.remoteConnection(signingMetadata, identity, request)
                .flatMap(remoteConnection -> {
                    RemoteS3Facade contextRemoteS3Facade = remoteConnection.appliedRemoteS3Facade(defaultS3Facade);
                    return remoteConnection.remoteSessionRole()
                            .map(remoteSessionRole ->
                                    internalRemoteSession(remoteSessionRole, remoteConnection.remoteCredential())
                                            .withUsage(credentials -> credentialsConsumer.apply(credentials, contextRemoteS3Facade)))
                            .or(() -> Optional.of(credentialsConsumer.apply(remoteConnection.remoteCredential(), contextRemoteS3Facade)));
                });
    }

    private Session internalRemoteSession(RemoteSessionRole remoteSessionRole, Credential remoteCredential)
    {
        String remoteAccessKey = remoteCredential.accessKey();
        return remoteSessions.computeIfAbsent(remoteAccessKey, _ -> internalStartRemoteSession(remoteSessionRole, remoteCredential, remoteAccessKey));
    }

    private Session internalStartRemoteSession(RemoteSessionRole remoteSessionRole, Credential remoteCredential, String sessionName)
    {
        AwsCredentials awsCredentials = remoteCredential.session()
                .map(session -> (AwsCredentials) AwsSessionCredentials.create(remoteCredential.accessKey(), remoteCredential.secretKey(), session))
                .orElseGet(() -> AwsBasicCredentials.create(remoteCredential.accessKey(), remoteCredential.secretKey()));

        URI stsEndpoint = remoteSessionRole.stsEndpoint().orElseGet(() -> defaultS3Facade.remoteUri(remoteSessionRole.region()));

        StsClient stsClient = StsClient.builder()
                .region(Region.of(remoteSessionRole.region()))
                .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
                .endpointProvider(_ -> completedFuture(Endpoint.builder().url(stsEndpoint).build()))
                .build();

        StsAssumeRoleCredentialsProvider credentialsProvider = StsAssumeRoleCredentialsProvider.builder()
                .refreshRequest(request -> {
                    request.roleArn(remoteSessionRole.roleArn()).roleSessionName(sessionName);
                    remoteSessionRole.externalId().ifPresent(request::externalId);
                })
                .stsClient(stsClient)
                .asyncCredentialUpdateEnabled(true)
                .build();

        return new Session(sessionName, stsClient, credentialsProvider);
    }
}
