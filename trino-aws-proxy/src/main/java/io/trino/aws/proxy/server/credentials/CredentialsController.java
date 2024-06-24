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
package io.trino.aws.proxy.server.credentials;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.aws.proxy.server.remote.RemoteS3Facade;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.credentials.Credentials;
import io.trino.aws.proxy.spi.credentials.CredentialsProvider;
import io.trino.aws.proxy.spi.remote.RemoteSessionRole;
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
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class CredentialsController
{
    private static final Logger log = Logger.get(CredentialsController.class);

    private final RemoteS3Facade remoteS3Facade;
    private final CredentialsProvider credentialsProvider;
    private final Map<String, Session> remoteSessions = new ConcurrentHashMap<>();

    private final class Session
            implements Closeable
    {
        private final String sessionName;
        private final StsClient stsClient;
        private final StsAssumeRoleCredentialsProvider credentialsProvider;

        // TODO: use useCount and lastUsage to clean old sessions that haven't been used in a while
        private final AtomicLong useCount = new AtomicLong();
        private volatile Instant lastUsage = Instant.now();

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

        private <T> Optional<T> withUsage(Credentials credentials, Function<Credentials, Optional<T>> credentialsConsumer)
        {
            incrementUsage();
            try {
                Credentials remoteSessionCredentials = Credentials.build(credentials.emulated(), currentCredential());
                return credentialsConsumer.apply(remoteSessionCredentials);
            }
            finally {
                decrementUsage();
            }
        }

        private void incrementUsage()
        {
            lastUsage = Instant.now();
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
    public CredentialsController(RemoteS3Facade remoteS3Facade, CredentialsProvider credentialsProvider)
    {
        this.remoteS3Facade = requireNonNull(remoteS3Facade, "remoteS3Facade is null");
        this.credentialsProvider = requireNonNull(credentialsProvider, "credentialsProvider is null");
    }

    @PreDestroy
    public void shutdown()
    {
        remoteSessions.values().forEach(Session::close);
    }

    @SuppressWarnings("resource")
    public <T> Optional<T> withCredentials(String emulatedAccessKey, Optional<String> emulatedSessionToken, Function<Credentials, Optional<T>> credentialsConsumer)
    {
        Optional<T> result = credentialsProvider.credentials(emulatedAccessKey, emulatedSessionToken)
                .flatMap(credentials -> credentials.remoteSessionRole()
                        .flatMap(remoteSessionRole -> internalRemoteSession(remoteSessionRole, credentials).withUsage(credentials, credentialsConsumer))
                        .or(() -> credentialsConsumer.apply(credentials)));

        result.ifPresentOrElse(_ -> log.debug("Credentials found. EmulatedAccessKey: %s", emulatedAccessKey),
                () -> log.debug("Credentials not found. EmulatedAccessKey: %s", emulatedAccessKey));

        return result;
    }

    private Session internalRemoteSession(RemoteSessionRole remoteSessionRole, Credentials credentials)
    {
        String emulatedAccessKey = credentials.emulated().accessKey();
        return remoteSessions.computeIfAbsent(emulatedAccessKey, _ -> internalStartRemoteSession(remoteSessionRole, credentials.requiredRemoteCredential(), emulatedAccessKey));
    }

    private Session internalStartRemoteSession(RemoteSessionRole remoteSessionRole, Credential remoteCredential, String sessionName)
    {
        AwsCredentials awsCredentials = remoteCredential.session()
                .map(session -> (AwsCredentials) AwsSessionCredentials.create(remoteCredential.accessKey(), remoteCredential.secretKey(), session))
                .orElseGet(() -> AwsBasicCredentials.create(remoteCredential.accessKey(), remoteCredential.secretKey()));

        StsClient stsClient = StsClient.builder()
                .region(Region.of(remoteSessionRole.region()))
                .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
                .endpointProvider(_ -> completedFuture(Endpoint.builder().url(remoteS3Facade.remoteUri(remoteSessionRole.region())).build()))
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
