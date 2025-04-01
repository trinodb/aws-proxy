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
package io.trino.aws.proxy.server.testing;

import io.trino.aws.proxy.spi.credentials.AssumedRoleProvider;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.credentials.CredentialsProvider;
import io.trino.aws.proxy.spi.credentials.EmulatedAssumedRole;
import io.trino.aws.proxy.spi.credentials.Identity;
import io.trino.aws.proxy.spi.credentials.IdentityCredential;
import io.trino.aws.proxy.spi.remote.RemoteS3Connection;
import io.trino.aws.proxy.spi.remote.RemoteS3ConnectionProvider;
import io.trino.aws.proxy.spi.rest.ParsedS3Request;
import io.trino.aws.proxy.spi.signing.SigningMetadata;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Note: the implementation here is for testing purposes only and not
 * meant for Production. A real, production ready implementation would
 * store credentials and assumed roles in a database, etc.
 */
public class TestingCredentialsRolesProvider
        implements CredentialsProvider, AssumedRoleProvider, RemoteS3ConnectionProvider
{
    private final Map<String, IdentityCredential> credentials = new ConcurrentHashMap<>();
    private final Map<String, Session> assumedRoleSessions = new ConcurrentHashMap<>();
    private final Map<String, RemoteS3Connection> remoteConnections = new ConcurrentHashMap<>();
    private RemoteS3Connection defaultRemoteS3Connection;

    private final AtomicInteger assumedRoleCount = new AtomicInteger();

    private record Session(Credential sessionCredential, String originalEmulatedAccessKey, Instant expiration)
    {
        private Session
        {
            requireNonNull(sessionCredential, "sessionCredential is null");
            requireNonNull(originalEmulatedAccessKey, "originalEmulatedAccessKey is null");
            requireNonNull(expiration, "expiration is null");
        }
    }

    @Override
    public Optional<RemoteS3Connection> remoteConnection(SigningMetadata signingMetadata, Optional<Identity> identity, ParsedS3Request request)
    {
        return signingMetadata.credential().session().flatMap(sessionToken -> {
            Session session = assumedRoleSessions.get(sessionToken);

            boolean isValid = (session != null) && session.expiration.isAfter(Instant.now());
            if (!isValid) {
                assumedRoleSessions.remove(sessionToken);
                return Optional.empty();
            }

            assumedRoleCount.incrementAndGet();

            String emulatedAccessKey = signingMetadata.credential().accessKey();
            checkState(emulatedAccessKey.equals(session.sessionCredential().accessKey()), "emulatedAccessKey and session accessKey mismatch");

            return Optional.ofNullable(remoteConnections.get(session.originalEmulatedAccessKey()));
        }).or(() -> Optional.ofNullable(remoteConnections.get(signingMetadata.credential().accessKey()))).or(() -> Optional.ofNullable(defaultRemoteS3Connection));
    }

    @Override
    public Optional<IdentityCredential> credentials(String emulatedAccessKey, Optional<String> maybeSessionToken)
    {
        return maybeSessionToken.flatMap(sessionToken -> {
            Session session = assumedRoleSessions.get(sessionToken);
            boolean isValid = (session != null) && session.expiration.isAfter(Instant.now());
            if (!isValid) {
                assumedRoleSessions.remove(sessionToken);
                return Optional.empty();
            }

            assumedRoleCount.incrementAndGet();

            checkState(emulatedAccessKey.equals(session.sessionCredential.accessKey()), "emulatedAccessKey and session accessKey mismatch");

            return Optional.of(new IdentityCredential(session.sessionCredential(), credentials.get(session.originalEmulatedAccessKey()).identity()));
        }).or(() -> Optional.ofNullable(credentials.get(emulatedAccessKey)));
    }

    @Override
    public Optional<EmulatedAssumedRole> assumeEmulatedRole(
            Credential emulatedCredential,
            String region,
            String requestArn,
            Optional<String> requestExternalId,
            Optional<String> requestRoleSessionName,
            Optional<Integer> requestDurationSeconds)
    {
        String originalEmulatedAccessKey = emulatedCredential.accessKey();
        return Optional.ofNullable(credentials.get(originalEmulatedAccessKey))
                .map(_ -> {
                    String sessionToken = UUID.randomUUID().toString();
                    Session session = new Session(new Credential(UUID.randomUUID().toString(), UUID.randomUUID().toString(), Optional.of(sessionToken)), originalEmulatedAccessKey, Instant.now().plusSeconds(TimeUnit.HOURS.toSeconds(1)));

                    assumedRoleSessions.put(sessionToken, session);

                    return new EmulatedAssumedRole(session.sessionCredential, requestArn, UUID.randomUUID().toString(), session.expiration);
                });
    }

    public int assumedRoleCount()
    {
        return assumedRoleCount.get();
    }

    public void addCredentials(IdentityCredential credential)
    {
        this.credentials.put(credential.emulated().accessKey(), credential);
    }

    public void addCredentials(IdentityCredential credential, RemoteS3Connection remoteS3Connection)
    {
        addCredentials(credential);
        this.remoteConnections.put(credential.emulated().accessKey(), remoteS3Connection);
    }

    public void resetAssumedRoles()
    {
        assumedRoleCount.set(0);
        assumedRoleSessions.clear();
    }

    public void setDefaultRemoteConnection(RemoteS3Connection remoteS3Connection)
    {
        this.defaultRemoteS3Connection = remoteS3Connection;
    }
}
