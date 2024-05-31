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
package io.trino.s3.proxy.server.testing;

import io.trino.s3.proxy.server.credentials.Credential;
import io.trino.s3.proxy.server.credentials.Credentials;
import io.trino.s3.proxy.server.credentials.CredentialsProvider;
import io.trino.s3.proxy.server.credentials.EmulatedAssumedRole;
import io.trino.s3.proxy.server.credentials.SigningMetadata;

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
public class TestingCredentialsProvider
        implements CredentialsProvider
{
    private final Map<String, Credentials> credentials = new ConcurrentHashMap<>();
    private final Map<String, Session> assumedRoleSessions = new ConcurrentHashMap<>();
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
    public Optional<Credentials> credentials(String emulatedAccessKey, Optional<String> maybeSessionToken)
    {
        if (maybeSessionToken.isPresent()) {
            return maybeSessionToken.flatMap(sessionToken -> {
                Session session = assumedRoleSessions.get(sessionToken);
                boolean isValid = (session != null) && session.expiration.isAfter(Instant.now());
                if (!isValid) {
                    assumedRoleSessions.remove(sessionToken);
                    return Optional.empty();
                }

                assumedRoleCount.incrementAndGet();

                checkState(emulatedAccessKey.equals(session.sessionCredential.accessKey()), "emulatedAccessKey and session accessKey mismatch");

                Credentials originalCredentials = requireNonNull(credentials.get(session.originalEmulatedAccessKey), "original credentials missing for: " + session.originalEmulatedAccessKey);
                return Optional.of(new Credentials(session.sessionCredential, originalCredentials.real()));
            });
        }

        return Optional.ofNullable(credentials.get(emulatedAccessKey));
    }

    @Override
    public Optional<EmulatedAssumedRole> assumeRole(
            SigningMetadata signingMetadata,
            String requestArn,
            Optional<String> requestExternalId,
            Optional<String> requestRoleSessionName,
            Optional<Integer> requestDurationSeconds)
    {
        String originalEmulatedAccessKey = signingMetadata.credentials().emulated().accessKey();
        return Optional.ofNullable(credentials.get(originalEmulatedAccessKey))
                .map(internal -> {
                    String sessionToken = UUID.randomUUID().toString();
                    Session session = new Session(new Credential(UUID.randomUUID().toString(), UUID.randomUUID().toString()), originalEmulatedAccessKey, Instant.now().plusSeconds(TimeUnit.HOURS.toSeconds(1)));

                    assumedRoleSessions.put(sessionToken, session);

                    return new EmulatedAssumedRole(session.sessionCredential, sessionToken, requestArn, UUID.randomUUID().toString(), session.expiration);
                });
    }

    public int assumedRoleCount()
    {
        return assumedRoleCount.get();
    }

    public void addCredentials(Credentials credentials)
    {
        this.credentials.put(credentials.emulated().accessKey(), credentials);
    }

    public void resetAssumedRoles()
    {
        assumedRoleCount.set(0);
        assumedRoleSessions.clear();
    }
}
