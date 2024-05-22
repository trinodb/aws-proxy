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

import io.trino.s3.proxy.server.credentials.Credentials;
import io.trino.s3.proxy.server.credentials.StsAssumedRole;
import io.trino.s3.proxy.server.credentials.StsController;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

public class TestingStsController
        implements StsController
{
    private final Map<Key, StsAssumedRole> assumedRoles = new ConcurrentHashMap<>();
    private final Map<String, Credentials> credentials = new ConcurrentHashMap<>();

    private record Key(String emulatedAccessKey, String requestRoleArn)
    {
        private Key
        {
            requireNonNull(emulatedAccessKey, "emulatedAccessKey is null");
            requireNonNull(requestRoleArn, "requestRoleArn is null");
        }
    }

    @Override
    public Optional<StsAssumedRole> assumeRole(
            String emulatedAccessKey,
            Optional<String> session,
            String requestRoleArn,
            Optional<String> requestExternalId,
            Optional<String> requestRoleSessionName,
            Optional<Integer> requestDurationSeconds)
    {
        return Optional.ofNullable(assumedRoles.get(new Key(emulatedAccessKey, requestRoleArn)));
    }

    @Override
    public Optional<Credentials> checkAssumedRole(String emulatedAccessKey, Optional<String> session)
    {
        return session.flatMap(value -> Optional.ofNullable(credentials.get(value)));
    }

    public void addAssumedRole(String emulatedAccessKey, String arn, Credentials credentials, Instant expiration)
    {
        String session = UUID.randomUUID().toString();
        StsAssumedRole stsAssumedRole = new StsAssumedRole(credentials.emulated(), session, arn, "role-id", expiration);
        assumedRoles.put(new Key(emulatedAccessKey, arn), stsAssumedRole);
        this.credentials.put(session, credentials);
    }
}
