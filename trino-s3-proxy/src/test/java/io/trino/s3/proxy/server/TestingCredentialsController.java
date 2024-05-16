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
package io.trino.s3.proxy.server;

import io.trino.s3.proxy.server.credentials.Credentials;
import io.trino.s3.proxy.server.credentials.CredentialsController;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class TestingCredentialsController
        implements CredentialsController
{
    private final Map<String, Credentials> credentials = new ConcurrentHashMap<>();

    @Override
    public Optional<Credentials> credentials(String emulatedAccessKey)
    {
        return Optional.ofNullable(credentials.get(emulatedAccessKey));
    }

    @Override
    public void upsertCredentials(Credentials credentials)
    {
        this.credentials.put(credentials.emulated().accessKey(), credentials);
    }
}
