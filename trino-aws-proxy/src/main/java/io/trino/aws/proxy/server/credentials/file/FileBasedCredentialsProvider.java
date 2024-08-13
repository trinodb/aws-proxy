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
package io.trino.aws.proxy.server.credentials.file;

import com.google.common.io.Files;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.aws.proxy.spi.credentials.Credentials;
import io.trino.aws.proxy.spi.credentials.CredentialsProvider;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class FileBasedCredentialsProvider
        implements CredentialsProvider
{
    private final Map<String, Credentials> credentialsStore;

    @Inject
    public FileBasedCredentialsProvider(FileBasedCredentialsProviderConfig config, JsonCodec<List<Credentials>> jsonCodec)
    {
        requireNonNull(config, "Config is null");
        requireNonNull(jsonCodec, "jsonCodec is null");
        this.credentialsStore = buildCredentialsMap(config.getCredentialsFile(), jsonCodec);
    }

    private Map<String, Credentials> buildCredentialsMap(File credentialsFile, JsonCodec<List<Credentials>> jsonCodec)
    {
        List<Credentials> credentialsList;
        try {
            credentialsList = jsonCodec.fromJson(Files.toByteArray(credentialsFile));
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read credentials file", e);
        }
        return credentialsList.stream()
                .collect(toImmutableMap(credentials -> credentials.emulated().accessKey(), Function.identity()));
    }

    @Override
    public Optional<Credentials> credentials(String emulatedAccessKey, Optional<String> session)
    {
        return Optional.ofNullable(credentialsStore.get(emulatedAccessKey));
    }
}
