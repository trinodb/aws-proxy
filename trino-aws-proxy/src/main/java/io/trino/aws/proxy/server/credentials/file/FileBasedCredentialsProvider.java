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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.trino.aws.proxy.spi.credentials.Credentials;
import io.trino.aws.proxy.spi.credentials.CredentialsProvider;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
    public FileBasedCredentialsProvider(FileBasedCredentialsProviderConfig config, ObjectMapper objectMapper)
    {
        requireNonNull(config, "Config is null");
        this.credentialsStore = buildCredentialsMap(config.getCredentialsFile(), objectMapper);
    }

    private Map<String, Credentials> buildCredentialsMap(File credentialsFile, ObjectMapper objectMapper)
    {
        List<Credentials> credentialsList;
        try (InputStream inputStream = new FileInputStream(credentialsFile)) {
            credentialsList = objectMapper.readValue(inputStream, new TypeReference<>() {});
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
