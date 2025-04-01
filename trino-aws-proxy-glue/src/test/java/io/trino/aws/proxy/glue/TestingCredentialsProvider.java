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
package io.trino.aws.proxy.glue;

import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.credentials.CredentialsProvider;
import io.trino.aws.proxy.spi.credentials.IdentityCredential;

import java.util.Optional;
import java.util.UUID;

public class TestingCredentialsProvider
        implements CredentialsProvider
{
    public static final IdentityCredential CREDENTIALS = new IdentityCredential(new Credential(UUID.randomUUID().toString(), UUID.randomUUID().toString()));

    @Override
    public Optional<IdentityCredential> credentials(String emulatedAccessKey, Optional<String> session)
    {
        return Optional.of(CREDENTIALS);
    }
}
