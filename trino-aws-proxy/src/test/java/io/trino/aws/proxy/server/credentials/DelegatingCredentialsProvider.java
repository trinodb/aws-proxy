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

import io.trino.aws.proxy.spi.credentials.Credentials;
import io.trino.aws.proxy.spi.credentials.CredentialsProvider;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

public class DelegatingCredentialsProvider
        implements CredentialsProvider
{
    private final AtomicReference<CredentialsProvider> delegate = new AtomicReference<>();

    public void setDelegate(CredentialsProvider delegate)
    {
        this.delegate.set(requireNonNull(delegate, "delegate is null"));
    }

    @Override
    public Optional<Credentials> credentials(String emulatedAccessKey, Optional<String> session)
    {
        return delegate.get().credentials(emulatedAccessKey, session);
    }
}
