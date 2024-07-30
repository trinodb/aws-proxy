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

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.aws.proxy.server.testing.containers.S3Container;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.credentials.Credentials;
import io.trino.aws.proxy.spi.remote.RemoteSessionRole;

import java.util.Optional;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

public class TestingRemoteCredentialsProvider
        implements Provider<Credentials>
{
    private final S3Container s3MockContainer;
    private final TestingCredentialsRolesProvider credentialsController;

    @Inject
    public TestingRemoteCredentialsProvider(S3Container s3MockContainer, TestingCredentialsRolesProvider credentialsController)
    {
        this.s3MockContainer = requireNonNull(s3MockContainer, "s3MockContainer is null");
        this.credentialsController = requireNonNull(credentialsController, "credentialsController is null");
    }

    @Override
    public Credentials get()
    {
        Credential policyUserCredential = s3MockContainer.policyUserCredential();

        RemoteSessionRole remoteSessionRole = new RemoteSessionRole("us-east-1", "minio-doesnt-care", Optional.empty());
        Credentials remoteCredentials = Credentials.build(new Credential(UUID.randomUUID().toString(), UUID.randomUUID().toString()), policyUserCredential, remoteSessionRole);
        credentialsController.addCredentials(remoteCredentials);

        return remoteCredentials;
    }
}
