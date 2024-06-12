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

import com.google.inject.Inject;
import io.trino.s3.proxy.server.security.SecurityFacadeProvider;
import io.trino.s3.proxy.server.security.SecurityResponse;
import io.trino.s3.proxy.server.testing.TestingSecurityFacade;
import io.trino.s3.proxy.server.testing.harness.TrinoS3ProxyTest;
import io.trino.s3.proxy.server.testing.harness.TrinoS3ProxyTestCommonModules.WithConfiguredBuckets;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TrinoS3ProxyTest(filters = WithConfiguredBuckets.class)
public class TestSecurityFacade
{
    private final S3Client client;
    private final TestingSecurityFacade securityFacade;

    private SecurityFacadeProvider savedDelegate;

    @Inject
    public TestSecurityFacade(S3Client client, TestingSecurityFacade securityFacade)
    {
        this.client = requireNonNull(client, "internalClient is null");
        this.securityFacade = securityFacade;
    }

    @BeforeEach
    public void setup()
    {
        savedDelegate = securityFacade.delegate();
    }

    @AfterEach
    public void reset()
    {
        securityFacade.setDelegate(savedDelegate);
    }

    @Test
    public void testDisableListObjects()
    {
        // list buckets is allowed
        ListBucketsResponse listBucketsResponse = client.listBuckets();
        assertThat(listBucketsResponse.buckets()).extracting(Bucket::name).containsExactlyInAnyOrder("one", "two", "three");

        // list objects is currently allowed
        ListObjectsResponse listObjectsResponse = client.listObjects(r -> r.bucket("one"));
        assertThat(listObjectsResponse.contents()).isEmpty();

        // set facade that disallows list objects on bucket "one"
        securityFacade.setDelegate((request, _, _) -> (_, _) -> {
            if ("one".equals(request.bucketName()) && request.httpVerb().equalsIgnoreCase("get")) {
                return new SecurityResponse(false, Optional.empty());
            }
            return SecurityResponse.DEFAULT;
        });

        // list objects on "one" now disallowed
        assertThatThrownBy(() -> client.listObjects(r -> r.bucket("one")))
                .asInstanceOf(InstanceOfAssertFactories.type(S3Exception.class))
                .extracting(S3Exception::statusCode)
                .isEqualTo(401);

        // list buckets still allowed
        listBucketsResponse = client.listBuckets();
        assertThat(listBucketsResponse.buckets()).extracting(Bucket::name).containsExactlyInAnyOrder("one", "two", "three");
    }
}
