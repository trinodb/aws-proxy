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
import io.trino.s3.proxy.server.testing.harness.TrinoS3ProxyTest;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@TrinoS3ProxyTest(initialBuckets = "one,two,three")
public class TestProxiedRequests
{
    private final S3Client s3Client;

    @Inject
    public TestProxiedRequests(S3Client s3Client)
    {
        this.s3Client = requireNonNull(s3Client, "s3Client is null");
    }

    @Test
    public void testListBuckets()
    {
        ListBucketsResponse listBucketsResponse = s3Client.listBuckets();
        assertThat(listBucketsResponse.buckets())
                .extracting(Bucket::name)
                .containsExactlyInAnyOrder("one", "two", "three");

        ListObjectsResponse listObjectsResponse = s3Client.listObjects(request -> request.bucket("one"));
        assertThat(listObjectsResponse.contents()).isEmpty();
    }
}
