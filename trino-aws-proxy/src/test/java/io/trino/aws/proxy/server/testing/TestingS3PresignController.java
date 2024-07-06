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
import io.trino.aws.proxy.server.TrinoAwsProxyConfig;
import io.trino.aws.proxy.server.rest.S3PresignController;
import io.trino.aws.proxy.server.security.S3SecurityController;
import io.trino.aws.proxy.server.testing.containers.TestContainerUtil;
import io.trino.aws.proxy.spi.rest.ParsedS3Request;
import io.trino.aws.proxy.spi.signing.SigningController;
import io.trino.aws.proxy.spi.signing.SigningMetadata;

import java.net.URI;
import java.time.Instant;
import java.util.Map;

public class TestingS3PresignController
        extends S3PresignController
{
    private volatile boolean rewriteUrisForContainers = true;

    @Inject
    public TestingS3PresignController(SigningController signingController, TrinoAwsProxyConfig trinoAwsProxyConfig, S3SecurityController s3SecurityController)
    {
        super(signingController, trinoAwsProxyConfig, s3SecurityController);
    }

    @Override
    public Map<String, URI> buildPresignedRemoteUrls(SigningMetadata signingMetadata, ParsedS3Request request, Instant targetRequestTimestamp, URI remoteUri)
    {
        if (rewriteUrisForContainers) {
            remoteUri = URI.create(TestContainerUtil.asHostUrl(remoteUri.toString()));
        }
        return super.buildPresignedRemoteUrls(signingMetadata, request, targetRequestTimestamp, remoteUri);
    }

    public void setRewriteUrisForContainers(boolean doRewrites)
    {
        rewriteUrisForContainers = doRewrites;
    }
}
