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
import io.trino.aws.proxy.server.testing.TestingUtil.ForTesting;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.rest.S3RequestRewriter.S3RewriteResult;

import static java.util.Objects.requireNonNull;

public class TestingS3RequestRewriteController
{
    private final TestingS3RequestRewriter s3RequestRewriter;
    private final Credential defaultCredential;

    @Inject
    public TestingS3RequestRewriteController(TestingS3RequestRewriter rewriter, @ForTesting Credential defaultCredential)
    {
        this.s3RequestRewriter = requireNonNull(rewriter, "rewriter is null");
        this.defaultCredential = requireNonNull(defaultCredential, "defaultCredentials is null");
    }

    private S3RewriteResult rewriteOrNoop(String accessKey, String bucket, String key)
    {
        return s3RequestRewriter.testRewrite(accessKey, bucket, key).orElseGet(() -> new S3RewriteResult(bucket, key));
    }

    public String getTargetBucket(String accessKey, String bucket, String key)
    {
        return rewriteOrNoop(accessKey, bucket, key).finalRequestBucket();
    }

    public String getTargetBucket(String bucket, String key)
    {
        return getTargetBucket(defaultCredential.accessKey(), bucket, key);
    }

    public String getTargetKey(String accessKey, String bucket, String key)
    {
        return rewriteOrNoop(accessKey, bucket, key).finalRequestKey();
    }

    public String getTargetKey(String bucket, String key)
    {
        return getTargetKey(defaultCredential.accessKey(), bucket, key);
    }
}
