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
package io.trino.aws.proxy.spark4;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.DelegatingS3Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Request;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PresignAwareS3Client
        extends DelegatingS3Client
{
    private static final Log log = LogFactory.getLog(PresignAwareS3Client.class);

    public PresignAwareS3Client(S3Client delegate)
    {
        super(delegate);
    }

    private Optional<URI> getPresignedUrl(String method, String bucket, String key, Optional<UploadPartRequest> uploadPartRequest)
    {
        Optional<String> uploadId = uploadPartRequest.map(UploadPartRequest::uploadId);
        Optional<Integer> partNumber = uploadPartRequest.map(UploadPartRequest::partNumber);

        log.debug("request.get-presigned-url method=%s bucket=%s key=%s uploadId=%s partNumber=%s".formatted(method, bucket, key, uploadId, partNumber));

        Map<String, List<String>> headers;
        try {
            HeadObjectResponse response = headObject(builder -> {
                builder.bucket(bucket).key(key);
                partNumber.ifPresent(builder::partNumber);
                uploadId.ifPresent(id -> builder.overrideConfiguration(c -> c.putRawQueryParameter("uploadId", id)));
            });
            headers = response.sdkHttpResponse().headers();
        }
        catch (AwsServiceException e) {
            boolean is404NotFound = (e.statusCode() == 404);
            boolean isGetOrDelete = method.equalsIgnoreCase("get") || method.equalsIgnoreCase("delete");

            if (is404NotFound && isGetOrDelete) {
                throw e;
            }

            // when the bucket isn't found an exception is thrown - but response headers still have pre-signed URLs
            // given that this not GET/DELETE, we can return the pre-signed URL
            headers = e.awsErrorDetails().sdkHttpResponse().headers();
        }
        catch (RuntimeException e) {
            log.error("request.get-presigned-url.exception method=%s bucket=%s key=%s uploadId=%s partNumber=%s".formatted(method, bucket, key, uploadId, partNumber), e);
            throw e;
        }

        List<String> headerValue = headers.get("X-Trino-Pre-Signed-Url-" + method);
        Optional<URI> presignedUri = Optional.ofNullable(headerValue).filter(list -> !list.isEmpty()).map(list -> URI.create(String.valueOf(list.get(0))));
        log.debug("response.get-presigned-url method=%s bucket=%s key=%s uploadId=%s partNumber=%s success=%s".formatted(method, bucket, key, uploadId, partNumber, presignedUri.isPresent()));
        return presignedUri;
    }

    private <T extends S3Request.Builder> T registerPresignedUrl(T builder, URI presignedUrl)
    {
        builder.overrideConfiguration(c -> c.signer(new PresignAwareSigner(presignedUrl)));
        return builder;
    }

    @Override
    public <ReturnT> ReturnT getObject(GetObjectRequest getObjectRequest, ResponseTransformer<GetObjectResponse, ReturnT> responseTransformer)
            throws AwsServiceException, SdkClientException
    {
        return getPresignedUrl("GET", getObjectRequest.bucket(), getObjectRequest.key(), Optional.empty())
                .map(presignedUrl -> super.getObject(registerPresignedUrl(getObjectRequest.toBuilder(), presignedUrl).build(), responseTransformer))
                .orElseGet(() -> super.getObject(getObjectRequest, responseTransformer));
    }

    @Override
    public PutObjectResponse putObject(PutObjectRequest putObjectRequest, RequestBody requestBody)
            throws AwsServiceException, SdkClientException
    {
        return getPresignedUrl("PUT", putObjectRequest.bucket(), putObjectRequest.key(), Optional.empty())
                .map(presignedUrl -> super.putObject(registerPresignedUrl(putObjectRequest.toBuilder(), presignedUrl).build(), requestBody))
                .orElseGet(() -> super.putObject(putObjectRequest, requestBody));
    }

    @Override
    public UploadPartResponse uploadPart(UploadPartRequest uploadPartRequest, RequestBody requestBody)
            throws AwsServiceException, SdkClientException
    {
        return getPresignedUrl("PUT", uploadPartRequest.bucket(), uploadPartRequest.key(), Optional.of(uploadPartRequest))
                .map(presignedUrl -> super.uploadPart(registerPresignedUrl(uploadPartRequest.toBuilder(), presignedUrl).build(), requestBody))
                .orElseGet(() -> super.uploadPart(uploadPartRequest, requestBody));
    }
}
