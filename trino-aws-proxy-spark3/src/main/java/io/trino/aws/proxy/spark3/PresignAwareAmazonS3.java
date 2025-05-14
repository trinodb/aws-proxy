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
package io.trino.aws.proxy.spark3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.HttpMethod;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.S3ResponseMetadata;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.BucketAccelerateConfiguration;
import com.amazonaws.services.s3.model.BucketCrossOriginConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketLoggingConfiguration;
import com.amazonaws.services.s3.model.BucketNotificationConfiguration;
import com.amazonaws.services.s3.model.BucketPolicy;
import com.amazonaws.services.s3.model.BucketReplicationConfiguration;
import com.amazonaws.services.s3.model.BucketTaggingConfiguration;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.BucketWebsiteConfiguration;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketAnalyticsConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketAnalyticsConfigurationResult;
import com.amazonaws.services.s3.model.DeleteBucketCrossOriginConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketEncryptionRequest;
import com.amazonaws.services.s3.model.DeleteBucketEncryptionResult;
import com.amazonaws.services.s3.model.DeleteBucketIntelligentTieringConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketIntelligentTieringConfigurationResult;
import com.amazonaws.services.s3.model.DeleteBucketInventoryConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketInventoryConfigurationResult;
import com.amazonaws.services.s3.model.DeleteBucketLifecycleConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketMetricsConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketMetricsConfigurationResult;
import com.amazonaws.services.s3.model.DeleteBucketOwnershipControlsRequest;
import com.amazonaws.services.s3.model.DeleteBucketOwnershipControlsResult;
import com.amazonaws.services.s3.model.DeleteBucketPolicyRequest;
import com.amazonaws.services.s3.model.DeleteBucketReplicationConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketTaggingConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteBucketWebsiteConfigurationRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectTaggingRequest;
import com.amazonaws.services.s3.model.DeleteObjectTaggingResult;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.DeletePublicAccessBlockRequest;
import com.amazonaws.services.s3.model.DeletePublicAccessBlockResult;
import com.amazonaws.services.s3.model.DeleteVersionRequest;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetBucketAccelerateConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketAclRequest;
import com.amazonaws.services.s3.model.GetBucketAnalyticsConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketAnalyticsConfigurationResult;
import com.amazonaws.services.s3.model.GetBucketCrossOriginConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketEncryptionRequest;
import com.amazonaws.services.s3.model.GetBucketEncryptionResult;
import com.amazonaws.services.s3.model.GetBucketIntelligentTieringConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketIntelligentTieringConfigurationResult;
import com.amazonaws.services.s3.model.GetBucketInventoryConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketInventoryConfigurationResult;
import com.amazonaws.services.s3.model.GetBucketLifecycleConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketLocationRequest;
import com.amazonaws.services.s3.model.GetBucketLoggingConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketMetricsConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketMetricsConfigurationResult;
import com.amazonaws.services.s3.model.GetBucketNotificationConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketOwnershipControlsRequest;
import com.amazonaws.services.s3.model.GetBucketOwnershipControlsResult;
import com.amazonaws.services.s3.model.GetBucketPolicyRequest;
import com.amazonaws.services.s3.model.GetBucketPolicyStatusRequest;
import com.amazonaws.services.s3.model.GetBucketPolicyStatusResult;
import com.amazonaws.services.s3.model.GetBucketReplicationConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketTaggingConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.GetBucketWebsiteConfigurationRequest;
import com.amazonaws.services.s3.model.GetObjectAclRequest;
import com.amazonaws.services.s3.model.GetObjectLegalHoldRequest;
import com.amazonaws.services.s3.model.GetObjectLegalHoldResult;
import com.amazonaws.services.s3.model.GetObjectLockConfigurationRequest;
import com.amazonaws.services.s3.model.GetObjectLockConfigurationResult;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRetentionRequest;
import com.amazonaws.services.s3.model.GetObjectRetentionResult;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.GetPublicAccessBlockRequest;
import com.amazonaws.services.s3.model.GetPublicAccessBlockResult;
import com.amazonaws.services.s3.model.GetS3AccountOwnerRequest;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.HeadBucketResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListBucketAnalyticsConfigurationsRequest;
import com.amazonaws.services.s3.model.ListBucketAnalyticsConfigurationsResult;
import com.amazonaws.services.s3.model.ListBucketIntelligentTieringConfigurationsRequest;
import com.amazonaws.services.s3.model.ListBucketIntelligentTieringConfigurationsResult;
import com.amazonaws.services.s3.model.ListBucketInventoryConfigurationsRequest;
import com.amazonaws.services.s3.model.ListBucketInventoryConfigurationsResult;
import com.amazonaws.services.s3.model.ListBucketMetricsConfigurationsRequest;
import com.amazonaws.services.s3.model.ListBucketMetricsConfigurationsResult;
import com.amazonaws.services.s3.model.ListBucketsRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListNextBatchOfObjectsRequest;
import com.amazonaws.services.s3.model.ListNextBatchOfVersionsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PresignedUrlDownloadRequest;
import com.amazonaws.services.s3.model.PresignedUrlDownloadResult;
import com.amazonaws.services.s3.model.PresignedUrlUploadRequest;
import com.amazonaws.services.s3.model.PresignedUrlUploadResult;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.RestoreObjectRequest;
import com.amazonaws.services.s3.model.RestoreObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.amazonaws.services.s3.model.SelectObjectContentResult;
import com.amazonaws.services.s3.model.SetBucketAccelerateConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketAclRequest;
import com.amazonaws.services.s3.model.SetBucketAnalyticsConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketAnalyticsConfigurationResult;
import com.amazonaws.services.s3.model.SetBucketCrossOriginConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketEncryptionRequest;
import com.amazonaws.services.s3.model.SetBucketEncryptionResult;
import com.amazonaws.services.s3.model.SetBucketIntelligentTieringConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketIntelligentTieringConfigurationResult;
import com.amazonaws.services.s3.model.SetBucketInventoryConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketInventoryConfigurationResult;
import com.amazonaws.services.s3.model.SetBucketLifecycleConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketLoggingConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketMetricsConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketMetricsConfigurationResult;
import com.amazonaws.services.s3.model.SetBucketNotificationConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketOwnershipControlsRequest;
import com.amazonaws.services.s3.model.SetBucketOwnershipControlsResult;
import com.amazonaws.services.s3.model.SetBucketPolicyRequest;
import com.amazonaws.services.s3.model.SetBucketReplicationConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketTaggingConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketWebsiteConfigurationRequest;
import com.amazonaws.services.s3.model.SetObjectAclRequest;
import com.amazonaws.services.s3.model.SetObjectLegalHoldRequest;
import com.amazonaws.services.s3.model.SetObjectLegalHoldResult;
import com.amazonaws.services.s3.model.SetObjectLockConfigurationRequest;
import com.amazonaws.services.s3.model.SetObjectLockConfigurationResult;
import com.amazonaws.services.s3.model.SetObjectRetentionRequest;
import com.amazonaws.services.s3.model.SetObjectRetentionResult;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.SetObjectTaggingResult;
import com.amazonaws.services.s3.model.SetPublicAccessBlockRequest;
import com.amazonaws.services.s3.model.SetPublicAccessBlockResult;
import com.amazonaws.services.s3.model.SetRequestPaymentConfigurationRequest;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.services.s3.model.WriteGetObjectResponseRequest;
import com.amazonaws.services.s3.model.WriteGetObjectResponseResult;
import com.amazonaws.services.s3.model.analytics.AnalyticsConfiguration;
import com.amazonaws.services.s3.model.intelligenttiering.IntelligentTieringConfiguration;
import com.amazonaws.services.s3.model.inventory.InventoryConfiguration;
import com.amazonaws.services.s3.model.metrics.MetricsConfiguration;
import com.amazonaws.services.s3.model.ownership.OwnershipControls;
import com.amazonaws.services.s3.waiters.AmazonS3Waiters;
import com.amazonaws.util.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

class PresignAwareAmazonS3
        implements AmazonS3
{
    private static final Log log = LogFactory.getLog(PresignAwareAmazonS3.class);

    private final AmazonS3 delegate;

    PresignAwareAmazonS3(AmazonS3 delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    private record Presigned(URL url, ObjectMetadata objectMetadata)
    {
        private Presigned
        {
            requireNonNull(url, "url is null");
            requireNonNull(objectMetadata, "objectMetadata is null");
        }
    }

    private Optional<Presigned> getPresignedUrl(String method, String bucket, String key)
    {
        return getPresignedUrl(method, bucket, key, Optional.empty());
    }

    private Optional<Presigned> getPresignedUrl(String method, String bucket, String key, Optional<UploadPartRequest> uploadPartRequest)
    {
        Optional<String> uploadId = uploadPartRequest.map(UploadPartRequest::getUploadId);
        Optional<Integer> partNumber = uploadPartRequest.map(UploadPartRequest::getPartNumber);

        log.debug("request.get-presigned-url method=%s bucket=%s key=%s uploadId=%s partNumber=%s".formatted(method, bucket, key, uploadId, partNumber));

        Map<String, ?> headers;
        ObjectMetadata objectMetadata;
        try {
            GetObjectMetadataRequest getObjectMetadataRequest = new GetObjectMetadataRequest(bucket, key);
            partNumber.ifPresent(getObjectMetadataRequest::setPartNumber);
            uploadId.ifPresent(id -> getObjectMetadataRequest.putCustomQueryParameter("uploadId", id));

            objectMetadata = delegate.getObjectMetadata(getObjectMetadataRequest);
            headers = objectMetadata.getRawMetadata();
        }
        catch (AmazonServiceException e) {
            boolean is404NotFound = (e.getStatusCode() == 404);
            boolean isGetOrDelete = method.equalsIgnoreCase("get") || method.equalsIgnoreCase("delete");

            if (is404NotFound && isGetOrDelete) {
                throw e;
            }

            // when the bucket isn't found an exception is thrown - but response headers still have pre-signed URLs
            // given that this not GET/DELETE, we can return the pre-signed URL
            headers = e.getHttpHeaders();
            ObjectMetadata work = new ObjectMetadata();
            headers.forEach(work::setHeader);
            objectMetadata = work;
        }
        catch (RuntimeException e) {
            log.error("request.get-presigned-url.exception method=%s bucket=%s key=%s uploadId=%s partNumber=%s".formatted(method, bucket, key, uploadId, partNumber), e);
            throw e;
        }

        Object value = headers.get("X-Trino-Pre-Signed-Url-" + method);
        log.debug("response.get-presigned-url.exception method=%s bucket=%s key=%s uploadId=%s partNumber=%s success=%s".formatted(method, bucket, key, uploadId, partNumber, value != null));
        try {
            return (value != null) ? Optional.of(new Presigned(new URL(String.valueOf(value)), objectMetadata)) : Optional.empty();
        }
        catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    // below are delegated methods

    @Override
    public void setEndpoint(String endpoint)
    {
        delegate.setEndpoint(endpoint);
    }

    @Override
    public void setRegion(Region region)
            throws IllegalArgumentException
    {
        delegate.setRegion(region);
    }

    @Override
    public void setS3ClientOptions(S3ClientOptions clientOptions)
    {
        delegate.setS3ClientOptions(clientOptions);
    }

    @Override
    @Deprecated
    public void changeObjectStorageClass(String bucketName, String key, StorageClass newStorageClass)
            throws SdkClientException
    {
        delegate.changeObjectStorageClass(bucketName, key, newStorageClass);
    }

    @Override
    @Deprecated
    public void setObjectRedirectLocation(String bucketName, String key, String newRedirectLocation)
            throws SdkClientException
    {
        delegate.setObjectRedirectLocation(bucketName, key, newRedirectLocation);
    }

    @Override
    public ObjectListing listObjects(String bucketName)
            throws SdkClientException
    {
        return delegate.listObjects(bucketName);
    }

    @Override
    public ObjectListing listObjects(String bucketName, String prefix)
            throws SdkClientException
    {
        return delegate.listObjects(bucketName, prefix);
    }

    @Override
    public ObjectListing listObjects(ListObjectsRequest listObjectsRequest)
            throws SdkClientException
    {
        return delegate.listObjects(listObjectsRequest);
    }

    @Override
    public ListObjectsV2Result listObjectsV2(String bucketName)
            throws SdkClientException
    {
        return delegate.listObjectsV2(bucketName);
    }

    @Override
    public ListObjectsV2Result listObjectsV2(String bucketName, String prefix)
            throws SdkClientException
    {
        return delegate.listObjectsV2(bucketName, prefix);
    }

    @Override
    public ListObjectsV2Result listObjectsV2(ListObjectsV2Request listObjectsV2Request)
            throws SdkClientException
    {
        return delegate.listObjectsV2(listObjectsV2Request);
    }

    @Override
    public ObjectListing listNextBatchOfObjects(ObjectListing previousObjectListing)
            throws SdkClientException
    {
        return delegate.listNextBatchOfObjects(previousObjectListing);
    }

    @Override
    public ObjectListing listNextBatchOfObjects(ListNextBatchOfObjectsRequest listNextBatchOfObjectsRequest)
            throws SdkClientException
    {
        return delegate.listNextBatchOfObjects(listNextBatchOfObjectsRequest);
    }

    @Override
    public VersionListing listVersions(String bucketName, String prefix)
            throws SdkClientException
    {
        return delegate.listVersions(bucketName, prefix);
    }

    @Override
    public VersionListing listNextBatchOfVersions(VersionListing previousVersionListing)
            throws SdkClientException
    {
        return delegate.listNextBatchOfVersions(previousVersionListing);
    }

    @Override
    public VersionListing listNextBatchOfVersions(ListNextBatchOfVersionsRequest listNextBatchOfVersionsRequest)
            throws SdkClientException
    {
        return delegate.listNextBatchOfVersions(listNextBatchOfVersionsRequest);
    }

    @Override
    public VersionListing listVersions(String bucketName, String prefix, String keyMarker, String versionIdMarker, String delimiter, Integer maxResults)
            throws SdkClientException
    {
        return delegate.listVersions(bucketName, prefix, keyMarker, versionIdMarker, delimiter, maxResults);
    }

    @Override
    public VersionListing listVersions(ListVersionsRequest listVersionsRequest)
            throws SdkClientException
    {
        return delegate.listVersions(listVersionsRequest);
    }

    @Override
    public Owner getS3AccountOwner()
            throws SdkClientException
    {
        return delegate.getS3AccountOwner();
    }

    @Override
    public Owner getS3AccountOwner(GetS3AccountOwnerRequest getS3AccountOwnerRequest)
            throws SdkClientException
    {
        return delegate.getS3AccountOwner(getS3AccountOwnerRequest);
    }

    @Override
    @Deprecated
    public boolean doesBucketExist(String bucketName)
            throws SdkClientException
    {
        return delegate.doesBucketExist(bucketName);
    }

    @Override
    public boolean doesBucketExistV2(String bucketName)
            throws SdkClientException
    {
        return delegate.doesBucketExistV2(bucketName);
    }

    @Override
    public HeadBucketResult headBucket(HeadBucketRequest headBucketRequest)
            throws SdkClientException
    {
        return delegate.headBucket(headBucketRequest);
    }

    @Override
    public List<Bucket> listBuckets()
            throws SdkClientException
    {
        return delegate.listBuckets();
    }

    @Override
    public List<Bucket> listBuckets(ListBucketsRequest listBucketsRequest)
            throws SdkClientException
    {
        return delegate.listBuckets(listBucketsRequest);
    }

    @Override
    public String getBucketLocation(String bucketName)
            throws SdkClientException
    {
        return delegate.getBucketLocation(bucketName);
    }

    @Override
    public String getBucketLocation(GetBucketLocationRequest getBucketLocationRequest)
            throws SdkClientException
    {
        return delegate.getBucketLocation(getBucketLocationRequest);
    }

    @Override
    public Bucket createBucket(CreateBucketRequest createBucketRequest)
            throws SdkClientException
    {
        return delegate.createBucket(createBucketRequest);
    }

    @Override
    public Bucket createBucket(String bucketName)
            throws SdkClientException
    {
        return delegate.createBucket(bucketName);
    }

    @Override
    @Deprecated
    public Bucket createBucket(String bucketName, com.amazonaws.services.s3.model.Region region)
            throws SdkClientException
    {
        return delegate.createBucket(bucketName, region);
    }

    @Override
    @Deprecated
    public Bucket createBucket(String bucketName, String region)
            throws SdkClientException
    {
        return delegate.createBucket(bucketName, region);
    }

    @Override
    public AccessControlList getObjectAcl(String bucketName, String key)
            throws SdkClientException
    {
        return delegate.getObjectAcl(bucketName, key);
    }

    @Override
    public AccessControlList getObjectAcl(String bucketName, String key, String versionId)
            throws SdkClientException
    {
        return delegate.getObjectAcl(bucketName, key, versionId);
    }

    @Override
    public AccessControlList getObjectAcl(GetObjectAclRequest getObjectAclRequest)
            throws SdkClientException
    {
        return delegate.getObjectAcl(getObjectAclRequest);
    }

    @Override
    public void setObjectAcl(String bucketName, String key, AccessControlList acl)
            throws SdkClientException
    {
        delegate.setObjectAcl(bucketName, key, acl);
    }

    @Override
    public void setObjectAcl(String bucketName, String key, CannedAccessControlList acl)
            throws SdkClientException
    {
        delegate.setObjectAcl(bucketName, key, acl);
    }

    @Override
    public void setObjectAcl(String bucketName, String key, String versionId, AccessControlList acl)
            throws SdkClientException
    {
        delegate.setObjectAcl(bucketName, key, versionId, acl);
    }

    @Override
    public void setObjectAcl(String bucketName, String key, String versionId, CannedAccessControlList acl)
            throws SdkClientException
    {
        delegate.setObjectAcl(bucketName, key, versionId, acl);
    }

    @Override
    public void setObjectAcl(SetObjectAclRequest setObjectAclRequest)
            throws SdkClientException
    {
        delegate.setObjectAcl(setObjectAclRequest);
    }

    @Override
    public AccessControlList getBucketAcl(String bucketName)
            throws SdkClientException
    {
        return delegate.getBucketAcl(bucketName);
    }

    @Override
    public void setBucketAcl(SetBucketAclRequest setBucketAclRequest)
            throws SdkClientException
    {
        delegate.setBucketAcl(setBucketAclRequest);
    }

    @Override
    public AccessControlList getBucketAcl(GetBucketAclRequest getBucketAclRequest)
            throws SdkClientException
    {
        return delegate.getBucketAcl(getBucketAclRequest);
    }

    @Override
    public void setBucketAcl(String bucketName, AccessControlList acl)
            throws SdkClientException
    {
        delegate.setBucketAcl(bucketName, acl);
    }

    @Override
    public void setBucketAcl(String bucketName, CannedAccessControlList acl)
            throws SdkClientException
    {
        delegate.setBucketAcl(bucketName, acl);
    }

    @Override
    public ObjectMetadata getObjectMetadata(String bucketName, String key)
            throws SdkClientException
    {
        return delegate.getObjectMetadata(bucketName, key);
    }

    @Override
    public ObjectMetadata getObjectMetadata(GetObjectMetadataRequest getObjectMetadataRequest)
            throws SdkClientException
    {
        return delegate.getObjectMetadata(getObjectMetadataRequest);
    }

    @Override
    public S3Object getObject(String bucketName, String key)
            throws SdkClientException
    {
        return delegate.getObject(bucketName, key);
    }

    @Override
    public S3Object getObject(GetObjectRequest getObjectRequest)
            throws SdkClientException
    {
        return getPresignedUrl("GET", getObjectRequest.getBucketName(), getObjectRequest.getKey())
                .map(presigned -> {
                    PresignedUrlDownloadRequest presignedUrlDownloadRequest = new PresignedUrlDownloadRequest(presigned.url);
                    Optional.ofNullable(getObjectRequest.getRange())
                            .ifPresent(range -> presignedUrlDownloadRequest.withRange(range[0], range[1]));
                    return delegate.download(presignedUrlDownloadRequest).getS3Object();
                })
                .orElseGet(() -> delegate.getObject(getObjectRequest));
    }

    @Override
    public ObjectMetadata getObject(GetObjectRequest getObjectRequest, File destinationFile)
            throws SdkClientException
    {
        return getPresignedUrl("GET", getObjectRequest.getBucketName(), getObjectRequest.getKey())
                .map(presigned -> {
                    PresignedUrlDownloadRequest presignedUrlDownloadRequest = new PresignedUrlDownloadRequest(presigned.url);
                    Optional.ofNullable(getObjectRequest.getRange())
                            .ifPresent(range -> presignedUrlDownloadRequest.withRange(range[0], range[1]));
                    delegate.download(presignedUrlDownloadRequest, destinationFile);
                    return presigned.objectMetadata;
                })
                .orElseGet(() -> delegate.getObject(getObjectRequest, destinationFile));
    }

    @Override
    public String getObjectAsString(String bucketName, String key)
            throws SdkClientException
    {
        S3Object object = getObject(bucketName, key);
        try {
            return IOUtils.toString(object.getObjectContent());
        }
        catch (IOException e) {
            throw new SdkClientException("Error streaming content from S3 during download", e);
        }
        finally {
            IOUtils.closeQuietly(object, null);
        }
    }

    @Override
    public GetObjectTaggingResult getObjectTagging(GetObjectTaggingRequest getObjectTaggingRequest)
    {
        return delegate.getObjectTagging(getObjectTaggingRequest);
    }

    @Override
    public SetObjectTaggingResult setObjectTagging(SetObjectTaggingRequest setObjectTaggingRequest)
    {
        return delegate.setObjectTagging(setObjectTaggingRequest);
    }

    @Override
    public DeleteObjectTaggingResult deleteObjectTagging(DeleteObjectTaggingRequest deleteObjectTaggingRequest)
    {
        return delegate.deleteObjectTagging(deleteObjectTaggingRequest);
    }

    @Override
    public void deleteBucket(DeleteBucketRequest deleteBucketRequest)
            throws SdkClientException
    {
        delegate.deleteBucket(deleteBucketRequest);
    }

    @Override
    public void deleteBucket(String bucketName)
            throws SdkClientException
    {
        delegate.deleteBucket(bucketName);
    }

    @Override
    public PutObjectResult putObject(PutObjectRequest putObjectRequest)
            throws SdkClientException
    {
        return getPresignedUrl("PUT", putObjectRequest.getBucketName(), putObjectRequest.getKey())
                .map(presigned -> {
                    PresignedUrlUploadRequest presignedUrlUploadRequest = new PresignedUrlUploadRequest(presigned.url).withMetadata(putObjectRequest.getMetadata());
                    if (putObjectRequest.getFile() != null) {
                        presignedUrlUploadRequest.withFile(putObjectRequest.getFile());
                    }
                    if (putObjectRequest.getInputStream() != null) {
                        presignedUrlUploadRequest.withInputStream(putObjectRequest.getInputStream());
                    }
                    PresignedUrlUploadResult result = delegate.upload(presignedUrlUploadRequest);
                    return toPutObjectResult(result);
                })
                .orElseGet(() -> delegate.putObject(putObjectRequest));
    }

    @Override
    public PutObjectResult putObject(String bucketName, String key, File file)
            throws SdkClientException
    {
        return getPresignedUrl("PUT", bucketName, key)
                .map(presigned -> {
                    PresignedUrlUploadRequest presignedUrlUploadRequest = new PresignedUrlUploadRequest(presigned.url).withFile(file).withMetadata(presigned.objectMetadata);
                    PresignedUrlUploadResult result = delegate.upload(presignedUrlUploadRequest);
                    return toPutObjectResult(result);
                })
                .orElseGet(() -> delegate.putObject(bucketName, key, file));
    }

    @Override
    public PutObjectResult putObject(String bucketName, String key, InputStream input, ObjectMetadata metadata)
            throws SdkClientException
    {
        return getPresignedUrl("PUT", bucketName, key)
                .map(presigned -> {
                    PresignedUrlUploadRequest presignedUrlUploadRequest = new PresignedUrlUploadRequest(presigned.url).withInputStream(input).withMetadata(metadata);
                    PresignedUrlUploadResult result = delegate.upload(presignedUrlUploadRequest);
                    return toPutObjectResult(result);
                })
                .orElseGet(() -> delegate.putObject(bucketName, key, input, metadata));
    }

    @Override
    public PutObjectResult putObject(String bucketName, String key, String content)
            throws SdkClientException
    {
        return getPresignedUrl("PUT", bucketName, key)
                .map(presigned -> {
                    PresignedUrlUploadRequest presignedUrlUploadRequest = new PresignedUrlUploadRequest(presigned.url)
                            .withInputStream(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)))
                            .withMetadata(presigned.objectMetadata);
                    PresignedUrlUploadResult result = delegate.upload(presignedUrlUploadRequest);
                    return toPutObjectResult(result);
                })
                .orElseGet(() -> delegate.putObject(bucketName, key, content));
    }

    private static PutObjectResult toPutObjectResult(PresignedUrlUploadResult result)
    {
        PutObjectResult putObjectResult = new PutObjectResult();
        putObjectResult.setMetadata(result.getMetadata());
        putObjectResult.setContentMd5(result.getContentMd5());
        return putObjectResult;
    }

    private static UploadPartResult toUploadPartResult(PresignedUrlUploadResult result, int partNumber)
    {
        UploadPartResult uploadPartResult = new UploadPartResult();
        uploadPartResult.setETag(result.getMetadata().getETag());
        uploadPartResult.setPartNumber(partNumber);
        return uploadPartResult;
    }

    @Override
    public CopyObjectResult copyObject(String sourceBucketName, String sourceKey, String destinationBucketName, String destinationKey)
            throws SdkClientException
    {
        return delegate.copyObject(sourceBucketName, sourceKey, destinationBucketName, destinationKey);
    }

    @Override
    public CopyObjectResult copyObject(CopyObjectRequest copyObjectRequest)
            throws SdkClientException
    {
        return delegate.copyObject(copyObjectRequest);
    }

    @Override
    public CopyPartResult copyPart(CopyPartRequest copyPartRequest)
            throws SdkClientException
    {
        return delegate.copyPart(copyPartRequest);
    }

    @Override
    public void deleteObject(String bucketName, String key)
            throws SdkClientException
    {
        delegate.deleteObject(bucketName, key);
    }

    @Override
    public void deleteObject(DeleteObjectRequest deleteObjectRequest)
            throws SdkClientException
    {
        delegate.deleteObject(deleteObjectRequest);
    }

    @Override
    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectsRequest)
            throws SdkClientException
    {
        return delegate.deleteObjects(deleteObjectsRequest);
    }

    @Override
    public void deleteVersion(String bucketName, String key, String versionId)
            throws SdkClientException
    {
        delegate.deleteVersion(bucketName, key, versionId);
    }

    @Override
    public void deleteVersion(DeleteVersionRequest deleteVersionRequest)
            throws SdkClientException
    {
        delegate.deleteVersion(deleteVersionRequest);
    }

    @Override
    public BucketLoggingConfiguration getBucketLoggingConfiguration(String bucketName)
            throws SdkClientException
    {
        return delegate.getBucketLoggingConfiguration(bucketName);
    }

    @Override
    public BucketLoggingConfiguration getBucketLoggingConfiguration(GetBucketLoggingConfigurationRequest getBucketLoggingConfigurationRequest)
            throws SdkClientException
    {
        return delegate.getBucketLoggingConfiguration(getBucketLoggingConfigurationRequest);
    }

    @Override
    public void setBucketLoggingConfiguration(SetBucketLoggingConfigurationRequest setBucketLoggingConfigurationRequest)
            throws SdkClientException
    {
        delegate.setBucketLoggingConfiguration(setBucketLoggingConfigurationRequest);
    }

    @Override
    public BucketVersioningConfiguration getBucketVersioningConfiguration(String bucketName)
            throws SdkClientException
    {
        return delegate.getBucketVersioningConfiguration(bucketName);
    }

    @Override
    public BucketVersioningConfiguration getBucketVersioningConfiguration(GetBucketVersioningConfigurationRequest getBucketVersioningConfigurationRequest)
            throws SdkClientException
    {
        return delegate.getBucketVersioningConfiguration(getBucketVersioningConfigurationRequest);
    }

    @Override
    public void setBucketVersioningConfiguration(SetBucketVersioningConfigurationRequest setBucketVersioningConfigurationRequest)
            throws SdkClientException
    {
        delegate.setBucketVersioningConfiguration(setBucketVersioningConfigurationRequest);
    }

    @Override
    public BucketLifecycleConfiguration getBucketLifecycleConfiguration(String bucketName)
    {
        return delegate.getBucketLifecycleConfiguration(bucketName);
    }

    @Override
    public BucketLifecycleConfiguration getBucketLifecycleConfiguration(GetBucketLifecycleConfigurationRequest getBucketLifecycleConfigurationRequest)
    {
        return delegate.getBucketLifecycleConfiguration(getBucketLifecycleConfigurationRequest);
    }

    @Override
    public void setBucketLifecycleConfiguration(String bucketName, BucketLifecycleConfiguration bucketLifecycleConfiguration)
    {
        delegate.setBucketLifecycleConfiguration(bucketName, bucketLifecycleConfiguration);
    }

    @Override
    public void setBucketLifecycleConfiguration(SetBucketLifecycleConfigurationRequest setBucketLifecycleConfigurationRequest)
    {
        delegate.setBucketLifecycleConfiguration(setBucketLifecycleConfigurationRequest);
    }

    @Override
    public void deleteBucketLifecycleConfiguration(String bucketName)
    {
        delegate.deleteBucketLifecycleConfiguration(bucketName);
    }

    @Override
    public void deleteBucketLifecycleConfiguration(DeleteBucketLifecycleConfigurationRequest deleteBucketLifecycleConfigurationRequest)
    {
        delegate.deleteBucketLifecycleConfiguration(deleteBucketLifecycleConfigurationRequest);
    }

    @Override
    public BucketCrossOriginConfiguration getBucketCrossOriginConfiguration(String bucketName)
    {
        return delegate.getBucketCrossOriginConfiguration(bucketName);
    }

    @Override
    public BucketCrossOriginConfiguration getBucketCrossOriginConfiguration(GetBucketCrossOriginConfigurationRequest getBucketCrossOriginConfigurationRequest)
    {
        return delegate.getBucketCrossOriginConfiguration(getBucketCrossOriginConfigurationRequest);
    }

    @Override
    public void setBucketCrossOriginConfiguration(String bucketName, BucketCrossOriginConfiguration bucketCrossOriginConfiguration)
    {
        delegate.setBucketCrossOriginConfiguration(bucketName, bucketCrossOriginConfiguration);
    }

    @Override
    public void setBucketCrossOriginConfiguration(SetBucketCrossOriginConfigurationRequest setBucketCrossOriginConfigurationRequest)
    {
        delegate.setBucketCrossOriginConfiguration(setBucketCrossOriginConfigurationRequest);
    }

    @Override
    public void deleteBucketCrossOriginConfiguration(String bucketName)
    {
        delegate.deleteBucketCrossOriginConfiguration(bucketName);
    }

    @Override
    public void deleteBucketCrossOriginConfiguration(DeleteBucketCrossOriginConfigurationRequest deleteBucketCrossOriginConfigurationRequest)
    {
        delegate.deleteBucketCrossOriginConfiguration(deleteBucketCrossOriginConfigurationRequest);
    }

    @Override
    public BucketTaggingConfiguration getBucketTaggingConfiguration(String bucketName)
    {
        return delegate.getBucketTaggingConfiguration(bucketName);
    }

    @Override
    public BucketTaggingConfiguration getBucketTaggingConfiguration(GetBucketTaggingConfigurationRequest getBucketTaggingConfigurationRequest)
    {
        return delegate.getBucketTaggingConfiguration(getBucketTaggingConfigurationRequest);
    }

    @Override
    public void setBucketTaggingConfiguration(String bucketName, BucketTaggingConfiguration bucketTaggingConfiguration)
    {
        delegate.setBucketTaggingConfiguration(bucketName, bucketTaggingConfiguration);
    }

    @Override
    public void setBucketTaggingConfiguration(SetBucketTaggingConfigurationRequest setBucketTaggingConfigurationRequest)
    {
        delegate.setBucketTaggingConfiguration(setBucketTaggingConfigurationRequest);
    }

    @Override
    public void deleteBucketTaggingConfiguration(String bucketName)
    {
        delegate.deleteBucketTaggingConfiguration(bucketName);
    }

    @Override
    public void deleteBucketTaggingConfiguration(DeleteBucketTaggingConfigurationRequest deleteBucketTaggingConfigurationRequest)
    {
        delegate.deleteBucketTaggingConfiguration(deleteBucketTaggingConfigurationRequest);
    }

    @Override
    public BucketNotificationConfiguration getBucketNotificationConfiguration(String bucketName)
            throws SdkClientException
    {
        return delegate.getBucketNotificationConfiguration(bucketName);
    }

    @Override
    public BucketNotificationConfiguration getBucketNotificationConfiguration(GetBucketNotificationConfigurationRequest getBucketNotificationConfigurationRequest)
            throws SdkClientException
    {
        return delegate.getBucketNotificationConfiguration(getBucketNotificationConfigurationRequest);
    }

    @Override
    public void setBucketNotificationConfiguration(SetBucketNotificationConfigurationRequest setBucketNotificationConfigurationRequest)
            throws SdkClientException
    {
        delegate.setBucketNotificationConfiguration(setBucketNotificationConfigurationRequest);
    }

    @Override
    public void setBucketNotificationConfiguration(String bucketName, BucketNotificationConfiguration bucketNotificationConfiguration)
            throws SdkClientException
    {
        delegate.setBucketNotificationConfiguration(bucketName, bucketNotificationConfiguration);
    }

    @Override
    public BucketWebsiteConfiguration getBucketWebsiteConfiguration(String bucketName)
            throws SdkClientException
    {
        return delegate.getBucketWebsiteConfiguration(bucketName);
    }

    @Override
    public BucketWebsiteConfiguration getBucketWebsiteConfiguration(GetBucketWebsiteConfigurationRequest getBucketWebsiteConfigurationRequest)
            throws SdkClientException
    {
        return delegate.getBucketWebsiteConfiguration(getBucketWebsiteConfigurationRequest);
    }

    @Override
    public void setBucketWebsiteConfiguration(String bucketName, BucketWebsiteConfiguration configuration)
            throws SdkClientException
    {
        delegate.setBucketWebsiteConfiguration(bucketName, configuration);
    }

    @Override
    public void setBucketWebsiteConfiguration(SetBucketWebsiteConfigurationRequest setBucketWebsiteConfigurationRequest)
            throws SdkClientException
    {
        delegate.setBucketWebsiteConfiguration(setBucketWebsiteConfigurationRequest);
    }

    @Override
    public void deleteBucketWebsiteConfiguration(String bucketName)
            throws SdkClientException
    {
        delegate.deleteBucketWebsiteConfiguration(bucketName);
    }

    @Override
    public void deleteBucketWebsiteConfiguration(DeleteBucketWebsiteConfigurationRequest deleteBucketWebsiteConfigurationRequest)
            throws SdkClientException
    {
        delegate.deleteBucketWebsiteConfiguration(deleteBucketWebsiteConfigurationRequest);
    }

    @Override
    public BucketPolicy getBucketPolicy(String bucketName)
            throws SdkClientException
    {
        return delegate.getBucketPolicy(bucketName);
    }

    @Override
    public BucketPolicy getBucketPolicy(GetBucketPolicyRequest getBucketPolicyRequest)
            throws SdkClientException
    {
        return delegate.getBucketPolicy(getBucketPolicyRequest);
    }

    @Override
    public void setBucketPolicy(String bucketName, String policyText)
            throws SdkClientException
    {
        delegate.setBucketPolicy(bucketName, policyText);
    }

    @Override
    public void setBucketPolicy(SetBucketPolicyRequest setBucketPolicyRequest)
            throws SdkClientException
    {
        delegate.setBucketPolicy(setBucketPolicyRequest);
    }

    @Override
    public void deleteBucketPolicy(String bucketName)
            throws SdkClientException
    {
        delegate.deleteBucketPolicy(bucketName);
    }

    @Override
    public void deleteBucketPolicy(DeleteBucketPolicyRequest deleteBucketPolicyRequest)
            throws SdkClientException
    {
        delegate.deleteBucketPolicy(deleteBucketPolicyRequest);
    }

    @Override
    public URL generatePresignedUrl(String bucketName, String key, Date expiration)
            throws SdkClientException
    {
        return delegate.generatePresignedUrl(bucketName, key, expiration);
    }

    @Override
    public URL generatePresignedUrl(String bucketName, String key, Date expiration, HttpMethod method)
            throws SdkClientException
    {
        return delegate.generatePresignedUrl(bucketName, key, expiration, method);
    }

    @Override
    public URL generatePresignedUrl(GeneratePresignedUrlRequest generatePresignedUrlRequest)
            throws SdkClientException
    {
        return delegate.generatePresignedUrl(generatePresignedUrlRequest);
    }

    @Override
    public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request)
            throws SdkClientException
    {
        return delegate.initiateMultipartUpload(request);
    }

    @Override
    public UploadPartResult uploadPart(UploadPartRequest request)
            throws SdkClientException
    {
        return getPresignedUrl("PUT", request.getBucketName(), request.getKey(), Optional.of(request))
                .map(presigned -> {
                    PresignedUrlUploadRequest presignedUrlUploadRequest = new PresignedUrlUploadRequest(presigned.url);
                    if (request.getFile() != null) {
                        presignedUrlUploadRequest.withFile(request.getFile());
                    }
                    if (request.getInputStream() != null) {
                        presignedUrlUploadRequest.withInputStream(request.getInputStream());
                    }
                    PresignedUrlUploadResult result = delegate.upload(presignedUrlUploadRequest);
                    return toUploadPartResult(result, request.getPartNumber());
                })
                .orElseGet(() -> delegate.uploadPart(request));
    }

    @Override
    public PartListing listParts(ListPartsRequest request)
            throws SdkClientException
    {
        return delegate.listParts(request);
    }

    @Override
    public void abortMultipartUpload(AbortMultipartUploadRequest request)
            throws SdkClientException
    {
        delegate.abortMultipartUpload(request);
    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request)
            throws SdkClientException
    {
        return delegate.completeMultipartUpload(request);
    }

    @Override
    public MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest request)
            throws SdkClientException
    {
        return delegate.listMultipartUploads(request);
    }

    @Override
    public S3ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request)
    {
        return delegate.getCachedResponseMetadata(request);
    }

    @Override
    @Deprecated
    public void restoreObject(RestoreObjectRequest request)
            throws AmazonServiceException
    {
        delegate.restoreObject(request);
    }

    @Override
    public RestoreObjectResult restoreObjectV2(RestoreObjectRequest request)
            throws AmazonServiceException
    {
        return delegate.restoreObjectV2(request);
    }

    @Override
    @Deprecated
    public void restoreObject(String bucketName, String key, int expirationInDays)
            throws AmazonServiceException
    {
        delegate.restoreObject(bucketName, key, expirationInDays);
    }

    @Override
    public void enableRequesterPays(String bucketName)
            throws SdkClientException
    {
        delegate.enableRequesterPays(bucketName);
    }

    @Override
    public void disableRequesterPays(String bucketName)
            throws SdkClientException
    {
        delegate.disableRequesterPays(bucketName);
    }

    @Override
    public boolean isRequesterPaysEnabled(String bucketName)
            throws SdkClientException
    {
        return delegate.isRequesterPaysEnabled(bucketName);
    }

    @Override
    public void setRequestPaymentConfiguration(SetRequestPaymentConfigurationRequest setRequestPaymentConfigurationRequest)
    {
        delegate.setRequestPaymentConfiguration(setRequestPaymentConfigurationRequest);
    }

    @Override
    public void setBucketReplicationConfiguration(String bucketName, BucketReplicationConfiguration configuration)
            throws SdkClientException
    {
        delegate.setBucketReplicationConfiguration(bucketName, configuration);
    }

    @Override
    public void setBucketReplicationConfiguration(SetBucketReplicationConfigurationRequest setBucketReplicationConfigurationRequest)
            throws SdkClientException
    {
        delegate.setBucketReplicationConfiguration(setBucketReplicationConfigurationRequest);
    }

    @Override
    public BucketReplicationConfiguration getBucketReplicationConfiguration(String bucketName)
            throws SdkClientException
    {
        return delegate.getBucketReplicationConfiguration(bucketName);
    }

    @Override
    public BucketReplicationConfiguration getBucketReplicationConfiguration(GetBucketReplicationConfigurationRequest getBucketReplicationConfigurationRequest)
            throws SdkClientException
    {
        return delegate.getBucketReplicationConfiguration(getBucketReplicationConfigurationRequest);
    }

    @Override
    public void deleteBucketReplicationConfiguration(String bucketName)
            throws SdkClientException
    {
        delegate.deleteBucketReplicationConfiguration(bucketName);
    }

    @Override
    public void deleteBucketReplicationConfiguration(DeleteBucketReplicationConfigurationRequest request)
            throws SdkClientException
    {
        delegate.deleteBucketReplicationConfiguration(request);
    }

    @Override
    public boolean doesObjectExist(String bucketName, String objectName)
            throws SdkClientException
    {
        return delegate.doesObjectExist(bucketName, objectName);
    }

    @Override
    public BucketAccelerateConfiguration getBucketAccelerateConfiguration(String bucketName)
            throws SdkClientException
    {
        return delegate.getBucketAccelerateConfiguration(bucketName);
    }

    @Override
    public BucketAccelerateConfiguration getBucketAccelerateConfiguration(GetBucketAccelerateConfigurationRequest getBucketAccelerateConfigurationRequest)
            throws SdkClientException
    {
        return delegate.getBucketAccelerateConfiguration(getBucketAccelerateConfigurationRequest);
    }

    @Override
    public void setBucketAccelerateConfiguration(String bucketName, BucketAccelerateConfiguration accelerateConfiguration)
            throws SdkClientException
    {
        delegate.setBucketAccelerateConfiguration(bucketName, accelerateConfiguration);
    }

    @Override
    public void setBucketAccelerateConfiguration(SetBucketAccelerateConfigurationRequest setBucketAccelerateConfigurationRequest)
            throws SdkClientException
    {
        delegate.setBucketAccelerateConfiguration(setBucketAccelerateConfigurationRequest);
    }

    @Override
    public DeleteBucketMetricsConfigurationResult deleteBucketMetricsConfiguration(String bucketName, String id)
            throws SdkClientException
    {
        return delegate.deleteBucketMetricsConfiguration(bucketName, id);
    }

    @Override
    public DeleteBucketMetricsConfigurationResult deleteBucketMetricsConfiguration(DeleteBucketMetricsConfigurationRequest deleteBucketMetricsConfigurationRequest)
            throws SdkClientException
    {
        return delegate.deleteBucketMetricsConfiguration(deleteBucketMetricsConfigurationRequest);
    }

    @Override
    public GetBucketMetricsConfigurationResult getBucketMetricsConfiguration(String bucketName, String id)
            throws SdkClientException
    {
        return delegate.getBucketMetricsConfiguration(bucketName, id);
    }

    @Override
    public GetBucketMetricsConfigurationResult getBucketMetricsConfiguration(GetBucketMetricsConfigurationRequest getBucketMetricsConfigurationRequest)
            throws SdkClientException
    {
        return delegate.getBucketMetricsConfiguration(getBucketMetricsConfigurationRequest);
    }

    @Override
    public SetBucketMetricsConfigurationResult setBucketMetricsConfiguration(String bucketName, MetricsConfiguration metricsConfiguration)
            throws SdkClientException
    {
        return delegate.setBucketMetricsConfiguration(bucketName, metricsConfiguration);
    }

    @Override
    public SetBucketMetricsConfigurationResult setBucketMetricsConfiguration(SetBucketMetricsConfigurationRequest setBucketMetricsConfigurationRequest)
            throws SdkClientException
    {
        return delegate.setBucketMetricsConfiguration(setBucketMetricsConfigurationRequest);
    }

    @Override
    public ListBucketMetricsConfigurationsResult listBucketMetricsConfigurations(ListBucketMetricsConfigurationsRequest listBucketMetricsConfigurationsRequest)
            throws SdkClientException
    {
        return delegate.listBucketMetricsConfigurations(listBucketMetricsConfigurationsRequest);
    }

    @Override
    public DeleteBucketOwnershipControlsResult deleteBucketOwnershipControls(DeleteBucketOwnershipControlsRequest deleteBucketOwnershipControlsRequest)
            throws SdkClientException
    {
        return delegate.deleteBucketOwnershipControls(deleteBucketOwnershipControlsRequest);
    }

    @Override
    public GetBucketOwnershipControlsResult getBucketOwnershipControls(GetBucketOwnershipControlsRequest getBucketOwnershipControlsRequest)
            throws SdkClientException
    {
        return delegate.getBucketOwnershipControls(getBucketOwnershipControlsRequest);
    }

    @Override
    public SetBucketOwnershipControlsResult setBucketOwnershipControls(String bucketName, OwnershipControls ownershipControls)
            throws SdkClientException
    {
        return delegate.setBucketOwnershipControls(bucketName, ownershipControls);
    }

    @Override
    public SetBucketOwnershipControlsResult setBucketOwnershipControls(SetBucketOwnershipControlsRequest setBucketOwnershipControlsRequest)
            throws SdkClientException
    {
        return delegate.setBucketOwnershipControls(setBucketOwnershipControlsRequest);
    }

    @Override
    public DeleteBucketAnalyticsConfigurationResult deleteBucketAnalyticsConfiguration(String bucketName, String id)
            throws SdkClientException
    {
        return delegate.deleteBucketAnalyticsConfiguration(bucketName, id);
    }

    @Override
    public DeleteBucketAnalyticsConfigurationResult deleteBucketAnalyticsConfiguration(DeleteBucketAnalyticsConfigurationRequest deleteBucketAnalyticsConfigurationRequest)
            throws SdkClientException
    {
        return delegate.deleteBucketAnalyticsConfiguration(deleteBucketAnalyticsConfigurationRequest);
    }

    @Override
    public GetBucketAnalyticsConfigurationResult getBucketAnalyticsConfiguration(String bucketName, String id)
            throws SdkClientException
    {
        return delegate.getBucketAnalyticsConfiguration(bucketName, id);
    }

    @Override
    public GetBucketAnalyticsConfigurationResult getBucketAnalyticsConfiguration(GetBucketAnalyticsConfigurationRequest getBucketAnalyticsConfigurationRequest)
            throws SdkClientException
    {
        return delegate.getBucketAnalyticsConfiguration(getBucketAnalyticsConfigurationRequest);
    }

    @Override
    public SetBucketAnalyticsConfigurationResult setBucketAnalyticsConfiguration(String bucketName, AnalyticsConfiguration analyticsConfiguration)
            throws SdkClientException
    {
        return delegate.setBucketAnalyticsConfiguration(bucketName, analyticsConfiguration);
    }

    @Override
    public SetBucketAnalyticsConfigurationResult setBucketAnalyticsConfiguration(SetBucketAnalyticsConfigurationRequest setBucketAnalyticsConfigurationRequest)
            throws SdkClientException
    {
        return delegate.setBucketAnalyticsConfiguration(setBucketAnalyticsConfigurationRequest);
    }

    @Override
    public ListBucketAnalyticsConfigurationsResult listBucketAnalyticsConfigurations(ListBucketAnalyticsConfigurationsRequest listBucketAnalyticsConfigurationsRequest)
            throws SdkClientException
    {
        return delegate.listBucketAnalyticsConfigurations(listBucketAnalyticsConfigurationsRequest);
    }

    @Override
    public DeleteBucketIntelligentTieringConfigurationResult deleteBucketIntelligentTieringConfiguration(String bucketName, String id)
            throws SdkClientException
    {
        return delegate.deleteBucketIntelligentTieringConfiguration(bucketName, id);
    }

    @Override
    public DeleteBucketIntelligentTieringConfigurationResult deleteBucketIntelligentTieringConfiguration(DeleteBucketIntelligentTieringConfigurationRequest deleteBucketIntelligentTieringConfigurationRequest)
            throws SdkClientException
    {
        return delegate.deleteBucketIntelligentTieringConfiguration(deleteBucketIntelligentTieringConfigurationRequest);
    }

    @Override
    public GetBucketIntelligentTieringConfigurationResult getBucketIntelligentTieringConfiguration(String bucketName, String id)
            throws SdkClientException
    {
        return delegate.getBucketIntelligentTieringConfiguration(bucketName, id);
    }

    @Override
    public GetBucketIntelligentTieringConfigurationResult getBucketIntelligentTieringConfiguration(GetBucketIntelligentTieringConfigurationRequest getBucketIntelligentTieringConfigurationRequest)
            throws SdkClientException
    {
        return delegate.getBucketIntelligentTieringConfiguration(getBucketIntelligentTieringConfigurationRequest);
    }

    @Override
    public SetBucketIntelligentTieringConfigurationResult setBucketIntelligentTieringConfiguration(String bucketName, IntelligentTieringConfiguration intelligentTieringConfiguration)
            throws SdkClientException
    {
        return delegate.setBucketIntelligentTieringConfiguration(bucketName, intelligentTieringConfiguration);
    }

    @Override
    public SetBucketIntelligentTieringConfigurationResult setBucketIntelligentTieringConfiguration(SetBucketIntelligentTieringConfigurationRequest setBucketIntelligentTieringConfigurationRequest)
            throws SdkClientException
    {
        return delegate.setBucketIntelligentTieringConfiguration(setBucketIntelligentTieringConfigurationRequest);
    }

    @Override
    public ListBucketIntelligentTieringConfigurationsResult listBucketIntelligentTieringConfigurations(ListBucketIntelligentTieringConfigurationsRequest listBucketIntelligentTieringConfigurationsRequest)
            throws SdkClientException
    {
        return delegate.listBucketIntelligentTieringConfigurations(listBucketIntelligentTieringConfigurationsRequest);
    }

    @Override
    public DeleteBucketInventoryConfigurationResult deleteBucketInventoryConfiguration(String bucketName, String id)
            throws SdkClientException
    {
        return delegate.deleteBucketInventoryConfiguration(bucketName, id);
    }

    @Override
    public DeleteBucketInventoryConfigurationResult deleteBucketInventoryConfiguration(DeleteBucketInventoryConfigurationRequest deleteBucketInventoryConfigurationRequest)
            throws SdkClientException
    {
        return delegate.deleteBucketInventoryConfiguration(deleteBucketInventoryConfigurationRequest);
    }

    @Override
    public GetBucketInventoryConfigurationResult getBucketInventoryConfiguration(String bucketName, String id)
            throws SdkClientException
    {
        return delegate.getBucketInventoryConfiguration(bucketName, id);
    }

    @Override
    public GetBucketInventoryConfigurationResult getBucketInventoryConfiguration(GetBucketInventoryConfigurationRequest getBucketInventoryConfigurationRequest)
            throws SdkClientException
    {
        return delegate.getBucketInventoryConfiguration(getBucketInventoryConfigurationRequest);
    }

    @Override
    public SetBucketInventoryConfigurationResult setBucketInventoryConfiguration(String bucketName, InventoryConfiguration inventoryConfiguration)
            throws SdkClientException
    {
        return delegate.setBucketInventoryConfiguration(bucketName, inventoryConfiguration);
    }

    @Override
    public SetBucketInventoryConfigurationResult setBucketInventoryConfiguration(SetBucketInventoryConfigurationRequest setBucketInventoryConfigurationRequest)
            throws SdkClientException
    {
        return delegate.setBucketInventoryConfiguration(setBucketInventoryConfigurationRequest);
    }

    @Override
    public ListBucketInventoryConfigurationsResult listBucketInventoryConfigurations(ListBucketInventoryConfigurationsRequest listBucketInventoryConfigurationsRequest)
            throws SdkClientException
    {
        return delegate.listBucketInventoryConfigurations(listBucketInventoryConfigurationsRequest);
    }

    @Override
    public DeleteBucketEncryptionResult deleteBucketEncryption(String bucketName)
            throws SdkClientException
    {
        return delegate.deleteBucketEncryption(bucketName);
    }

    @Override
    public DeleteBucketEncryptionResult deleteBucketEncryption(DeleteBucketEncryptionRequest request)
            throws SdkClientException
    {
        return delegate.deleteBucketEncryption(request);
    }

    @Override
    public GetBucketEncryptionResult getBucketEncryption(String bucketName)
            throws SdkClientException
    {
        return delegate.getBucketEncryption(bucketName);
    }

    @Override
    public GetBucketEncryptionResult getBucketEncryption(GetBucketEncryptionRequest request)
            throws SdkClientException
    {
        return delegate.getBucketEncryption(request);
    }

    @Override
    public SetBucketEncryptionResult setBucketEncryption(SetBucketEncryptionRequest setBucketEncryptionRequest)
            throws SdkClientException
    {
        return delegate.setBucketEncryption(setBucketEncryptionRequest);
    }

    @Override
    public SetPublicAccessBlockResult setPublicAccessBlock(SetPublicAccessBlockRequest request)
    {
        return delegate.setPublicAccessBlock(request);
    }

    @Override
    public GetPublicAccessBlockResult getPublicAccessBlock(GetPublicAccessBlockRequest request)
    {
        return delegate.getPublicAccessBlock(request);
    }

    @Override
    public DeletePublicAccessBlockResult deletePublicAccessBlock(DeletePublicAccessBlockRequest request)
    {
        return delegate.deletePublicAccessBlock(request);
    }

    @Override
    public GetBucketPolicyStatusResult getBucketPolicyStatus(GetBucketPolicyStatusRequest request)
    {
        return delegate.getBucketPolicyStatus(request);
    }

    @Override
    public SelectObjectContentResult selectObjectContent(SelectObjectContentRequest selectRequest)
            throws SdkClientException
    {
        return delegate.selectObjectContent(selectRequest);
    }

    @Override
    public SetObjectLegalHoldResult setObjectLegalHold(SetObjectLegalHoldRequest setObjectLegalHoldRequest)
    {
        return delegate.setObjectLegalHold(setObjectLegalHoldRequest);
    }

    @Override
    public GetObjectLegalHoldResult getObjectLegalHold(GetObjectLegalHoldRequest getObjectLegalHoldRequest)
    {
        return delegate.getObjectLegalHold(getObjectLegalHoldRequest);
    }

    @Override
    public SetObjectLockConfigurationResult setObjectLockConfiguration(SetObjectLockConfigurationRequest setObjectLockConfigurationRequest)
    {
        return delegate.setObjectLockConfiguration(setObjectLockConfigurationRequest);
    }

    @Override
    public GetObjectLockConfigurationResult getObjectLockConfiguration(GetObjectLockConfigurationRequest getObjectLockConfigurationRequest)
    {
        return delegate.getObjectLockConfiguration(getObjectLockConfigurationRequest);
    }

    @Override
    public SetObjectRetentionResult setObjectRetention(SetObjectRetentionRequest setObjectRetentionRequest)
    {
        return delegate.setObjectRetention(setObjectRetentionRequest);
    }

    @Override
    public GetObjectRetentionResult getObjectRetention(GetObjectRetentionRequest getObjectRetentionRequest)
    {
        return delegate.getObjectRetention(getObjectRetentionRequest);
    }

    @Override
    public WriteGetObjectResponseResult writeGetObjectResponse(WriteGetObjectResponseRequest writeGetObjectResponseRequest)
    {
        return delegate.writeGetObjectResponse(writeGetObjectResponseRequest);
    }

    @Override
    public PresignedUrlDownloadResult download(PresignedUrlDownloadRequest presignedUrlDownloadRequest)
    {
        return delegate.download(presignedUrlDownloadRequest);
    }

    @Override
    public void download(PresignedUrlDownloadRequest presignedUrlDownloadRequest, File destinationFile)
    {
        delegate.download(presignedUrlDownloadRequest, destinationFile);
    }

    @Override
    public PresignedUrlUploadResult upload(PresignedUrlUploadRequest presignedUrlUploadRequest)
    {
        return delegate.upload(presignedUrlUploadRequest);
    }

    @Override
    public void shutdown()
    {
        delegate.shutdown();
    }

    @Override
    public com.amazonaws.services.s3.model.Region getRegion()
    {
        return delegate.getRegion();
    }

    @Override
    public String getRegionName()
    {
        return delegate.getRegionName();
    }

    @Override
    public URL getUrl(String bucketName, String key)
    {
        return delegate.getUrl(bucketName, key);
    }

    @Override
    public AmazonS3Waiters waiters()
    {
        return delegate.waiters();
    }
}
