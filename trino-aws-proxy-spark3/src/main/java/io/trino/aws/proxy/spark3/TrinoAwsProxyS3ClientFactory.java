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

import com.amazonaws.services.s3.AmazonS3;
import org.apache.hadoop.fs.s3a.DefaultS3ClientFactory;

import java.io.IOException;
import java.net.URI;

public class TrinoAwsProxyS3ClientFactory
        extends DefaultS3ClientFactory
{
    @Override
    public AmazonS3 createS3Client(URI uri, S3ClientCreationParameters parameters)
            throws IOException
    {
        AmazonS3 s3Client = super.createS3Client(uri, parameters);
        return new PresignAwareAmazonS3(s3Client);
    }
}
