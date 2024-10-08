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

import org.apache.hadoop.fs.s3a.DefaultS3ClientFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.net.URI;

public class TrinoAwsProxyS4ClientFactory
        extends DefaultS3ClientFactory
{
    @Override
    public S3Client createS3Client(URI uri, S3ClientCreationParameters parameters)
            throws IOException
    {
        return new PresignAwareS3Client(super.createS3Client(uri, parameters));
    }
}
