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
import software.amazon.awssdk.services.s3.S3Client;

import static java.util.Objects.requireNonNull;

public class TestProxiedRequests
        extends AbstractTestProxiedRequests
{
    private final S3Client s3Client;

    @Inject
    public TestProxiedRequests(S3Client s3Client)
    {
        this.s3Client = requireNonNull(s3Client, "s3Client is null");
    }

    @Override
    protected S3Client buildClient()
    {
        return s3Client;
    }
}
