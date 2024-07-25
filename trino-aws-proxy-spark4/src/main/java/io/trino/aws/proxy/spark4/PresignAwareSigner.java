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

import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.http.SdkHttpFullRequest;

import java.net.URI;

import static java.util.Objects.requireNonNull;

class PresignAwareSigner
        implements Signer
{
    private final URI uri;

    PresignAwareSigner(URI uri)
    {
        this.uri = requireNonNull(uri, "uri is null");
    }

    @Override
    public SdkHttpFullRequest sign(SdkHttpFullRequest request, ExecutionAttributes executionAttributes)
    {
        // presigned requests are not signed, and we can take the opportunity
        // here to replace the URI with the presigned one
        return request.toBuilder().encodedPath("").uri(uri).build();
    }
}
