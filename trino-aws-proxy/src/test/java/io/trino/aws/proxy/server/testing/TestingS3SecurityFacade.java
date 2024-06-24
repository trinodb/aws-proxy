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

import io.trino.aws.proxy.spi.rest.ParsedS3Request;
import io.trino.aws.proxy.spi.security.S3SecurityFacade;
import io.trino.aws.proxy.spi.security.S3SecurityFacadeProvider;
import io.trino.aws.proxy.spi.security.SecurityResponse;
import jakarta.ws.rs.WebApplicationException;

import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

public class TestingS3SecurityFacade
        implements S3SecurityFacadeProvider
{
    private final AtomicReference<S3SecurityFacadeProvider> delegate = new AtomicReference<>(_ -> _ -> SecurityResponse.DEFAULT);

    @Override
    public S3SecurityFacade securityFacadeForRequest(ParsedS3Request request)
            throws WebApplicationException
    {
        return delegate.get().securityFacadeForRequest(request);
    }

    public void setDelegate(S3SecurityFacadeProvider delegate)
    {
        this.delegate.set(requireNonNull(delegate, "delegate is null"));
    }

    public S3SecurityFacadeProvider delegate()
    {
        return this.delegate.get();
    }
}
