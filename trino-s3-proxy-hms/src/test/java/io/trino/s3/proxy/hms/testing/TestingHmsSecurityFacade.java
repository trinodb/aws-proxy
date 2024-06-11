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
package io.trino.s3.proxy.hms.testing;

import io.trino.s3.proxy.spi.credentials.Credentials;
import io.trino.s3.proxy.spi.hms.HmsSecurityFacade;
import io.trino.s3.proxy.spi.hms.HmsSecurityFacadeProvider;
import io.trino.s3.proxy.spi.rest.ParsedS3Request;
import jakarta.ws.rs.WebApplicationException;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

public class TestingHmsSecurityFacade
        implements HmsSecurityFacadeProvider
{
    private final AtomicReference<HmsSecurityFacadeProvider> delegate = new AtomicReference<>(((_, _, _) -> HmsSecurityFacade.DEFAULT));

    public void setDelegate(HmsSecurityFacadeProvider delegate)
    {
        this.delegate.set(requireNonNull(delegate, "delegate is null"));
    }

    public HmsSecurityFacadeProvider delegate()
    {
        return delegate.get();
    }

    @Override
    public HmsSecurityFacade securityFacadeForRequest(ParsedS3Request request, Credentials credentials, Optional<String> session)
            throws WebApplicationException
    {
        return delegate.get().securityFacadeForRequest(request, credentials, session);
    }
}
