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
import io.trino.aws.proxy.server.rest.RequestLoggerController;
import io.trino.aws.proxy.server.security.S3SecurityController;
import io.trino.aws.proxy.spi.security.S3DatabaseSecurityFacadeProvider;
import io.trino.aws.proxy.spi.security.S3SecurityFacadeProvider;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class TestingS3SecurityController
        extends S3SecurityController
{
    private final AtomicReference<Optional<S3SecurityFacadeProvider>> delegate = new AtomicReference<>(Optional.empty());

    @Inject
    public TestingS3SecurityController(Optional<S3SecurityFacadeProvider> s3SecurityFacadeProvider, Optional<S3DatabaseSecurityFacadeProvider> s3DatabaseSecurityFacadeProvider, RequestLoggerController requestLoggerController)
    {
        super(s3SecurityFacadeProvider, s3DatabaseSecurityFacadeProvider, requestLoggerController);
    }

    @Override
    protected S3SecurityFacadeProvider currentProvider()
    {
        return delegate.get().orElseGet(super::currentProvider);
    }

    public void setDelegate(S3SecurityFacadeProvider delegate)
    {
        this.delegate.set(Optional.of(delegate));
    }

    public void clear()
    {
        delegate.set(Optional.empty());
    }
}
