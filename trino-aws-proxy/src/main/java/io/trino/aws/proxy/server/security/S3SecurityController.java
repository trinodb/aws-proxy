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
package io.trino.aws.proxy.server.security;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.inject.Inject;
import io.trino.aws.proxy.server.rest.RequestLoggerController;
import io.trino.aws.proxy.spi.rest.ParsedS3Request;
import io.trino.aws.proxy.spi.security.S3DatabaseSecurityFacadeProvider;
import io.trino.aws.proxy.spi.security.S3SecurityFacade;
import io.trino.aws.proxy.spi.security.S3SecurityFacadeProvider;
import io.trino.aws.proxy.spi.security.SecurityResponse;

import java.util.Locale;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class S3SecurityController
{
    private static final S3SecurityFacadeProvider DEFAULT_SECURITY_FACADE_PROVIDER = _ -> _ -> SecurityResponse.DEFAULT;

    private final S3SecurityFacadeProvider s3SecurityFacadeProvider;

    @Inject
    public S3SecurityController(Optional<S3SecurityFacadeProvider> s3SecurityFacadeProvider, Optional<S3DatabaseSecurityFacadeProvider> s3DatabaseSecurityFacadeProvider, RequestLoggerController requestLoggerController)
    {
        boolean hasDatabaseSecurity = s3DatabaseSecurityFacadeProvider.isPresent();
        boolean hasNonDefaultSecurity = s3SecurityFacadeProvider.isPresent();

        checkArgument(!hasDatabaseSecurity || !hasNonDefaultSecurity, "Both database security and non-default security have been bound. This is not supported.");

        this.s3SecurityFacadeProvider = requireNonNull(s3SecurityFacadeProvider, "securityFacadeProvider is null")
                .orElseGet(() -> s3DatabaseSecurityFacadeProvider.map(databaseSecurity -> (S3SecurityFacadeProvider) new S3DatabaseSecurityController(databaseSecurity, requestLoggerController))
                        .orElse(DEFAULT_SECURITY_FACADE_PROVIDER));
    }

    public SecurityResponse apply(ParsedS3Request request)
    {
        S3SecurityFacade s3SecurityFacade = currentProvider().securityFacadeForRequest(request);

        Optional<String> lowercaseAction = request.rawQuery().flatMap(S3SecurityController::parseAction);

        return s3SecurityFacade.apply(lowercaseAction);
    }

    @VisibleForTesting
    protected S3SecurityFacadeProvider currentProvider()
    {
        return s3SecurityFacadeProvider;
    }

    private static Optional<String> parseAction(String rawQuery)
    {
        if (rawQuery.isBlank()) {
            return Optional.empty();
        }

        String possibleAction = Splitter.on('&').limit(1).splitToList(rawQuery).getFirst();
        boolean isAction = !possibleAction.contains("=");
        return isAction ? Optional.of(possibleAction.toLowerCase(Locale.ROOT)) : Optional.empty();
    }
}
