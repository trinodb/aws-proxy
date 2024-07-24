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

import io.airlift.log.Logger;
import io.trino.aws.proxy.server.rest.RequestLoggerController;
import io.trino.aws.proxy.server.rest.RequestLoggingSession;
import io.trino.aws.proxy.spi.rest.ParsedS3Request;
import io.trino.aws.proxy.spi.security.S3DatabaseSecurityFacade;
import io.trino.aws.proxy.spi.security.S3DatabaseSecurityFacadeProvider;
import io.trino.aws.proxy.spi.security.S3SecurityFacade;
import io.trino.aws.proxy.spi.security.S3SecurityFacadeProvider;
import io.trino.aws.proxy.spi.security.SecurityResponse;
import io.trino.aws.proxy.spi.security.SecurityResponse.Failure;
import io.trino.aws.proxy.spi.security.SecurityResponse.Success;
import jakarta.ws.rs.WebApplicationException;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

class S3DatabaseSecurityController
        implements S3SecurityFacadeProvider
{
    private static final Logger log = Logger.get(S3DatabaseSecurityController.class);

    private final S3DatabaseSecurityFacadeProvider facadeProvider;
    private final RequestLoggerController requestLoggerController;

    S3DatabaseSecurityController(S3DatabaseSecurityFacadeProvider facadeProvider, RequestLoggerController requestLoggerController)
    {
        this.facadeProvider = requireNonNull(facadeProvider, "facadeProvider is null");
        this.requestLoggerController = requireNonNull(requestLoggerController, "requestLoggerController is null");
    }

    @Override
    public S3SecurityFacade securityFacadeForRequest(ParsedS3Request request)
            throws WebApplicationException
    {
        S3DatabaseSecurityFacade s3DatabaseSecurityFacade = facadeProvider.securityFacadeForRequest(request);
        return lowercaseAction -> applyToRequest(s3DatabaseSecurityFacade, request, lowercaseAction);
    }

    @SuppressWarnings("resource")
    private SecurityResponse applyToRequest(S3DatabaseSecurityFacade securityFacade, ParsedS3Request request, Optional<String> lowercaseAction)
    {
        RequestLoggingSession requestLoggingSession = requestLoggerController.currentRequestSession(request.requestId());

        requestLoggingSession.logProperty("request.database.bucket", request.bucketName());
        requestLoggingSession.logProperty("request.database.key", request.keyInBucket());
        requestLoggingSession.logProperty("request.database.action", lowercaseAction);

        return securityFacade.tableName(lowercaseAction)
                .map(tableName -> {
                    SecurityResponse securityResponse = securityFacade.tableOperation(tableName, lowercaseAction);

                    requestLoggingSession.logProperty("response.database.table-operation.table-name", tableName);
                    addLogging(requestLoggingSession, "response.database.table-operation.", securityResponse);

                    return securityResponse;
                })
                .orElseGet(() -> {
                    SecurityResponse securityResponse = securityFacade.nonTableOperation(lowercaseAction);

                    addLogging(requestLoggingSession, "response.database.non-table-operation.", securityResponse);

                    return securityResponse;
                });
    }

    private void addLogging(RequestLoggingSession requestLoggingSession, String prefix, SecurityResponse securityResponse)
    {
        switch (securityResponse) {
            case Success _ -> addLogging(requestLoggingSession, prefix, true, Optional.empty());
            case Failure(var error) -> addLogging(requestLoggingSession, prefix, false, error);
        }
    }

    private void addLogging(RequestLoggingSession requestLoggingSession, String prefix, boolean success, Optional<String> error)
    {
        requestLoggingSession.logProperty(prefix + "success", success);
        requestLoggingSession.logProperty(prefix + "error", error);
    }
}
