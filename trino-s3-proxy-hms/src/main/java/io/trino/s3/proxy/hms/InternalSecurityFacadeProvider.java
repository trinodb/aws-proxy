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
package io.trino.s3.proxy.hms;

import com.google.common.base.Splitter;
import com.google.inject.Inject;
import io.trino.s3.proxy.spi.credentials.Credentials;
import io.trino.s3.proxy.spi.hms.HmsSecurityFacade;
import io.trino.s3.proxy.spi.hms.HmsSecurityFacadeProvider;
import io.trino.s3.proxy.spi.rest.ParsedS3Request;
import io.trino.s3.proxy.spi.security.SecurityFacade;
import io.trino.s3.proxy.spi.security.SecurityFacadeProvider;
import io.trino.s3.proxy.spi.security.SecurityResponse;
import jakarta.ws.rs.WebApplicationException;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static io.trino.s3.proxy.spi.security.SecurityFacade.DELETE_ACTION;
import static io.trino.s3.proxy.spi.security.SecurityFacade.UPLOADS_ACTION;
import static java.util.Objects.requireNonNull;

public class InternalSecurityFacadeProvider
        implements SecurityFacadeProvider
{
    private final TablesController tablesController;
    private final HmsSecurityFacadeProvider securityFacadeProvider;

    @Inject
    public InternalSecurityFacadeProvider(TablesController tablesController, HmsSecurityFacadeProvider securityFacadeProvider)
    {
        this.tablesController = requireNonNull(tablesController, "tablesController is null");
        this.securityFacadeProvider = requireNonNull(securityFacadeProvider, "securityFacadeProvider is null");
    }

    @Override
    public SecurityFacade securityFacadeForRequest(ParsedS3Request request, Credentials credentials, Optional<String> session)
            throws WebApplicationException
    {
        HmsSecurityFacade hmsSecurityFacade = securityFacadeProvider.securityFacadeForRequest(request, credentials, session);
        return (lowercaseAction, region) -> applyToRequest(hmsSecurityFacade, request, region, lowercaseAction);
    }

    private SecurityResponse applyToRequest(HmsSecurityFacade hmsSecurityFacade, ParsedS3Request request, String region, Optional<String> lowercaseAction)
    {
        Optional<String> maybeKey = request.keyInBucket().isBlank() ? Optional.empty() : Optional.of(request.keyInBucket());
        return maybeKey.map(key -> objectRequest(hmsSecurityFacade, request, region, key, lowercaseAction)).orElseGet(() -> bucketRequest(hmsSecurityFacade, request, region));
    }

    private SecurityResponse objectRequest(HmsSecurityFacade hmsSecurityFacade, ParsedS3Request request, String region, String key, Optional<String> lowercaseAction)
    {
        if (!lowercaseAction.map(action -> action.equals(DELETE_ACTION) || action.equals(UPLOADS_ACTION)).orElse(true)) {
            return new SecurityResponse(false, Optional.empty());
        }

        return switch (request.httpVerb().toUpperCase(Locale.ROOT)) {
            case "PUT" -> hmsSecurityFacade.canUploadObject(region, request.bucketName(), key);
            case "DELETE" -> hmsSecurityFacade.canDeleteObject(region, request.bucketName(), key);
            case "HEAD" -> hmsSecurityFacade.canGetTableMetadata(region, request.bucketName(), findTableName(request.bucketName(), key));
            case "GET" -> hmsSecurityFacade.canQueryTable(region, request.bucketName(), findTableName(request.bucketName(), key));
            default -> new SecurityResponse(false, Optional.empty());
        };
    }

    private SecurityResponse bucketRequest(HmsSecurityFacade hmsSecurityFacade, ParsedS3Request request, String region)
    {
        return switch (request.httpVerb().toUpperCase(Locale.ROOT)) {
            case "PUT" -> hmsSecurityFacade.canCreateDatabase(region, request.bucketName());
            case "DELETE" -> hmsSecurityFacade.canDropDatabase(region, request.bucketName());
            case "GET" -> hmsSecurityFacade.canGetDatabaseMetadata(region, request.bucketName());
            default -> new SecurityResponse(false, Optional.empty());
        };
    }

    private String findTableName(String bucketName, String key)
    {
        // lots TODO here

        List<String> keyParts = Splitter.on("/").splitToList(key);
        if (!keyParts.isEmpty()) {
            if (keyParts.getLast().contains(".")) {
                // remove the file part so that we only have the path to the file
                key = String.join("/", keyParts.subList(0, keyParts.size() - 1));
            }
        }

        Map<String, String> locationToTables = tablesController.currentTables().get(bucketName);
        if (locationToTables != null) {
            return locationToTables.get(key);
        }
        // TODO
        return "TODO";
    }
}
