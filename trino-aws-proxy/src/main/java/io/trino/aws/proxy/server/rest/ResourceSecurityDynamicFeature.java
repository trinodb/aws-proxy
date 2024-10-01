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
package io.trino.aws.proxy.server.rest;

import com.google.inject.Inject;
import io.trino.aws.proxy.server.rest.ResourceSecurity.AccessType;
import io.trino.aws.proxy.server.rest.ResourceSecurity.AccessType.Access.PublicAccess;
import io.trino.aws.proxy.server.rest.ResourceSecurity.AccessType.Access.SigV4Access;
import io.trino.aws.proxy.spi.signing.SigningController;
import io.trino.aws.proxy.spi.signing.SigningServiceType;
import jakarta.ws.rs.container.DynamicFeature;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.FeatureContext;

import java.lang.reflect.AnnotatedElement;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ResourceSecurityDynamicFeature
        implements DynamicFeature
{
    private final SigningController signingController;
    private final RequestLoggerController requestLoggerController;

    @Inject
    public ResourceSecurityDynamicFeature(SigningController signingController, RequestLoggerController requestLoggerController)
    {
        this.signingController = requireNonNull(signingController);
        this.requestLoggerController = requireNonNull(requestLoggerController);
    }

    @Override
    public void configure(ResourceInfo resourceInfo, FeatureContext context)
    {
        if (resourceInfo.getResourceClass().getPackageName().startsWith("io.trino.aws")) {
            AccessType accessType = getAccessType(resourceInfo);
            switch (accessType.access()) {
                case PublicAccess _ -> {}
                case SigV4Access(SigningServiceType signingServiceType) ->
                        context.register(new SecurityFilter(signingController, signingServiceType, requestLoggerController));
            }
        }
    }

    private static AccessType getAccessType(ResourceInfo resourceInfo)
    {
        return getAccessTypeFromAnnotation(resourceInfo.getResourceMethod())
                .or(() -> getAccessTypeFromAnnotation(resourceInfo.getResourceClass()))
                .orElseThrow(() -> new IllegalArgumentException("Proxy resource is not annotated with @" + ResourceSecurity.class.getSimpleName() + ": " + resourceInfo.getResourceMethod()));
    }

    private static Optional<AccessType> getAccessTypeFromAnnotation(AnnotatedElement annotatedElement)
    {
        return Optional.ofNullable(annotatedElement.getAnnotation(ResourceSecurity.class))
                .map(ResourceSecurity::value);
    }
}
