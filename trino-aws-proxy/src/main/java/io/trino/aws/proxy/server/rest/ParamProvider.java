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

import com.google.common.reflect.TypeToken;
import io.trino.aws.proxy.spi.credentials.Identity;
import io.trino.aws.proxy.spi.rest.Request;
import io.trino.aws.proxy.spi.signing.SigningMetadata;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.model.Parameter;
import org.glassfish.jersey.server.spi.internal.ValueParamProvider;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.aws.proxy.server.rest.SecurityFilter.unwrapType;
import static org.glassfish.jersey.server.spi.internal.ValueParamProvider.Priority.HIGH;

public class ParamProvider
        implements ValueParamProvider
{
    private static final List<TypeToken<?>> SUPPORTED_TYPES =
            Stream.of(Request.class, SigningMetadata.class, Identity.class, RequestLoggingSession.class).map(TypeToken::of).collect(toImmutableList());

    @Override
    public Function<ContainerRequest, ?> getValueProvider(Parameter parameter)
    {
        return getValueProvider(parameter.getType());
    }

    @Override
    public PriorityType getPriority()
    {
        return HIGH;
    }

    private Function<ContainerRequest, ?> getValueProvider(Type type)
    {
        if (type instanceof ParameterizedType parameterizedType) {
            if (parameterizedType.getRawType().equals(Optional.class)) {
                var innerValueProvider = getValueProvider(parameterizedType.getActualTypeArguments()[0]);
                if (innerValueProvider != null) {
                    return containerRequest -> Optional.ofNullable(innerValueProvider.apply(containerRequest));
                }
                return null;
            }
        }

        if (SUPPORTED_TYPES.stream().anyMatch(supportedType -> supportedType.isSubtypeOf(type))) {
            return containerRequest -> unwrapType(containerRequest, type);
        }
        return null;
    }
}
