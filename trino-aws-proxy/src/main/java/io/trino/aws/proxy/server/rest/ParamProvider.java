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

import io.trino.aws.proxy.spi.rest.Request;
import io.trino.aws.proxy.spi.signing.SigningMetadata;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.model.Parameter;
import org.glassfish.jersey.server.spi.internal.ValueParamProvider;

import java.util.function.Function;

import static io.trino.aws.proxy.server.rest.SecurityFilter.unwrap;
import static org.glassfish.jersey.server.spi.internal.ValueParamProvider.Priority.HIGH;

public class ParamProvider
        implements ValueParamProvider
{
    @Override
    public Function<ContainerRequest, ?> getValueProvider(Parameter parameter)
    {
        if (Request.class.isAssignableFrom(parameter.getRawType()) || SigningMetadata.class.isAssignableFrom(parameter.getRawType()) || RequestLoggingSession.class.isAssignableFrom(parameter.getRawType())) {
            return containerRequest -> unwrap(containerRequest, parameter.getRawType());
        }
        return null;
    }

    @Override
    public PriorityType getPriority()
    {
        return HIGH;
    }
}
