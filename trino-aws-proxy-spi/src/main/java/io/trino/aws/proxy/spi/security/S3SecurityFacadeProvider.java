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
package io.trino.aws.proxy.spi.security;

import io.trino.aws.proxy.spi.credentials.Identity;
import io.trino.aws.proxy.spi.rest.ParsedS3Request;
import jakarta.ws.rs.WebApplicationException;

import java.util.Optional;

public interface S3SecurityFacadeProvider
{
    S3SecurityFacadeProvider NOOP = (_, _) -> S3SecurityFacade.NOOP;

    /**
     * Return a validated/authenticated facade for the given request or
     * throw {@link jakarta.ws.rs.WebApplicationException}
     */
    S3SecurityFacade securityFacadeForRequest(ParsedS3Request request, Optional<Identity> identity)
            throws WebApplicationException;
}
