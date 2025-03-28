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
package io.trino.aws.proxy.glue.handler;

import io.trino.aws.proxy.glue.rest.ParsedGlueRequest;
import io.trino.aws.proxy.server.rest.RequestLoggingSession;
import io.trino.aws.proxy.spi.signing.SigningMetadata;
import software.amazon.awssdk.services.glue.model.GlueResponse;

public interface GlueRequestHandler
{
    GlueResponse handleRequest(ParsedGlueRequest request, SigningMetadata signingMetadata, RequestLoggingSession requestLoggingSession);
}
