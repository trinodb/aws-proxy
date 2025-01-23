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
package io.trino.aws.proxy.glue.rest;

import io.trino.aws.proxy.server.rest.ResourceSecurity.SigV4AccessType;
import io.trino.aws.proxy.spi.signing.SigningServiceType;

public final class GlueResourceSecurity
        implements SigV4AccessType
{
    private static final SigningServiceType GLUE = new SigningServiceType("glue");

    @Override
    public SigningServiceType signingServiceType()
    {
        return GLUE;
    }
}
