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
package io.trino.aws.proxy.server.credentials;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.aws.proxy.spi.credentials.Identity;

public class JsonIdentityProvider
        implements Provider<Module>
{
    private final Class<? extends Identity> identityType;

    @Inject
    public JsonIdentityProvider(Class<? extends Identity> identityType)
    {
        this.identityType = identityType;
    }

    @Override
    public Module get()
    {
        return new SimpleModule().addAbstractTypeMapping(Identity.class, identityType);
    }
}
