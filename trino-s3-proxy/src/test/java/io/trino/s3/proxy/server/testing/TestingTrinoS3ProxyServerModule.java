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
package io.trino.s3.proxy.server.testing;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.trino.s3.proxy.server.TrinoS3ProxyServerModule;
import io.trino.s3.proxy.server.credentials.CredentialsController;

public class TestingTrinoS3ProxyServerModule
        extends TrinoS3ProxyServerModule
{
    @Override
    protected void moduleSpecificBinding(Binder binder)
    {
        binder.bind(CredentialsController.class).to(TestingCredentialsController.class).in(Scopes.SINGLETON);
        binder.bind(TestingCredentialsController.class).in(Scopes.SINGLETON);
    }
}
