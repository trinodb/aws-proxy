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
package io.trino.aws.proxy.glue;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

public class TrinoGlueConfig
{
    private String gluePath = "/api/v1/glue";

    @NotNull
    public String getGluePath()
    {
        return gluePath;
    }

    @Config("aws.proxy.glue.path")
    @ConfigDescription("URL Path for Glue operations, optional")
    public TrinoGlueConfig setGluePath(String gluePath)
    {
        this.gluePath = gluePath;
        return this;
    }
}
