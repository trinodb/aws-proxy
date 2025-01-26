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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.Min;

public class RequestLoggerConfig
{
    private int requestLoggerSavedQty = 10000;

    @Min(0)
    public int getRequestLoggerSavedQty()
    {
        return requestLoggerSavedQty;
    }

    @Config("aws.proxy.request.logger.saved-qty")
    @ConfigDescription("Number of log entries to store")
    public RequestLoggerConfig setRequestLoggerSavedQty(int requestLoggerSavedQty)
    {
        this.requestLoggerSavedQty = requestLoggerSavedQty;
        return this;
    }
}
