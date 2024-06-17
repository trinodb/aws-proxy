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
package io.trino.s3.proxy.server.signing;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import java.util.concurrent.TimeUnit;

public class SigningControllerConfig
{
    private Duration maxClockDrift = new Duration(15, TimeUnit.MINUTES);

    @MinDuration("0s")
    public Duration getMaxClockDrift()
    {
        return maxClockDrift;
    }

    @Config("signing-controller.clock.max-drift")
    public SigningControllerConfig setMaxClockDrift(Duration maxClockDrift)
    {
        this.maxClockDrift = maxClockDrift;
        return this;
    }
}
