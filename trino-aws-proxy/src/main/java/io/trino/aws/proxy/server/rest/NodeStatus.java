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

import io.airlift.units.Duration;

import static java.util.Objects.requireNonNull;

public record NodeStatus(
        String nodeId,
        String environment,
        Duration uptime,
        String externalAddress,
        String internalAddress,
        int processors,
        double processCpuLoad,
        double systemCpuLoad,
        long heapUsed,
        long heapAvailable,
        long nonHeapUsed)
{
    public NodeStatus
    {
        requireNonNull(nodeId, "nodeId is null");
        requireNonNull(environment, "environment is null");
        requireNonNull(uptime, "uptime is null");
        requireNonNull(externalAddress, "externalAddress is null");
        requireNonNull(internalAddress, "internalAddress is null");
    }
}
