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

import com.google.inject.Inject;
import com.sun.management.OperatingSystemMXBean;
import io.airlift.node.NodeInfo;
import io.trino.aws.proxy.server.rest.ResourceSecurity.Public;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Response;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

import static io.airlift.units.Duration.nanosSince;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;

@ResourceSecurity(Public.class)
public class TrinoStatusResource
{
    private final NodeInfo nodeInfo;
    private final long startTime = System.nanoTime();
    private final int logicalCores;
    private final MemoryMXBean memoryMXBean;

    private OperatingSystemMXBean operatingSystemMXBean;

    @Inject
    public TrinoStatusResource(NodeInfo nodeInfo)
    {
        this.nodeInfo = requireNonNull(nodeInfo, "nodeInfo is null");
        this.memoryMXBean = ManagementFactory.getMemoryMXBean();
        this.logicalCores = Runtime.getRuntime().availableProcessors();

        if (ManagementFactory.getOperatingSystemMXBean() instanceof OperatingSystemMXBean operatingSystemMXBean) {
            // we want the com.sun.management sub-interface of java.lang.management.OperatingSystemMXBean
            this.operatingSystemMXBean = operatingSystemMXBean;
        }
    }

    @HEAD
    @Produces(APPLICATION_JSON)
    public Response statusPing()
    {
        return Response.ok().build();
    }

    @GET
    @Produces(APPLICATION_JSON)
    public NodeStatus getStatus()
    {
        return new NodeStatus(
                nodeInfo.getNodeId(),
                nodeInfo.getEnvironment(),
                nanosSince(startTime),
                nodeInfo.getExternalAddress(),
                nodeInfo.getInternalAddress(),
                logicalCores,
                operatingSystemMXBean == null ? 0 : operatingSystemMXBean.getProcessCpuLoad(),
                operatingSystemMXBean == null ? 0 : operatingSystemMXBean.getCpuLoad(),
                memoryMXBean.getHeapMemoryUsage().getUsed(),
                memoryMXBean.getHeapMemoryUsage().getMax(),
                memoryMXBean.getNonHeapMemoryUsage().getUsed());
    }
}
