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
package io.trino.s3.proxy.server.testing.containers;

import org.testcontainers.Testcontainers;

public class TestContainerUtil
{
    private TestContainerUtil() {}

    public static void exposeHostPort(int hostPort)
    {
        Testcontainers.exposeHostPorts(hostPort);
    }

    public static String asHostUrl(String localhostUrl)
    {
        return localhostUrl.replace("localhost:", "host.testcontainers.internal:")
                .replace("127.0.0.1:", "host.testcontainers.internal:");
    }
}
