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
package io.trino.aws.proxy.server.remote;

import com.google.common.base.Strings;

public interface RemoteS3HostBuilder
{
    static String getHostName(String bucket, String region, String domain, String hostnameTemplate)
    {
        String hostname = hostnameTemplate
                .replace("${bucket}", bucket)
                .replace("${region}", region)
                .replace("${domain}", domain);

        // Handles usecases when the bucket is empty (Eg: List buckets request)
        if (Strings.isNullOrEmpty(bucket)) {
            if (hostname.startsWith(".")) {         /* Handles cases when ${bucket} is leading. Eg: "${bucket}.{}.{}" */
                hostname = hostname.substring(1);
            }
            else if (hostname.contains("..")) {     /* Handles cases when ${bucket} is in the midst. Eg: "{}.${bucket}.{}" */
                hostname = hostname.replace("..", ".");
            }
        }
        return hostname;
    }

    String build(String bucket, String region);
}
