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
package io.trino.s3.proxy.spi.hms;

import io.trino.s3.proxy.spi.security.SecurityResponse;

public interface HmsSecurityFacade
{
    HmsSecurityFacade DEFAULT = new HmsSecurityFacade() {};

    default SecurityResponse canCreateDatabase(String region, String databaseName)
    {
        return SecurityResponse.DEFAULT;
    }

    default SecurityResponse canDropDatabase(String region, String databaseName)
    {
        return SecurityResponse.DEFAULT;
    }

    default SecurityResponse canGetDatabaseMetadata(String region, String databaseName)
    {
        return SecurityResponse.DEFAULT;
    }

    default SecurityResponse canUploadObject(String region, String databaseName, String objectName)
    {
        return SecurityResponse.DEFAULT;
    }

    default SecurityResponse canDeleteObject(String region, String databaseName, String objectName)
    {
        return SecurityResponse.DEFAULT;
    }

    default SecurityResponse canGetTableMetadata(String region, String databaseName, String tableName)
    {
        return SecurityResponse.DEFAULT;
    }

    default SecurityResponse canQueryTable(String region, String databaseName, String tableName)
    {
        return SecurityResponse.DEFAULT;
    }
}
