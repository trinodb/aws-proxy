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
package io.trino.s3.proxy.hms;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.NotBlank;

import java.util.concurrent.TimeUnit;

public class HmsConfig
{
    private String jdbcUrl;
    private String jdbcUsername;
    private String jdbcPassword;

    private Duration cacheRefreshPeriod = new Duration(15, TimeUnit.MINUTES);

    private String tableNameColumn = "TBL_NAME";
    private String locationNameColumn = "LOCATION";

    private String tablesQuery = """
            SELECT t."TBL_NAME", s."LOCATION"
            FROM "TBLS" t INNER JOIN "SDS" s ON t."SD_ID" = s."SD_ID"
            """;

    @NotBlank
    public String getJdbcUrl()
    {
        return jdbcUrl;
    }

    @Config("hms.jdbc.url")
    public HmsConfig setJdbcUrl(String jdbcUrl)
    {
        this.jdbcUrl = jdbcUrl;
        return this;
    }

    @NotBlank
    public String getJdbcUsername()
    {
        return jdbcUsername;
    }

    @Config("hms.jdbc.username")
    public HmsConfig setJdbcUsername(String jdbcUsername)
    {
        this.jdbcUsername = jdbcUsername;
        return this;
    }

    @NotBlank
    public String getJdbcPassword()
    {
        return jdbcPassword;
    }

    @Config("hms.jdbc.password")
    public HmsConfig setJdbcPassword(String jdbcPassword)
    {
        this.jdbcPassword = jdbcPassword;
        return this;
    }

    @NotBlank
    public String getTablesQuery()
    {
        return tablesQuery;
    }

    @Config("hms.jdbc.query.tables")
    public HmsConfig setTablesQuery(String tablesQuery)
    {
        this.tablesQuery = tablesQuery;
        return this;
    }

    @NotBlank
    public String getTableNameColumn()
    {
        return tableNameColumn;
    }

    @Config("hms.jdbc.query.column.table-name")
    public HmsConfig setTableNameColumn(String tableNameColumn)
    {
        this.tableNameColumn = tableNameColumn;
        return this;
    }

    @NotBlank
    public String getLocationNameColumn()
    {
        return locationNameColumn;
    }

    @Config("hms.jdbc.query.column.location-name")
    public HmsConfig setLocationNameColumn(String locationNameColumn)
    {
        this.locationNameColumn = locationNameColumn;
        return this;
    }

    @MinDuration("0s")
    public Duration getCacheRefreshPeriod()
    {
        return cacheRefreshPeriod;
    }

    @Config("hms.cache.refresh-period")
    public HmsConfig setCacheRefreshPeriod(Duration cacheRefreshPeriod)
    {
        this.cacheRefreshPeriod = cacheRefreshPeriod;
        return this;
    }
}
