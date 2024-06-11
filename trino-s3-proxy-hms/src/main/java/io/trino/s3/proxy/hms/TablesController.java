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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import jakarta.annotation.PreDestroy;

import java.net.URI;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;

public class TablesController
{
    private static final Logger log = Logger.get(TablesController.class);

    private final Object key = new Object();
    private final Callable<Connection> connectionSupplier;
    private final String tablesQuery;
    private final String tableNameColumn;
    private final String locationNameColumn;
    private final Map<Object, Map<String, Map<String, String>>> cache = new ConcurrentHashMap<>();
    private final ExecutorService executorService;
    private final Duration cacheRefreshPeriod;

    @Inject
    public TablesController(HmsConfig config)
    {
        Driver driver;
        try {
            driver = DriverManager.getDriver(config.getJdbcUrl());
        }
        catch (SQLException e) {
            throw new RuntimeException("Could not load JDBC driver for url: " + config.getJdbcUrl(), e);
        }

        Properties properties = new Properties();
        properties.put("user", config.getJdbcUsername());
        properties.put("password", config.getJdbcPassword());
        connectionSupplier = () -> driver.connect(config.getJdbcUrl(), properties);

        tablesQuery = config.getTablesQuery();
        tableNameColumn = config.getTableNameColumn();
        locationNameColumn = config.getLocationNameColumn();

        executorService = Executors.newVirtualThreadPerTaskExecutor();
        cacheRefreshPeriod = config.getCacheRefreshPeriod().toJavaTime();
    }

    // must be manually started
    public void start()
    {
        if (!cacheRefreshPeriod.isZero()) {
            executorService.submit(this::refreshTask);
        }
    }

    @PreDestroy
    public void shutdown()
    {
        if (!shutdownAndAwaitTermination(executorService, 10, TimeUnit.SECONDS)) {
            log.error("Could not terminate refreshTask");
        }
    }

    /**
     * {@code Map<DatabaseName, Map<Location, TableName>>}
     */
    public Map<String, Map<String, String>> currentTables()
    {
        Map<String, Map<String, String>> current = cache.computeIfAbsent(key, _ -> query().orElse(null));
        return firstNonNull(current, ImmutableMap.of());
    }

    private Optional<Map<String, Map<String, String>>> query()
    {
        Map<String, Map<String, String>> builders = new HashMap<>();

        try (Connection connection = connectionSupplier.call()) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(tablesQuery)) {
                    while (resultSet.next()) {
                        String tableName = firstNonNull(resultSet.getString(tableNameColumn), "");
                        String location = firstNonNull(resultSet.getString(locationNameColumn), "");

                        URI uri = URI.create(location);
                        // TODO - check scheme to make sure it's s3:// or s3a://
                        String databaseName = uri.getHost();
                        String adjustedPath = uri.getRawPath();
                        if (adjustedPath.startsWith("/")) {
                            adjustedPath = adjustedPath.substring(1);
                        }

                        builders.computeIfAbsent(databaseName, _ -> new HashMap<>()).put(adjustedPath, tableName);
                    }
                }
            }
        }
        catch (Exception e) {
            log.error(e, "Could not query for tables");
            return Optional.empty();
        }

        return Optional.of(builders.entrySet().stream().collect(toImmutableMap(Map.Entry::getKey, entry -> ImmutableMap.copyOf(entry.getValue()))));
    }

    @VisibleForTesting
    public void refresh()
    {
        try {
            log.info("refreshTask refreshing tables");

            query().ifPresentOrElse(tables -> cache.put(key, tables),
                    () -> log.warn("refreshTask refreshing tables failed"));

            log.info("refreshTask refreshing tables completed");
        }
        catch (Throwable e) {
            log.error(e, "refreshTask error");
        }
    }

    private void refreshTask()
    {
        log.info("refreshTask started");

        try {
            while (!Thread.currentThread().isInterrupted()) {
                refresh();

                try {
                    Thread.sleep(cacheRefreshPeriod);
                }
                catch (InterruptedException _) {
                    log.info("refreshTask interrupted");
                    break;
                }
            }
        }
        finally {
            log.info("refreshTask exiting");
        }
    }
}
