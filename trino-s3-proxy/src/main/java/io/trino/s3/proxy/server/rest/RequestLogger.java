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
package io.trino.s3.proxy.server.rest;

import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.s3.proxy.spi.signing.SigningServiceType;
import jakarta.ws.rs.WebApplicationException;
import org.glassfish.jersey.server.ContainerRequest;

import java.net.URI;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class RequestLogger
{
    private static final Logger log = Logger.get(RequestLogger.class);

    private static final LoggerProc debugLogger = new LoggerProc()
    {
        @Override
        public void log(String format, Object... args)
        {
            log.debug(format, args);
        }

        @Override
        public boolean isEnabled()
        {
            return log.isDebugEnabled();
        }
    };

    private static final LoggerProc infoLogger = new LoggerProc()
    {
        @Override
        public void log(String format, Object... args)
        {
            log.info(format, args);
        }

        @Override
        public boolean isEnabled()
        {
            return log.isInfoEnabled();
        }
    };

    private volatile LoggerProc loggerProc = debugLogger;

    private interface LoggerProc
    {
        void log(String format, Object... args);

        boolean isEnabled();
    }

    // TODO - allow levels to be set for only certain users, IPs, etc.

    public void setLevelInfo()
    {
        loggerProc = infoLogger;
    }

    public void setLevelDebug()
    {
        loggerProc = debugLogger;
    }

    public RequestLoggingSession requestSession(SigningServiceType serviceType, ContainerRequest request)
    {
        if (!loggerProc.isEnabled()) {
            return () -> {};
        }

        // TODO introduce telemetry/spans
        UUID requestId = UUID.randomUUID();

        Map<String, String> entries = new ConcurrentHashMap<>();

        Map<String, String> properties = new ConcurrentHashMap<>();

        Map<String, String> errors = new ConcurrentHashMap<>();

        String requestMethod = request.getMethod();
        URI requestUri = request.getRequestUri();
        boolean requestHasEntity = request.hasEntity();

        add(entries, "request.id", requestId);
        add(entries, "request.type", serviceType);
        add(entries, "request.http.method", requestMethod);
        add(entries, "request.http.uri", requestUri);
        add(entries, "request.http.entity", requestHasEntity);
        push("RequestStart", entries);

        return new RequestLoggingSession()
        {
            private final Stopwatch stopwatch = Stopwatch.createStarted();
            private volatile boolean closed;

            @Override
            public void logProperty(String name, Object value)
            {
                properties.put(name, String.valueOf(value));
            }

            @Override
            public void logError(String name, Object value)
            {
                errors.put(name, String.valueOf(value));
            }

            @SuppressWarnings({"ThrowableNotThrown", "SwitchStatementWithTooFewBranches"})
            @Override
            public void logException(Throwable e)
            {
                switch (Throwables.getRootCause(e)) {
                    case WebApplicationException webApplicationException -> {
                        errors.put("webException.status", Integer.toString(webApplicationException.getResponse().getStatus()));
                        errors.put("webException.message", webApplicationException.getMessage());
                    }

                    default -> {
                        errors.put("exception.type", e.getClass().getName());
                        errors.put("exception.message", e.getMessage());
                    }
                }
            }

            @Override
            public void close()
            {
                if (closed) {
                    return;
                }
                closed = true;

                add(entries, "request.id", requestId);
                add(entries, "request.type", serviceType);
                add(entries, "request.http.method", requestMethod);
                add(entries, "request.http.uri", requestUri);
                add(entries, "request.http.entity", requestHasEntity);
                add(entries, "request.elapsed.ms", stopwatch.elapsed().toMillis());
                add(entries, "request.properties", properties);
                add(entries, "request.errors", errors);
                push("RequestEnd", entries);
            }
        };
    }

    private void add(Map<String, String> entries, String key, Object value)
    {
        entries.put(key, String.valueOf(value));
    }

    private void push(String message, Map<String, String> entries)
    {
        // TODO - keep a stck of recent entries, etc.

        Map<String, String> copy = ImmutableMap.copyOf(entries);
        entries.clear();

        loggerProc.log("%s: %s", message, copy);
    }
}
