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
package io.trino.aws.proxy.server;

import com.google.inject.Inject;
import com.google.inject.Scopes;
import io.trino.aws.proxy.server.testing.TestingTrinoAwsProxyServer;
import io.trino.aws.proxy.server.testing.containers.PySparkContainer;
import io.trino.aws.proxy.server.testing.harness.BuilderFilter;
import io.trino.aws.proxy.server.testing.harness.TrinoAwsProxyTest;
import io.trino.aws.proxy.spi.credentials.Identity;
import io.trino.aws.proxy.spi.rest.ParsedS3Request;
import io.trino.aws.proxy.spi.security.S3DatabaseSecurityDecorator;
import io.trino.aws.proxy.spi.security.S3SecurityFacade;
import io.trino.aws.proxy.spi.security.S3SecurityFacadeProvider;
import io.trino.aws.proxy.spi.security.SecurityResponse;
import jakarta.ws.rs.WebApplicationException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.aws.proxy.server.TestPySparkSql.createDatabaseAndTable;
import static io.trino.aws.proxy.server.testing.containers.DockerAttachUtil.clearInputStreamAndClose;
import static io.trino.aws.proxy.server.testing.containers.DockerAttachUtil.inputToContainerStdin;
import static io.trino.aws.proxy.spi.plugin.TrinoAwsProxyServerBinding.s3SecurityFacadeProviderModule;
import static io.trino.aws.proxy.spi.security.SecurityResponse.FAILURE;
import static io.trino.aws.proxy.spi.security.SecurityResponse.SUCCESS;
import static java.util.Objects.requireNonNull;

@TrinoAwsProxyTest(filters = TestDatabaseSecurity.Filter.class)
public class TestDatabaseSecurity
{
    public static final String DATABASE_NAME = "db";
    public static final String TABLE_NAME = "people";

    public static class Filter
            implements BuilderFilter
    {
        @Override
        public TestingTrinoAwsProxyServer.Builder filter(TestingTrinoAwsProxyServer.Builder builder)
        {
            return builder.addModule(s3SecurityFacadeProviderModule("db", FacadeProvider.class, binder -> binder.bind(FacadeProvider.class).in(Scopes.SINGLETON)))
                    .withProperty("s3-security.type", "db")
                    .withV3PySparkContainer();
        }
    }

    public static class FacadeProvider
            implements S3DatabaseSecurityDecorator, S3SecurityFacadeProvider
    {
        final AtomicBoolean disallowGets = new AtomicBoolean();

        @Override
        public S3SecurityFacade securityFacadeForRequest(ParsedS3Request request, Optional<Identity> identity)
                throws WebApplicationException
        {
            S3SecurityFacade s3SecurityFacade = lowercaseAction -> SUCCESS;
            return S3DatabaseSecurityDecorator.decorate(request, s3SecurityFacade, this);
        }

        @Override
        public Optional<String> tableName(ParsedS3Request request, Optional<String> lowercaseAction)
        {
            return Optional.of(TABLE_NAME);
        }

        @Override
        public SecurityResponse tableOperation(ParsedS3Request request, String tableName, Optional<String> lowercaseAction)
        {
            if (disallowGets.get() && request.httpVerb().equalsIgnoreCase("GET")) {
                return FAILURE;
            }
            return SUCCESS;
        }
    }

    private final S3Client s3Client;
    private final PySparkContainer pySparkContainer;
    private final FacadeProvider facadeProvider;

    @Inject
    public TestDatabaseSecurity(S3Client s3Client, PySparkContainer pySparkContainer, FacadeProvider facadeProvider)
    {
        this.s3Client = requireNonNull(s3Client, "s3Client is null");
        this.pySparkContainer = requireNonNull(pySparkContainer, "pySparkContainer is null");
        this.facadeProvider = requireNonNull(facadeProvider, "facadeProvider is null");
    }

    @Test
    public void testDatabaseSecurity()
            throws Exception
    {
        // create the test bucket
        s3Client.createBucket(r -> r.bucket("test"));

        createDatabaseAndTable(s3Client, pySparkContainer);

        clearInputStreamAndClose(inputToContainerStdin(pySparkContainer.containerId(), "spark.sql(\"select * from %s.%s\").show()".formatted(DATABASE_NAME, TABLE_NAME)), line -> line.equals("|    John Galt| 28|"));

        try {
            facadeProvider.disallowGets.set(true);
            clearInputStreamAndClose(inputToContainerStdin(pySparkContainer.containerId(), "spark.sql(\"select * from %s.%s\").show()".formatted(DATABASE_NAME, TABLE_NAME)), line -> line.contains("Status Code: 401; Error Code: Unauthorized"));
        }
        finally {
            facadeProvider.disallowGets.set(false);
        }
    }
}
