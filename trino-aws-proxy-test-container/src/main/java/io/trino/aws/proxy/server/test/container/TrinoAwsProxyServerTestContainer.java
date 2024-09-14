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
package io.trino.aws.proxy.server.test.container;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.ByteStreams.exhaust;
import static java.util.Objects.requireNonNull;

/**
 * Notes:
 * <p>
 * I couldn't find a base image with both Java 22 and Python 3 so I use Temurin and install Python in a script.
 * TODO build a base image that does this.
 * </p>
 * <p>
 * This container relies on the Maven build to work properly. See the {@code maven-dependency-plugin} of this module's
 * POM file. It copies the launcher tarball from the main module (trino-aws-proxy). The launcher
 * tarball is bundled into this module's JAR (i.e. it's copied to target/classes) as we need it at runtime.
 * The constructor of {@code TrinoAwsProxyServerTestContainer} copies the tarball into the Temurin container
 * via {@code withCopyToContainer()}, {@code startScript} (which is also added to the container) untars it,
 * installs Python, adds symlinks to any additional JARs and runs the launcher.
 * </p>
 */
public class TrinoAwsProxyServerTestContainer
        extends GenericContainer<TrinoAwsProxyServerTestContainer>
{
    private static final String startScript = """
            #!/usr/bin/env bash
            
            set -euo pipefail
            
            cd /opt/app
            tar xf app.tar.gz
            
            set -xeu
            apt-get update -q
            apt-get install -q -y python3 net-tools lsof
            update-alternatives --install /usr/bin/python python /usr/bin/python3 1
            
            cd /opt/app/trino*
            mkdir etc
            cp ../config.properties etc/config.properties
            touch etc/jvm.config
            
            ADDITIONAL_LIBS=($ADDITIONAL_LIB$)
            
            if (( ${#ADDITIONAL_LIBS[@]} != 0 )); then
                for lib in "${ADDITIONAL_LIBS[@]}"; do
                    name="${lib##*/}"\s
                    ln -s "$lib" "./lib/$name"
                done
            fi
            
            bin/launcher run
            """;

    private static final Properties projectProperties;

    static {
        URL resource = Resources.getResource("project-version.properties");
        projectProperties = new Properties();
        try (InputStream inputStream = resource.openStream()) {
            projectProperties.load(inputStream);
        }
        catch (IOException e) {
            throw new RuntimeException("Could not read project-version file", e);
        }
    }

    private final ImmutableMap.Builder<String, String> configFile = ImmutableMap.builder();
    private final ImmutableSet.Builder<String> additionalJars = ImmutableSet.builder();

    public TrinoAwsProxyServerTestContainer()
    {
        super(DockerImageName.parse("eclipse-temurin").withTag("%s-jdk-jammy".formatted(javaVersion())));

        withCopyToContainer(tarballTransferable(), "/opt/app/app.tar.gz")
                .withCommand("/opt/app/startup.sh")
                .withExposedPorts(8080)
                .waitingFor(new LogMessageWaitStrategy().withRegEx(".*SERVER STARTED.*"));

        configFile.put("node.environment", "test");
    }

    public TrinoAwsProxyServerTestContainer withConfig(String name, String value)
    {
        assertNotStarted();

        configFile.put(name, value);
        return this;
    }

    public TrinoAwsProxyServerTestContainer withAdditionalLib(String containerPath)
    {
        assertNotStarted();

        additionalJars.add(containerPath);
        return this;
    }

    @Override
    @SuppressWarnings("OctalInteger")
    public void start()
    {
        String configFileContents = configFile.build()
                .entrySet()
                .stream()
                .map(entry -> "%s=%s".formatted(entry.getKey(), entry.getValue()))
                .collect(Collectors.joining("\n"));

        Set<String> additionalJarsList = additionalJars.build();
        String additionalLib = additionalJarsList.isEmpty() ? "" : additionalJarsList.stream()
                .collect(Collectors.joining(" ", "\"", "\""));
        String adjustedScript = startScript.replace("$ADDITIONAL_LIB$", additionalLib);

        withCopyToContainer(Transferable.of(configFileContents), "/opt/app/config.properties")
                .withCopyToContainer(Transferable.of(adjustedScript, 0744), "/opt/app/startup.sh");

        super.start();
    }

    public static String projectVersion()
    {
        return projectProperties.getProperty("project.version");
    }

    public static String javaVersion()
    {
        return projectProperties.getProperty("java.version");
    }

    private void assertNotStarted()
    {
        checkState(getContainerId() == null, "Container has already been started");
    }

    private static Transferable tarballTransferable()
    {
        URL resource = Resources.getResource(tarball());
        return new UrlTransferable(resource);
    }

    private static String tarball()
    {
        return "trino-aws-proxy-%s.tar.gz".formatted(projectVersion());
    }

    private static class UrlTransferable
            implements Transferable
    {
        private final URL url;

        private UrlTransferable(URL url)
        {
            this.url = requireNonNull(url, "url is null");
        }

        @Override
        public long getSize()
        {
            // not used
            return 0;
        }

        @Override
        public void transferTo(TarArchiveOutputStream tarArchiveOutputStream, String destination)
        {
            try {
                long size;
                try (InputStream countStream = url.openStream()) {
                    size = exhaust(countStream);
                }

                TarArchiveEntry tarEntry = new TarArchiveEntry(destination);
                tarEntry.setMode(getFileMode());
                tarEntry.setSize(size);
                tarArchiveOutputStream.putArchiveEntry(tarEntry);
                try (InputStream inputStream = url.openStream()) {
                    inputStream.transferTo(tarArchiveOutputStream);
                }
                tarArchiveOutputStream.closeArchiveEntry();
            }
            catch (IOException e) {
                throw new RuntimeException("Can't transfer from %s to %s".formatted(url, destination), e);
            }
        }
    }
}
