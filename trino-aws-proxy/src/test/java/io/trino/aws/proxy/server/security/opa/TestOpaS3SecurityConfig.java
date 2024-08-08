package io.trino.aws.proxy.server.security.opa;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.*;

public class TestOpaS3SecurityConfig {

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Map<String, String> properties = ImmutableMap.of(
                "opa-s3-security.server-base-uri", "http://localhost");

        OpaS3SecurityConfig expected = new OpaS3SecurityConfig().setOpaServerBaseUri("http://localhost");
        assertFullMapping(properties, expected);
    }
}
