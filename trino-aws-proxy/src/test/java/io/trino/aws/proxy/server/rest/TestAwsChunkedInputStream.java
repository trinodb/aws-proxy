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

import com.google.common.io.ByteStreams;
import io.trino.aws.proxy.server.signing.TestingChunkSigningSession;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.signing.ChunkSigningSession;
import jakarta.ws.rs.WebApplicationException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestAwsChunkedInputStream
{
    private static final Credential GOOD_CREDENTIAL = new Credential("TEST_ACCESS_KEY", "TEST_SECRET_KEY");
    private static final String GOOD_CONTENT = "The quick brown fox jumps over the lazy dog's head";
    private static final String GOOD_SEED = "THIS IS A FAKE GOOD SEED";

    private static final Credential BAD_CREDENTIAL = new Credential("BAD_TEST_ACCESS_KEY", "BAD_TEST_SECRET_KEY");
    private static final String BAD_SEED = "THIS IS A FAKE BAD SEED";

    @Test
    public void testGood()
            throws IOException
    {
        TestingChunkSigningSession signingSession = TestingChunkSigningSession.build(GOOD_CREDENTIAL, GOOD_SEED);
        String chunkedStream = signingSession.generateChunkedStream(GOOD_CONTENT, 3);

        assertThat(readChunked(chunkedStream, signingSession)).isEqualTo(GOOD_CONTENT.getBytes(UTF_8));
    }

    @Test
    public void testRecreateSessionValidatesGoodPayload()
            throws IOException
    {
        TestingChunkSigningSession signingSession = TestingChunkSigningSession.build(GOOD_CREDENTIAL, GOOD_SEED);
        String chunkedStream = signingSession.generateChunkedStream(GOOD_CONTENT, 3);

        assertThat(readChunked(chunkedStream, TestingChunkSigningSession.build(GOOD_CREDENTIAL, GOOD_SEED))).isEqualTo(GOOD_CONTENT.getBytes(UTF_8));
    }

    @Test
    public void testBadSeed()
    {
        TestingChunkSigningSession signingSession = TestingChunkSigningSession.build(GOOD_CREDENTIAL, GOOD_SEED);
        String chunkedStream = signingSession.generateChunkedStream(GOOD_CONTENT, 3);

        assertThatThrownBy(() -> readChunked(chunkedStream, TestingChunkSigningSession.build(GOOD_CREDENTIAL, BAD_SEED)))
                .isInstanceOf(WebApplicationException.class);
    }

    @Test
    public void testBadCredential()
    {
        TestingChunkSigningSession signingSession = TestingChunkSigningSession.build(GOOD_CREDENTIAL, GOOD_SEED);
        String chunkedStream = signingSession.generateChunkedStream(GOOD_CONTENT, 3);

        assertThatThrownBy(() -> readChunked(chunkedStream, TestingChunkSigningSession.build(BAD_CREDENTIAL, BAD_SEED)))
                .isInstanceOf(WebApplicationException.class);
    }

    @Test
    public void testMultipleExtensions()
            throws IOException
    {
        TestingChunkSigningSession signingSession = TestingChunkSigningSession.build(GOOD_CREDENTIAL, GOOD_SEED);
        String chunkedStream = signingSession.generateChunkedStream(GOOD_CONTENT, 3);
        chunkedStream = chunkedStream.replace(";chunk-signature=", ";foo=bar;chunk-signature=");

        assertThat(readChunked(chunkedStream, signingSession)).isEqualTo(GOOD_CONTENT.getBytes(UTF_8));
    }

    private byte[] readChunked(String chunkedStream, TestingChunkSigningSession signingSession)
            throws IOException
    {
        try (InputStream in = new AwsChunkedInputStream(new ByteArrayInputStream(chunkedStream.getBytes(UTF_8)), signingSession)) {
            return ByteStreams.toByteArray(in);
        }
    }

    // NOTE: below vvvvvvv tests are modified from https://github.com/apache/httpcomponents-core/blob/e009a923eefe79cf3593efbb0c18a3525ae63669/httpcore5/src/test/java/org/apache/hc/core5/http/impl/io/TestChunkCoding.java

    private static final String CHUNKED_INPUT
            = "10;chunk-signature=0\r\n1234567890123456\r\n5;chunk-signature=0\r\n12345\r\n0;chunk-signature=0\r\n\r\n";

    private static final String CHUNKED_RESULT
            = "123456789012345612345";

    @Test
    public void testChunkedInputStreamLargeBuffer()
            throws IOException
    {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(CHUNKED_INPUT.getBytes(UTF_8));
        InputStream in = new AwsChunkedInputStream(inputStream, new DummyChunkSigningSession());
        byte[] buffer = new byte[300];
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int len;
        while ((len = in.read(buffer)) > 0) {
            out.write(buffer, 0, len);
        }
        assertEquals(-1, in.read(buffer));
        assertEquals(-1, in.read(buffer));

        in.close();

        String result = out.toString(UTF_8);
        assertEquals(CHUNKED_RESULT, result);
    }

    //Test for when buffer is smaller than chunk size.
    @Test
    public void testChunkedInputStreamSmallBuffer()
            throws IOException
    {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(CHUNKED_INPUT.getBytes(UTF_8));
        InputStream in = new AwsChunkedInputStream(inputStream, new DummyChunkSigningSession());

        byte[] buffer = new byte[7];
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int len;
        while ((len = in.read(buffer)) > 0) {
            out.write(buffer, 0, len);
        }
        assertEquals(-1, in.read(buffer));
        assertEquals(-1, in.read(buffer));

        String result = out.toString(UTF_8);
        assertEquals("123456789012345612345", result);
    }

    // One byte read
    @Test
    public void testChunkedInputStreamOneByteRead()
            throws IOException
    {
        String s = "5;chunk-signature=0\r\n01234\r\n5;chunk-signature=0\r\n56789\r\n0;chunk-signature=0\r\n\r\n";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(s.getBytes(UTF_8));
        InputStream in = new AwsChunkedInputStream(inputStream, new DummyChunkSigningSession());
        int ch;
        int i = '0';
        while ((ch = in.read()) != -1) {
            assertEquals(i, ch);
            i++;
        }
        assertEquals(-1, in.read());
        assertEquals(-1, in.read());

        in.close();
    }

    // Missing closing chunk
    @Test
    public void testChunkedInputStreamNoClosingChunk()
    {
        String s = "5;chunk-signature=0\r\n01234\r\n";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(s.getBytes(UTF_8));
        InputStream in = new AwsChunkedInputStream(inputStream, new DummyChunkSigningSession());
        byte[] tmp = new byte[5];
        // altered from original test. Our AwsChunkedInputStream is improved and throws when the final chunk is missing or bad
        assertThrows(IOException.class, () -> in.read(tmp));
    }

    // Truncated stream (missing closing CRLF)
    @Test
    public void testCorruptChunkedInputStreamTruncatedCRLF()
            throws IOException
    {
        // altered to add a few more bad stings
        Stream.of("5;chunk-signature=0\r\n01234", ";chunk-signature=0\r\n01234\r\n", "5;chunk-signature=0\r\n012340;chunk-signature=0\r\n\r\n")
                .forEach(s -> {
                    ByteArrayInputStream inputStream = new ByteArrayInputStream(s.getBytes(UTF_8));
                    InputStream in = new AwsChunkedInputStream(inputStream, new DummyChunkSigningSession());
                    byte[] tmp = new byte[5];
                    // altered from original test. Our AwsChunkedInputStream is improved and throws when the final chunk is missing or bad
                    assertThrows(IOException.class, () -> in.read(tmp));
                    try {
                        in.close();
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
    }

    @Test
    public void testCorruptChunkedInputStreamMissingCRLF()
            throws IOException
    {
        String s = "5;chunk-signature=0\r\n012345\r\n56789\r\n0;chunk-signature=0\r\n\r\n";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(s.getBytes(UTF_8));
        InputStream in = new AwsChunkedInputStream(inputStream, new DummyChunkSigningSession());
        byte[] buffer = new byte[300];
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertThrows(IOException.class, () -> {
            int len;
            while ((len = in.read(buffer)) > 0) {
                out.write(buffer, 0, len);
            }
        });
        in.close();
    }

    // Missing LF
    @Test
    public void testCorruptChunkedInputStreamMissingLF()
            throws IOException
    {
        String s = "5;chunk-signature=0\r01234\r\n5;chunk-signature=0\r\n56789\r\n0;chunk-signature=0\r\n\r\n";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(s.getBytes(UTF_8));
        InputStream in = new AwsChunkedInputStream(inputStream, new DummyChunkSigningSession());
        assertThrows(IOException.class, in::read);
        in.close();
    }

    // Invalid chunk size
    @Test
    public void testCorruptChunkedInputStreamInvalidSize()
            throws IOException
    {
        String s = "whatever;chunk-signature=0\r\n01234\r\n5;chunk-signature=0\r\n56789\r\n0;chunk-signature=0\r\n\r\n";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(s.getBytes(UTF_8));
        InputStream in = new AwsChunkedInputStream(inputStream, new DummyChunkSigningSession());
        assertThrows(IOException.class, in::read);
        in.close();
    }

    // Negative chunk size
    @Test
    public void testCorruptChunkedInputStreamNegativeSize()
            throws IOException
    {
        String s = "-5;chunk-signature=0\r\n01234\r\n5;chunk-signature=0\r\n56789\r\n0;chunk-signature=0\r\n\r\n";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(s.getBytes(UTF_8));
        InputStream in = new AwsChunkedInputStream(inputStream, new DummyChunkSigningSession());
        assertThrows(IOException.class, in::read);
        in.close();
    }

    // Truncated chunk
    @Test
    public void testCorruptChunkedInputStreamTruncatedChunk()
            throws IOException
    {
        String s = "3;chunk-signature=0\r\n12";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(s.getBytes(UTF_8));
        InputStream in = new AwsChunkedInputStream(inputStream, new DummyChunkSigningSession());
        byte[] buffer = new byte[300];
        assertEquals(2, in.read(buffer));
        assertThrows(IOException.class, () -> in.read(buffer));
        in.close();
    }

    @Test
    public void testCorruptChunkedInputStreamClose()
            throws IOException
    {
        String s = "whatever;chunk-signature=0\r\n01234\r\n5;chunk-signature=0\r\n56789\r\n0;chunk-signature=0\r\n\r\n";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(s.getBytes(UTF_8));
        try (InputStream in = new AwsChunkedInputStream(inputStream, new DummyChunkSigningSession())) {
            assertThrows(IOException.class, in::read);
        }
    }

    @Test
    public void testEmptyChunkedInputStream()
            throws IOException
    {
        String s = "0;chunk-signature=0\r\n\r\n";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(s.getBytes(UTF_8));
        InputStream in = new AwsChunkedInputStream(inputStream, new DummyChunkSigningSession());
        byte[] buffer = new byte[300];
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int len;
        while ((len = in.read(buffer)) > 0) {
            out.write(buffer, 0, len);
        }
        assertEquals(0, out.size());
        in.close();
    }

    // Test for when buffer is larger than chunk size
    @Test
    public void testHugeChunk()
            throws IOException
    {
        ByteArrayInputStream inputStream = new ByteArrayInputStream("499602D2;chunk-signature=0\r\n01234567".getBytes(UTF_8));
        InputStream in = new AwsChunkedInputStream(inputStream, new DummyChunkSigningSession());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0; i < 8; ++i) {
            out.write(in.read());
        }

        String result = out.toString(UTF_8);
        assertEquals("01234567", result);
    }

    private static class DummyChunkSigningSession
            implements ChunkSigningSession
    {
        @Override
        public void startChunk(String expectedSignature)
        {
            // NOP
        }

        @Override
        public void complete()
        {
            // NOP
        }

        @Override
        public void write(byte b)
        {
            // NOP
        }

        @Override
        public void write(byte[] b, int off, int len)
        {
            // NOP
        }
    }
}
