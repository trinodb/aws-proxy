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

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import io.trino.aws.proxy.server.signing.TestingChunkSigningSession;
import io.trino.aws.proxy.spi.credentials.Credential;
import io.trino.aws.proxy.spi.signing.ChunkSigningSession;
import io.trino.aws.proxy.spi.util.AwsTimestamp;
import jakarta.ws.rs.WebApplicationException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.signer.internal.chunkedencoding.AwsS3V4ChunkSigner;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
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
    private static final String ILLEGAL_CHUNK_SIGNATURE = "0".repeat(AwsS3V4ChunkSigner.getSignatureLength());

    private interface ChunkReader
    {
        void read(String chunkedData, int decodedContentLength, TestingChunkSigningSession signingSession, ByteArrayOutputStream output)
                throws IOException;
    }

    private static TestingChunkSigningSession goodTestSigningSession()
    {
        return TestingChunkSigningSession.build(GOOD_CREDENTIAL, GOOD_SEED);
    }

    private static TestingChunkSigningSession fixedTimeGoodTestSigningSession()
    {
        return TestingChunkSigningSession.build(GOOD_CREDENTIAL, GOOD_SEED, AwsTimestamp.fromRequestTimestamp("20240801T010203Z"));
    }

    @Test
    public void testGood()
            throws IOException
    {
        TestingChunkSigningSession session = goodTestSigningSession();
        String chunkedStream = session.generateChunkedStream(GOOD_CONTENT, 3);

        assertThat(readChunked(chunkedStream, session)).isEqualTo(GOOD_CONTENT.getBytes(UTF_8));
    }

    @Test
    public void testRecreateSessionValidatesGoodPayload()
            throws IOException
    {
        String chunkedStream = goodTestSigningSession().generateChunkedStream(GOOD_CONTENT, 3);

        assertThat(readChunked(chunkedStream, goodTestSigningSession())).isEqualTo(GOOD_CONTENT.getBytes(UTF_8));
    }

    @Test
    public void testBadSeed()
    {
        String chunkedStream = goodTestSigningSession().generateChunkedStream(GOOD_CONTENT, 3);

        assertThatThrownBy(() -> readChunked(chunkedStream, TestingChunkSigningSession.build(GOOD_CREDENTIAL, BAD_SEED)))
                .isInstanceOf(WebApplicationException.class);
    }

    @Test
    public void testBadCredential()
    {
        String chunkedStream = goodTestSigningSession().generateChunkedStream(GOOD_CONTENT, 3);

        assertThatThrownBy(() -> readChunked(chunkedStream, TestingChunkSigningSession.build(BAD_CREDENTIAL, BAD_SEED)))
                .isInstanceOf(WebApplicationException.class);
    }

    @Test
    public void testMultipleExtensions()
            throws IOException
    {
        String chunkedStream = goodTestSigningSession().generateChunkedStream(GOOD_CONTENT, 3);
        chunkedStream = chunkedStream.replace(";chunk-signature=", ";foo=bar;chunk-signature=");

        assertThat(readChunked(chunkedStream, goodTestSigningSession())).isEqualTo(GOOD_CONTENT.getBytes(UTF_8));
    }

    @Test
    public void testAwsChunkedCornerCases()
            throws IOException
    {
        // Ensure that the input stream always validates the data prior to returning x-amz-decoded-content-length bytes
        // Otherwise we could be skipping signature verification if a chunk reports a larger size than x-amz-decoded-content-length
        // We need to ensure this is the case regardless of the amount of bytes we read at a time - which is controlled by Jetty
        for (ChunkReader readerMethod : ImmutableList.of(
                // Read 1 byte at a time using the read() method
                TestAwsChunkedInputStream::tryReadAwsChunkedData,
                // Read using the read(byte[], off, len) method
                // Read 1 byte at a time
                buildBatchedAwsChunkedReader(1),
                // Read an even number of bytes
                buildBatchedAwsChunkedReader(2),
                // Read an odd number of bytes
                buildBatchedAwsChunkedReader(3),
                // Read a very large number of bytes
                buildBatchedAwsChunkedReader(4096))) {
            // Both chunks are the same size
            testAwsChunkedCornerCases("abcdef", "ghijkl", readerMethod);
            // Chunks are different sizes
            testAwsChunkedCornerCases("abcdef", "ghi", readerMethod);
            // Potential tricky case: the last chunk is a single byte (meaning a single read from the chunk should force signature verification)
            testAwsChunkedCornerCases("abcdef", "g", readerMethod);
        }
    }

    private void testAwsChunkedCornerCases(String firstChunkContent, String secondChunkContent, ChunkReader readMethod)
            throws IOException
    {
        final int totalContentLength = firstChunkContent.length() + secondChunkContent.length();

        String firstChunkSignature = fixedTimeGoodTestSigningSession().getChunkSignature(firstChunkContent, GOOD_SEED);
        String secondChunkSignature = fixedTimeGoodTestSigningSession().getChunkSignature(secondChunkContent, firstChunkSignature);
        String finalChunkSignature = fixedTimeGoodTestSigningSession().getChunkSignature("", secondChunkSignature);

        String validFirstChunk = buildChunk(firstChunkContent.length(), firstChunkSignature, firstChunkContent);
        String validSecondChunk = buildChunk(secondChunkContent.length(), secondChunkSignature, secondChunkContent);
        String validFinalChunk = buildChunk(0, finalChunkSignature, "");

        // Sanity check - the below should be read correctly
        ByteArrayOutputStream testOutput = new ByteArrayOutputStream();
        String correctChunkedData = String.join("", validFirstChunk, validSecondChunk, validFinalChunk);
        readMethod.read(correctChunkedData, totalContentLength, fixedTimeGoodTestSigningSession(), testOutput);
        assertThat(testOutput.toByteArray()).hasSize(totalContentLength);
        assertThat(testOutput.toString(UTF_8)).isEqualTo(firstChunkContent + secondChunkContent);

        // Data is correctly chunked, but the decoded content length is underreported
        testIllegalAwsChunkedData(
                correctChunkedData,
                totalContentLength - 1,
                fixedTimeGoodTestSigningSession(),
                readMethod);
        // Data is correctly chunked, but the decoded content length is overreported
        testIllegalAwsChunkedData(
                correctChunkedData,
                totalContentLength + 1,
                fixedTimeGoodTestSigningSession(),
                readMethod);

        // Missing final chunk
        testIllegalAwsChunkedData(
                String.join("", validFirstChunk, validSecondChunk),
                totalContentLength,
                fixedTimeGoodTestSigningSession(),
                readMethod);

        // First chunk has an invalid signature
        testIllegalAwsChunkedData(
                String.join("", buildChunk(firstChunkContent.length(), ILLEGAL_CHUNK_SIGNATURE, firstChunkContent), validSecondChunk, validFinalChunk),
                totalContentLength,
                fixedTimeGoodTestSigningSession(),
                readMethod);
        // Second chunk has an invalid signature
        testIllegalAwsChunkedData(
                String.join("", validFirstChunk, buildChunk(secondChunkContent.length(), ILLEGAL_CHUNK_SIGNATURE, secondChunkContent), validFinalChunk),
                totalContentLength,
                fixedTimeGoodTestSigningSession(),
                readMethod);
        // Final chunk has an invalid signature
        testIllegalAwsChunkedData(
                String.join("", validFirstChunk, validSecondChunk, buildChunk(0, ILLEGAL_CHUNK_SIGNATURE, "")),
                totalContentLength,
                fixedTimeGoodTestSigningSession(),
                readMethod);

        // First chunk overreports its size - total content length unchanged
        testIllegalAwsChunkedData(
                String.join("", buildChunk(firstChunkContent.length() + 1, firstChunkSignature, firstChunkContent), validSecondChunk, validFinalChunk),
                totalContentLength,
                fixedTimeGoodTestSigningSession(),
                readMethod);
        // First chunk overreports its size - total content length increased to match
        testIllegalAwsChunkedData(
                String.join("", buildChunk(firstChunkContent.length() + 1, firstChunkSignature, firstChunkContent), validSecondChunk, validFinalChunk),
                totalContentLength + 1,
                fixedTimeGoodTestSigningSession(),
                readMethod);

        // Second chunk overreports its size - total content length unchanged
        testIllegalAwsChunkedData(
                String.join("", validFirstChunk, buildChunk(secondChunkContent.length() + 1, secondChunkSignature, secondChunkContent), validFinalChunk),
                totalContentLength,
                fixedTimeGoodTestSigningSession(),
                readMethod);
        // Second chunk overreports its size - total content length increased to match
        testIllegalAwsChunkedData(
                String.join("", validFirstChunk, buildChunk(secondChunkContent.length() + 1, secondChunkSignature, secondChunkContent), validFinalChunk),
                totalContentLength + 1,
                fixedTimeGoodTestSigningSession(),
                readMethod);

        // Final chunk has invalid size - total content length unchanged
        testIllegalAwsChunkedData(
                String.join("", validFirstChunk, validSecondChunk, buildChunk(1, finalChunkSignature, "")),
                totalContentLength,
                fixedTimeGoodTestSigningSession(),
                readMethod);
        // Final chunk has invalid size - total content length increased to match
        testIllegalAwsChunkedData(
                String.join("", validFirstChunk, validSecondChunk, buildChunk(1, finalChunkSignature, "")),
                totalContentLength + 1,
                fixedTimeGoodTestSigningSession(),
                readMethod);
    }

    private static String buildChunk(int reportedChunkSize, String chunkSignature, String chunkContent)
    {
        return "%s;chunk-signature=%s\r\n%s\r\n".formatted(Integer.toString(reportedChunkSize, 16), chunkSignature, chunkContent);
    }

    private static ChunkReader buildBatchedAwsChunkedReader(int bytesToReadAtATime)
    {
        return (chunkedData, decodedContentLength, signingSession, output) -> tryReadAwsChunkedDataBatch(chunkedData, decodedContentLength, signingSession, output, bytesToReadAtATime);
    }

    private static void tryReadAwsChunkedDataBatch(String chunkedData, int decodedContentLength, TestingChunkSigningSession signingSession, ByteArrayOutputStream output, int bytesToReadAtATime)
            throws IOException
    {
        int remainingBytes = decodedContentLength;
        try (InputStream in = new AwsChunkedInputStream(new ByteArrayInputStream(chunkedData.getBytes(UTF_8)), signingSession, decodedContentLength)) {
            while (remainingBytes > 0) {
                byte[] readBytes = new byte[bytesToReadAtATime];
                int count = in.read(readBytes, 0, bytesToReadAtATime);
                if (count < 0) {
                    throw new EOFException("Unexpected EOF");
                }
                remainingBytes -= count;
                output.write(readBytes, 0, count);
            }
        }
    }

    private static void tryReadAwsChunkedData(String chunkedData, int decodedContentLength, TestingChunkSigningSession signingSession, ByteArrayOutputStream output)
            throws IOException
    {
        int remainingBytes = decodedContentLength;
        try (InputStream in = new AwsChunkedInputStream(new ByteArrayInputStream(chunkedData.getBytes(UTF_8)), signingSession, decodedContentLength)) {
            while (remainingBytes-- > 0) {
                int readByte = in.read();
                if (readByte == -1) {
                    throw new EOFException("Unexpected EOF");
                }
                output.write(readByte);
            }
        }
    }

    private static void testIllegalAwsChunkedData(String chunkedData, int decodedContentLength, TestingChunkSigningSession signingSession, ChunkReader readerMethod)
    {
        ByteArrayOutputStream testOutput = new ByteArrayOutputStream();
        assertThatThrownBy(() -> readerMethod.read(chunkedData, decodedContentLength, signingSession, testOutput)).isInstanceOfAny(WebApplicationException.class, IOException.class);
        assertThat(testOutput.toByteArray().length).isLessThan(decodedContentLength);
    }

    private static byte[] readChunked(String chunkedStream, TestingChunkSigningSession signingSession)
            throws IOException
    {
        try (InputStream in = new AwsChunkedInputStream(new ByteArrayInputStream(chunkedStream.getBytes(UTF_8)), signingSession, chunkedStream.length())) {
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
        byte[] rawBytes = CHUNKED_INPUT.getBytes(UTF_8);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(rawBytes);
        InputStream in = new AwsChunkedInputStream(inputStream, new DummyChunkSigningSession(), rawBytes.length);
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
        byte[] rawBytes = CHUNKED_INPUT.getBytes(UTF_8);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(rawBytes);
        InputStream in = new AwsChunkedInputStream(inputStream, new DummyChunkSigningSession(), rawBytes.length);

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
        byte[] rawBytes = s.getBytes(UTF_8);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(rawBytes);
        InputStream in = new AwsChunkedInputStream(inputStream, new DummyChunkSigningSession(), rawBytes.length);
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
        byte[] rawBytes = s.getBytes(UTF_8);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(rawBytes);
        InputStream in = new AwsChunkedInputStream(inputStream, new DummyChunkSigningSession(), rawBytes.length);
        byte[] tmp = new byte[5];
        // altered from original test. Our AwsChunkedInputStream is improved and throws when the final chunk is missing or bad
        assertThrows(WebApplicationException.class, () -> in.read(tmp));
    }

    // Truncated stream (missing closing CRLF)
    @Test
    public void testCorruptChunkedInputStreamTruncatedCRLF()
            throws IOException
    {
        // altered to add a few more bad stings
        Stream.of("5;chunk-signature=0\r\n01234", ";chunk-signature=0\r\n01234\r\n", "5;chunk-signature=0\r\n012340;chunk-signature=0\r\n\r\n")
                .forEach(s -> {
                    byte[] rawBytes = s.getBytes(UTF_8);
                    ByteArrayInputStream inputStream = new ByteArrayInputStream(rawBytes);
                    InputStream in = new AwsChunkedInputStream(inputStream, new DummyChunkSigningSession(), rawBytes.length);
                    byte[] tmp = new byte[5];
                    // altered from original test. Our AwsChunkedInputStream is improved and throws when the final chunk is missing or bad
                    assertThrows(WebApplicationException.class, () -> in.read(tmp));
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
        byte[] rawBytes = s.getBytes(UTF_8);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(rawBytes);
        InputStream in = new AwsChunkedInputStream(inputStream, new DummyChunkSigningSession(), rawBytes.length);
        byte[] buffer = new byte[300];
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertThrows(WebApplicationException.class, () -> {
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
        byte[] rawBytes = s.getBytes(UTF_8);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(rawBytes);
        InputStream in = new AwsChunkedInputStream(inputStream, new DummyChunkSigningSession(), rawBytes.length);
        assertThrows(WebApplicationException.class, in::read);
        in.close();
    }

    // Invalid chunk size
    @Test
    public void testCorruptChunkedInputStreamInvalidSize()
            throws IOException
    {
        String s = "whatever;chunk-signature=0\r\n01234\r\n5;chunk-signature=0\r\n56789\r\n0;chunk-signature=0\r\n\r\n";
        byte[] rawBytes = s.getBytes(UTF_8);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(rawBytes);
        InputStream in = new AwsChunkedInputStream(inputStream, new DummyChunkSigningSession(), rawBytes.length);
        assertThrows(WebApplicationException.class, in::read);
        in.close();
    }

    // Negative chunk size
    @Test
    public void testCorruptChunkedInputStreamNegativeSize()
            throws IOException
    {
        String s = "-5;chunk-signature=0\r\n01234\r\n5;chunk-signature=0\r\n56789\r\n0;chunk-signature=0\r\n\r\n";
        byte[] rawBytes = s.getBytes(UTF_8);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(rawBytes);
        InputStream in = new AwsChunkedInputStream(inputStream, new DummyChunkSigningSession(), rawBytes.length);
        assertThrows(WebApplicationException.class, in::read);
        in.close();
    }

    // Truncated chunk
    @Test
    public void testCorruptChunkedInputStreamTruncatedChunk()
            throws IOException
    {
        String s = "3;chunk-signature=0\r\n12";
        byte[] rawBytes = s.getBytes(UTF_8);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(rawBytes);
        InputStream in = new AwsChunkedInputStream(inputStream, new DummyChunkSigningSession(), rawBytes.length);
        byte[] buffer = new byte[300];
        assertEquals(2, in.read(buffer));
        assertThrows(WebApplicationException.class, () -> in.read(buffer));
        in.close();
    }

    @Test
    public void testCorruptChunkedInputStreamClose()
    {
        String s = "whatever;chunk-signature=0\r\n01234\r\n5;chunk-signature=0\r\n56789\r\n0;chunk-signature=0\r\n\r\n";
        byte[] rawBytes = s.getBytes(UTF_8);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(rawBytes);
        InputStream in = new AwsChunkedInputStream(inputStream, new DummyChunkSigningSession(), rawBytes.length);
        assertThrows(WebApplicationException.class, in::read);
    }

    @Test
    public void testEmptyChunkedInputStream()
            throws IOException
    {
        String s = "0;chunk-signature=0\r\n\r\n";
        byte[] rawBytes = s.getBytes(UTF_8);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(rawBytes);
        InputStream in = new AwsChunkedInputStream(inputStream, new DummyChunkSigningSession(), rawBytes.length);
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
        byte[] rawBytes = "499602D2;chunk-signature=0\r\n01234567".getBytes(UTF_8);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(rawBytes);
        InputStream in = new AwsChunkedInputStream(inputStream, new DummyChunkSigningSession(), 1234567890);

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
