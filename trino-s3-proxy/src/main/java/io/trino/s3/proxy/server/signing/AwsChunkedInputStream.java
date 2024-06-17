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
package io.trino.s3.proxy.server.signing;

import com.google.common.base.Splitter;
import org.apache.commons.httpclient.util.EncodingUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.httpclient.HttpParser.parseHeaders;

// based/copied on Apache Commons ChunkedInputStream
class AwsChunkedInputStream
        extends InputStream
{
    private final InputStream delegate;
    private final Optional<ChunkSigningSession> chunkSigningSession;

    private int chunkSize;
    private int position;
    private boolean latent = true;
    private boolean eof;
    private boolean closed;

    AwsChunkedInputStream(InputStream delegate, Optional<ChunkSigningSession> chunkSigningSession)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.chunkSigningSession = requireNonNull(chunkSigningSession, "chunkSigningSession is null");
    }

    public int read()
            throws IOException
    {
        checkState(!closed, "Stream is closed");

        if (eof) {
            return -1;
        }
        if (position >= chunkSize) {
            nextChunk();
            if (eof) {
                return -1;
            }
        }
        position++;
        int i = delegate.read();
        if (i >= 0) {
            chunkSigningSession.ifPresent(session -> session.write((byte) (i & 0xff)));
        }
        return i;
    }

    public int read(byte[] b, int off, int len)
            throws IOException
    {
        checkState(!closed, "Stream is closed");

        if (eof) {
            return -1;
        }
        if (position >= chunkSize) {
            nextChunk();
            if (eof) {
                return -1;
            }
        }

        len = Math.min(len, chunkSize - position);
        int count = delegate.read(b, off, len);
        position += count;

        chunkSigningSession.ifPresent(session -> session.write(b, off, count));

        return count;
    }

    private void readCRLF()
            throws IOException
    {
        int cr = delegate.read();
        int lf = delegate.read();
        if ((cr != '\r') || (lf != '\n')) {
            throw new IOException("CRLF expected at end of chunk: " + cr + "/" + lf);
        }
    }

    private void nextChunk()
            throws IOException
    {
        if (!latent) {
            readCRLF();
        }

        ChunkMetadata metadata = chunkMetadata(delegate);
        chunkSigningSession.ifPresent(session -> {
            String chunkSignature = metadata.chunkSignature().orElseThrow(() -> new UncheckedIOException(new IOException("Chunk is missing a signature: " + metadata.rawDataString)));
            session.startChunk(chunkSignature);
        });

        chunkSize = metadata.chunkSize;
        latent = false;
        position = 0;
        if (chunkSize == 0) {
            chunkSigningSession.ifPresent(ChunkSigningSession::complete);
            eof = true;
            parseHeaders(delegate, "UTF-8");
        }
    }

    private record ChunkMetadata(String rawDataString, int chunkSize, Optional<String> chunkSignature)
    {
        private ChunkMetadata
        {
            requireNonNull(rawDataString, "rawDataString is null");
            requireNonNull(chunkSignature, "chunkSignature is null");
        }
    }

    private static ChunkMetadata chunkMetadata(InputStream in)
            throws IOException
    {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        // States: 0=normal, 1=\r was scanned, 2=inside quoted string, -1=end
        int state = 0;
        while (state != -1) {
            int b = in.read();
            if (b == -1) {
                throw new IOException("chunked stream ended unexpectedly");
            }
            switch (state) {
                case 0:
                    switch (b) {
                        case '\r':
                            state = 1;
                            break;
                        case '\"':
                            state = 2;
                            /* fall through */
                        default:
                            outputStream.write(b);
                    }
                    break;

                case 1:
                    if (b == '\n') {
                        state = -1;
                    }
                    else {
                        // this was not CRLF
                        throw new IOException("Protocol violation: Unexpected single newline character in chunk size");
                    }
                    break;

                case 2:
                    switch (b) {
                        case '\\':
                            b = in.read();
                            outputStream.write(b);
                            break;
                        case '\"':
                            state = 0;
                            /* fall through */
                        default:
                            outputStream.write(b);
                    }
                    break;
                default:
                    throw new RuntimeException("assertion failed");
            }
        }

        String dataString = EncodingUtil.getAsciiString(outputStream.toByteArray());

        String chunkSizeString;
        Optional<String> chunkSignature;

        int separatorIndex = dataString.indexOf(';');
        if (separatorIndex > 0) {
            chunkSizeString = dataString.substring(0, separatorIndex).trim();

            if ((separatorIndex + 1) < dataString.length()) {
                String remainder = dataString.substring(separatorIndex + 1).trim();
                chunkSignature = Splitter.on(';').trimResults().withKeyValueSeparator('=').split(remainder)
                        .entrySet()
                        .stream()
                        .filter(entry -> entry.getKey().equalsIgnoreCase("chunk-signature"))
                        .map(Map.Entry::getValue)
                        .findFirst();
            }
            else {
                chunkSignature = Optional.empty();
            }
        }
        else {
            chunkSizeString = dataString.trim();
            chunkSignature = Optional.empty();
        }

        int chunkSize;
        try {
            chunkSize = Integer.parseInt(chunkSizeString, 16);
        }
        catch (NumberFormatException e) {
            throw new IOException("Bad chunk size: " + chunkSizeString);
        }

        return new ChunkMetadata(dataString, chunkSize, chunkSignature);
    }

    public void close()
            throws IOException
    {
        if (!closed) {
            try {
                if (!eof) {
                    exhaustInputStream(this);
                }
            }
            finally {
                eof = true;
                closed = true;
            }
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private static void exhaustInputStream(InputStream inStream)
            throws IOException
    {
        // read and discard the remainder of the message
        byte[] buffer = new byte[8192];
        while (inStream.read(buffer) >= 0) {
            // NOP
        }
    }
}
