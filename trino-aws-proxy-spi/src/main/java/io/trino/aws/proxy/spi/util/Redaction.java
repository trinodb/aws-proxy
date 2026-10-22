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
package io.trino.aws.proxy.spi.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Helpers for rendering credential material in {@code toString()} and log output
 * without exposing the secret values.
 */
public final class Redaction
{
    private static final int VISIBLE_CHARS = 4;
    private static final int MIN_PARTIAL_LENGTH = 2 * VISIBLE_CHARS;
    private static final int HASH_PREFIX_HEX_CHARS = 8;

    /**
     * Masks all but the last 4 characters. Values of 8 characters or fewer are fully masked.
     */
    public static String maskAccessKey(String value)
    {
        if (value == null) {
            return "null";
        }
        if (value.length() <= MIN_PARTIAL_LENGTH) {
            return "****";
        }
        return "****" + value.substring(value.length() - VISIBLE_CHARS);
    }

    /**
     * Keeps only the first and last 4 characters plus the length. Values of 8 characters or fewer are fully masked.
     */
    public static String maskSignature(String value)
    {
        if (value == null) {
            return "null";
        }
        if (value.length() <= MIN_PARTIAL_LENGTH) {
            return "<redacted,len=%s>".formatted(value.length());
        }
        return "%s...%s(len=%s)".formatted(value.substring(0, VISIBLE_CHARS), value.substring(value.length() - VISIBLE_CHARS), value.length());
    }

    /**
     * Never prints any part of the value; only its length and a short SHA-256 prefix so identical values can be correlated.
     */
    public static String redactWithHash(String value)
    {
        if (value == null) {
            return "null";
        }
        return "<redacted,len=%s,sha256=%s>".formatted(value.length(), sha256Prefix(value));
    }

    public static String redactWithHash(Optional<String> value)
    {
        if (value == null) {
            return "null";
        }
        return value.map(Redaction::redactWithHash).orElse("Optional.empty");
    }

    /**
     * Never prints any part of the value; only its length.
     */
    public static String redact(String value)
    {
        if (value == null) {
            return "null";
        }
        return "<redacted,len=%s>".formatted(value.length());
    }

    private static String sha256Prefix(String value)
    {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(UTF_8));
            return HexFormat.of().formatHex(digest).substring(0, HASH_PREFIX_HEX_CHARS);
        }
        catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    private Redaction() {}
}
