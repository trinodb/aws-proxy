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
package io.trino.aws.proxy.spi.collections;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;

import static java.util.Objects.requireNonNull;

public class ImmutableMultiMap
        implements MultiMap
{
    private final boolean caseSensitiveKeys;
    private final ImmutableListMultimap<String, String> mapData;

    private ImmutableMultiMap(ImmutableListMultimap<String, String> data, boolean caseSensitiveKeys)
    {
        this.mapData = requireNonNull(data, "data is null");
        this.caseSensitiveKeys = caseSensitiveKeys;
    }

    public boolean isCaseSensitiveKeys()
    {
        return caseSensitiveKeys;
    }

    public Set<String> keySet()
    {
        return ImmutableSet.copyOf(mapData.keySet());
    }

    public Set<Map.Entry<String, List<String>>> entrySet()
    {
        return Multimaps.asMap(mapData).entrySet();
    }

    public List<String> get(String key)
    {
        return mapData.get(getActualKey(key));
    }

    public Optional<String> getFirst(String key)
    {
        List<String> values = get(key);
        if (!values.isEmpty()) {
            return Optional.of(values.getFirst());
        }
        return Optional.empty();
    }

    public void forEachEntry(BiConsumer<String, String> consumer)
    {
        mapData.forEach(consumer);
    }

    public void forEach(BiConsumer<String, List<String>> consumer)
    {
        Multimaps.asMap(mapData).forEach(consumer);
    }

    public boolean containsKey(String key)
    {
        return !get(key).isEmpty();
    }

    public static ImmutableMultiMap empty()
    {
        return builder(false).build();
    }

    public static Builder builder(boolean caseSensitiveKeys)
    {
        return new Builder(caseSensitiveKeys);
    }

    public static ImmutableMultiMap copyOf(Set<Map.Entry<String, List<String>>> entrySet)
    {
        return copyOf(entrySet, true);
    }

    public static ImmutableMultiMap copyOfCaseInsensitive(Set<Map.Entry<String, List<String>>> entrySet)
    {
        return copyOf(entrySet, false);
    }

    public static ImmutableMultiMap copyOf(MultiMap data)
    {
        return copyOf(data, true);
    }

    public static ImmutableMultiMap copyOfCaseInsensitive(MultiMap data)
    {
        return copyOf(data, false);
    }

    private static ImmutableMultiMap copyOf(MultiMap data, boolean caseSensitiveKeys)
    {
        if (data instanceof ImmutableMultiMap immutableData && caseSensitiveKeys == data.isCaseSensitiveKeys()) {
            return immutableData;
        }
        return copyOf(data.entrySet(), data.isCaseSensitiveKeys());
    }

    private String getActualKey(String key)
    {
        return getActualKey(key, isCaseSensitiveKeys());
    }

    private static String getActualKey(String key, boolean caseSensitiveKeys)
    {
        requireNonNull(key, "key is null");
        return caseSensitiveKeys ? key : key.toLowerCase(Locale.ROOT);
    }

    private static ImmutableMultiMap copyOf(Set<? extends Map.Entry<String, ? extends Collection<String>>> entrySet, boolean caseSensitiveKeys)
    {
        Builder builder = builder(caseSensitiveKeys);
        entrySet.forEach(entry -> builder.addAll(entry.getKey(), entry.getValue()));
        return builder.build();
    }

    public static class Builder
    {
        private final Multimap<String, String> data;
        private final boolean caseSensitiveKeys;

        private Builder(boolean caseSensitiveKeys)
        {
            this.data = LinkedListMultimap.create();
            this.caseSensitiveKeys = caseSensitiveKeys;
        }

        /**
         * Put a single entry under the provided key, replacing all values already present if any
         *
         * @throws NullPointerException if key is null
         */
        public Builder putOrReplaceSingle(String key, String value)
        {
            data.replaceValues(getActualKey(key, caseSensitiveKeys), ImmutableList.of(value));
            return this;
        }

        /**
         * Add an entry under the provided key
         *
         * @throws NullPointerException if key is null
         */
        public Builder add(String key, String value)
        {
            data.put(getActualKey(key, caseSensitiveKeys), value);
            return this;
        }

        /**
         * Add all values under the provided key. No-op if the provided collection has no values
         *
         * @throws NullPointerException if key is null
         */
        public Builder addAll(String key, Collection<String> values)
        {
            data.putAll(getActualKey(key, caseSensitiveKeys), values);
            return this;
        }

        public ImmutableMultiMap build()
        {
            return new ImmutableMultiMap(ImmutableListMultimap.copyOf(data), caseSensitiveKeys);
        }
    }
}
