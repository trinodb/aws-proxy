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
package io.trino.s3.proxy.server.collections;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * A map between keys and values, each entry may have one or more values
 * Null keys are not supported.
 * The map may be configured to be case-insensitive at construction time, in which case
 * all keys are lowercased.
 */
public interface MultiMap
{
    /**
     * Check whether this map has case-sensitive keys
     */
    boolean isCaseSensitiveKeys();

    /**
     * Obtain a copy of the keys of the data in the map
     *
     * @return Immutable copy of the keys of the map
     */
    Set<String> keySet();

    /**
     * Obtain the set of entries in the map
     */
    Set<Map.Entry<String, List<String>>> entrySet();

    /**
     * Get all values associated to a given key in the map.
     * Returns an empty list if the key is not present in the map.
     *
     * @param key key
     * @return list of values, empty list if key not present
     * @throws NullPointerException if key is null
     */
    List<String> get(String key);

    /**
     * Obtain the first value associated with a key, if any
     *
     * @return first value associated with the key, empty otherwise
     * @throws NullPointerException if key is null
     */
    Optional<String> getFirst(String key);

    /**
     * Process each entry in the map using the provided consumer
     * The consumer will be called once per key/value pair, meaning it may be called multiple times
     * with the same key
     *
     * @see MultiMap#forEach to process all values associated with a key together
     * @param consumer function to be executed for each entry
     */
    void forEachEntry(BiConsumer<String, String> consumer);

    /**
     * Process each unique key in the map along with all its associated values using the provided consumer
     *
     * @see MultiMap#forEachEntry to process each key-value pair individually
     * @param consumer function to be executed for each key + list of values
     */
    void forEach(BiConsumer<String, List<String>> consumer);

    /**
     * Determine whether a given key as at least one value associated with it in the map
     *
     * @throws NullPointerException if key is null
     */
    boolean containsKey(String key);
}
