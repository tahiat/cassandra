/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.io.sstable.format;

import java.util.List;
import java.util.Set;

import com.google.common.base.CharMatcher;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.GaugeProvider;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.OutputHandler;

/**
 * Provides the accessors to data on disk.
 */
public interface SSTableFormat<R extends SSTableReader, W extends SSTableWriter>
{
    boolean enableSSTableDevelopmentTestMode = Boolean.getBoolean("cassandra.test.sstableformatdevelopment");

    Type getType();

    Version getLatestVersion();
    Version getVersion(String version);

    SSTableWriter.Factory getWriterFactory();
    SSTableReaderFactory<R, ?> getReaderFactory();

    /**
     * All the components that the writter can produce when saving an sstable, as well as all the components
     * that the reader can read.
     */
    Set<Component> allComponents();

    Set<Component> streamingComponents();

    Set<Component> primaryComponents();

    Set<Component> batchComponents();

    Set<Component> uploadComponents();

    Set<Component> mutableComponents();

    Set<Component> generatedOnLoadComponents();

    boolean cachesKeys();
    AbstractRowIndexEntry.KeyCacheValueSerializer<?, ?> getKeyCacheValueSerializer();

    /**
     * Returns a new scrubber for an sstable. Note that the transaction must contain only one reader
     * and the reader must match the provided cfs.
     */
    IScrubber getScrubber(ColumnFamilyStore cfs,
                          LifecycleTransaction transaction,
                          OutputHandler outputHandler,
                          IScrubber.Options options);

    R cast(SSTableReader sstr);

    W cast(SSTableWriter sstw);

    FormatSpecificMetricsProviders getFormatSpecificMetricsProviders();

    enum Type
    {
        //The original sstable format
        BIG("big", BigFormat.instance);

        public final SSTableFormat info;
        public final String name;

        public static Type current()
        {
            return BIG;
        }

        Type(String name, SSTableFormat info)
        {
            //Since format comes right after generation
            //we disallow formats with numeric names
            assert !CharMatcher.digit().matchesAllOf(name);

            this.name = name;
            this.info = info;
        }

        public static Type validate(String name)
        {
            for (Type valid : Type.values())
            {
                if (valid.name.equalsIgnoreCase(name))
                    return valid;
            }

            throw new IllegalArgumentException("No Type constant " + name);
        }
    }

    interface FormatSpecificMetricsProviders
    {
        List<GaugeProvider<?, ?>> getGaugeProviders();
    }

    interface SSTableReaderFactory<R extends SSTableReader, B extends SSTableReaderBuilder<R, B>>
    {
        /**
         * A simple builder which simply creates an instnace of {@link SSTableReader} with the provided parameters.
         * It expects that all the required resources will be opened/loaded externally by the caller.
         * <p>
         * The builder is expected to perform basic validation of the provided parameters.
         */
        SSTableReaderBuilder<R, B> builder(Descriptor descriptor);

        /**
         * A builder which opens/loads all the required resources upon execution of
         * {@link SSTableReaderLoadingBuilder#build(boolean, boolean)} and passed those resources to the created
         * instance of {@link SSTableReader}.
         * If the creation of {@link SSTableReader} fails, no resources should be left opened.
         * <p>
         * The builder is expected to perform basic validation of the provided parameters.
         */
        SSTableReaderLoadingBuilder<R, B> loadingBuilder(Descriptor descriptor, TableMetadataRef tableMetadataRef, Set<Component> components);
    }
}