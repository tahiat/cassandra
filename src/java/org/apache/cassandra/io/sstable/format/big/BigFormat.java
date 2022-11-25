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
package org.apache.cassandra.io.sstable.format.big;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.UnknownColumnException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.GaugeProvider;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.*;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Legacy bigtable format
 */
public class BigFormat implements SSTableFormat<BigTableReader, BigTableWriter>
{
    public static final BigFormat instance = new BigFormat();
    public static final Version latestVersion = new BigVersion(BigVersion.current_version);
    private static final SSTableReader.Factory readerFactory = new ReaderFactory();
    private static final SSTableWriter.Factory writerFactory = new WriterFactory();

    private static final Set<Component> STREAM_COMPONENTS = ImmutableSet.of(Component.DATA,
                                                                            Component.PRIMARY_INDEX,
                                                                            Component.STATS,
                                                                            Component.COMPRESSION_INFO,
                                                                            Component.FILTER,
                                                                            Component.SUMMARY,
                                                                            Component.DIGEST,
                                                                            Component.CRC);

    private static final Set<Component> PRIMARY_COMPONENTS = ImmutableSet.of(Component.DATA,
                                                                             Component.PRIMARY_INDEX);

    private static final Set<Component> BATCH_COMPONENTS = ImmutableSet.of(Component.DATA,
                                                                           Component.PRIMARY_INDEX,
                                                                           Component.COMPRESSION_INFO,
                                                                           Component.FILTER,
                                                                           Component.STATS);

    private static final Set<Component> UPLOAD_COMPONENTS = ImmutableSet.of(Component.DATA,
                                                                            Component.PRIMARY_INDEX,
                                                                            Component.SUMMARY,
                                                                            Component.COMPRESSION_INFO,
                                                                            Component.STATS);

    private static final Set<Component> MUTABLE_COMPONENTS = ImmutableSet.of(Component.STATS,
                                                                             Component.SUMMARY);


    private BigFormat()
    {

    }

    @Override
    public Type getType()
    {
        return Type.BIG;
    }

    @Override
    public Version getLatestVersion()
    {
        return latestVersion;
    }

    @Override
    public Version getVersion(String version)
    {
        return new BigVersion(version);
    }

    @Override
    public SSTableWriter.Factory getWriterFactory()
    {
        return writerFactory;
    }

    @Override
    public SSTableReader.Factory getReaderFactory()
    {
        return readerFactory;
    }

    @Override
    public AbstractRowIndexEntry.KeyCacheValueSerializer<BigTableReader, RowIndexEntry> getKeyCacheValueSerializer()
    {
        return KeyCacheValueSerializer.instance;
    }

    @Override
    public BigTableReader cast(SSTableReader sstr)
    {
        return sstr == null ? null : (BigTableReader) sstr;
    }

    @Override
    public BigTableWriter cast(SSTableWriter sstw)
    {
        return sstw == null ? null : (BigTableWriter) sstw;
    }

    @Override
    public FormatSpecificMetricsProviders getFormatSpecificMetricsProviders()
    {
        return BigTableSpecificMetricsProviders.instance;
    }

    @Override
    public Set<Component> streamingComponents()
    {
        return STREAM_COMPONENTS;
    }

    @Override
    public Set<Component> primaryComponents()
    {
        return PRIMARY_COMPONENTS;
    }

    @Override
    public Set<Component> batchComponents()
    {
        return BATCH_COMPONENTS;
    }

    @Override
    public Set<Component> uploadComponents()
    {
        return UPLOAD_COMPONENTS;
    }

    @Override
    public Set<Component> mutableComponents()
    {
        return MUTABLE_COMPONENTS;
    }

    static class KeyCacheValueSerializer implements AbstractRowIndexEntry.KeyCacheValueSerializer<BigTableReader, RowIndexEntry>
    {
        private final static KeyCacheValueSerializer instance = new KeyCacheValueSerializer();

        @Override
        public void skip(DataInputPlus input) throws IOException
        {
            RowIndexEntry.Serializer.skipForCache(input);
        }

        @Override
        public RowIndexEntry deserialize(BigTableReader reader, DataInputPlus input) throws IOException
        {
            return reader.deserializeKeyCacheValue(input);
        }

        @Override
        public void serialize(RowIndexEntry entry, DataOutputPlus output) throws IOException
        {
            entry.serializeForCache(output);
        }
    }

    static class WriterFactory extends SSTableWriter.Factory
    {
        @Override
        public long estimateSize(SSTableWriter.SSTableSizeParameters parameters)
        {
            return (long) ((parameters.partitionKeysSize() // index entries
                            + parameters.partitionKeysSize() // keys in data file
                            + parameters.dataSize()) // data
                           * 1.2); // bloom filter and row index overhead
        }

        @Override
        public SSTableWriter open(Descriptor descriptor,
                                  long keyCount,
                                  long repairedAt,
                                  TimeUUID pendingRepair,
                                  boolean isTransient,
                                  TableMetadataRef metadata,
                                  MetadataCollector metadataCollector,
                                  SerializationHeader header,
                                  Collection<SSTableFlushObserver> observers,
                                  LifecycleNewTracker lifecycleNewTracker)
        {
            SSTable.validateRepairedMetadata(repairedAt, pendingRepair, isTransient);
            return new BigTableWriter(descriptor, keyCount, repairedAt, pendingRepair, isTransient, metadata, metadataCollector, header, observers, lifecycleNewTracker);
        }
    }

    static class ReaderFactory implements SSTableReader.Factory<BigTableReader>
    {
        private static final Logger logger = LoggerFactory.getLogger(ReaderFactory.class);

        @Override
        public BigTableReader open(SSTableReaderBuilder builder)
        {
            return new BigTableReader(builder);
        }

        @Override
        public BigTableReader open(Descriptor descriptor, Set<Component> components, TableMetadataRef metadata, boolean validate, boolean isOffline)
        {
            // Minimum components without which we can't do anything
            assert components.contains(Component.DATA) : "Data component is missing for sstable " + descriptor;
            assert !validate || components.contains(Component.PRIMARY_INDEX) : "Primary index component is missing for sstable " + descriptor;

            // For the 3.0+ sstable format, the (misnomed) stats component hold the serialization header which we need to deserialize the sstable content
            assert components.contains(Component.STATS) : "Stats component is missing for sstable " + descriptor;

            descriptor.verifyCompressionInfoExistenceIfApplicable(components);

            EnumSet<MetadataType> types = EnumSet.of(MetadataType.VALIDATION, MetadataType.STATS, MetadataType.HEADER);

            Map<MetadataType, MetadataComponent> sstableMetadata;
            try
            {
                sstableMetadata = descriptor.getMetadataSerializer().deserialize(descriptor, types);
            }
            catch (Throwable t)
            {
                throw new CorruptSSTableException(t, descriptor.filenameFor(Component.STATS));
            }
            ValidationMetadata validationMetadata = (ValidationMetadata) sstableMetadata.get(MetadataType.VALIDATION);
            StatsMetadata statsMetadata = (StatsMetadata) sstableMetadata.get(MetadataType.STATS);
            SerializationHeader.Component header = (SerializationHeader.Component) sstableMetadata.get(MetadataType.HEADER);
            assert header != null;

            // Check if sstable is created using same partitioner.
            // Partitioner can be null, which indicates older version of sstable or no stats available.
            // In that case, we skip the check.
            String partitionerName = metadata.get().partitioner.getClass().getCanonicalName();
            if (validationMetadata != null && !partitionerName.equals(validationMetadata.partitioner))
            {
                logger.error("Cannot open {}; partitioner {} does not match system partitioner {}.  Note that the default partitioner starting with Cassandra 1.2 is Murmur3Partitioner, so you will need to edit that to match your old partitioner if upgrading.",
                             descriptor, validationMetadata.partitioner, partitionerName);
                System.exit(1);
            }

            BigTableReader sstable;
            try
            {
                sstable = (BigTableReader) new SSTableReaderBuilder.ForRead(descriptor,
                                                                            metadata,
                                                                            validationMetadata,
                                                                            isOffline,
                                                                            components,
                                                                            statsMetadata,
                                                                            header.toHeader(metadata.get())).build();
            }
            catch (UnknownColumnException e)
            {
                throw new IllegalStateException(e);
            }

            try
            {
                if (validate)
                    sstable.validate();

                if (sstable.getKeyCache() != null)
                    logger.trace("key cache contains {}/{} keys", sstable.getKeyCache().size(), sstable.getKeyCache().getCapacity());

                return sstable;
            }
            catch (Throwable t)
            {
                sstable.selfRef().release();
                throw new CorruptSSTableException(t, sstable.getFilename());
            }
        }

        @Override
        public BigTableReader openForBatch(Descriptor descriptor, Set<Component> components, TableMetadataRef metadata)
        {
            // Minimum components without which we can't do anything
            assert components.contains(Component.DATA) : "Data component is missing for sstable " + descriptor;
            assert components.contains(Component.PRIMARY_INDEX) : "Primary index component is missing for sstable " + descriptor;
            descriptor.verifyCompressionInfoExistenceIfApplicable(components);

            EnumSet<MetadataType> types = EnumSet.of(MetadataType.VALIDATION, MetadataType.STATS, MetadataType.HEADER);
            Map<MetadataType, MetadataComponent> sstableMetadata;
            try
            {
                sstableMetadata = descriptor.getMetadataSerializer().deserialize(descriptor, types);
            }
            catch (IOException e)
            {
                throw new CorruptSSTableException(e, descriptor.filenameFor(Component.STATS));
            }

            ValidationMetadata validationMetadata = (ValidationMetadata) sstableMetadata.get(MetadataType.VALIDATION);
            StatsMetadata statsMetadata = (StatsMetadata) sstableMetadata.get(MetadataType.STATS);
            SerializationHeader.Component header = (SerializationHeader.Component) sstableMetadata.get(MetadataType.HEADER);

            // Check if sstable is created using same partitioner.
            // Partitioner can be null, which indicates older version of sstable or no stats available.
            // In that case, we skip the check.
            String partitionerName = metadata.get().partitioner.getClass().getCanonicalName();
            if (validationMetadata != null && !partitionerName.equals(validationMetadata.partitioner))
            {
                logger.error("Cannot open {}; partitioner {} does not match system partitioner {}.  Note that the default partitioner starting with Cassandra 1.2 is Murmur3Partitioner, so you will need to edit that to match your old partitioner if upgrading.",
                             descriptor, validationMetadata.partitioner, partitionerName);
                System.exit(1);
            }

            try
            {
                return (BigTableReader) new SSTableReaderBuilder.ForBatch(descriptor, metadata, components, statsMetadata, header.toHeader(metadata.get())).build();
            }
            catch (UnknownColumnException e)
            {
                throw new IllegalStateException(e);
            }
        }
    }

    // versions are denoted as [major][minor].  Minor versions must be forward-compatible:
    // new fields are allowed in e.g. the metadata component, but fields can't be removed
    // or have their size changed.
    //
    // Minor versions were introduced with version "hb" for Cassandra 1.0.3; prior to that,
    // we always incremented the major version.
    static class BigVersion extends Version
    {
        public static final String current_version = "nb";
        public static final String earliest_supported_version = "ma";

        // ma (3.0.0): swap bf hash order
        //             store rows natively
        // mb (3.0.7, 3.7): commit log lower bound included
        // mc (3.0.8, 3.9): commit log intervals included
        // md (3.0.18, 3.11.4): corrected sstable min/max clustering
        // me (3.0.25, 3.11.11): added hostId of the node from which the sstable originated

        // na (4.0-rc1): uncompressed chunks, pending repair session, isTransient, checksummed sstable metadata file, new Bloomfilter format
        // nb (4.0.0): originating host id
        //
        // NOTE: when adding a new version, please add that to LegacySSTableTest, too.

        private final boolean isLatestVersion;
        public final int correspondingMessagingVersion;
        private final boolean hasCommitLogLowerBound;
        private final boolean hasCommitLogIntervals;
        private final boolean hasAccurateMinMax;
        private final boolean hasOriginatingHostId;
        public final boolean hasMaxCompressedLength;
        private final boolean hasPendingRepair;
        private final boolean hasMetadataChecksum;
        private final boolean hasIsTransient;

        /**
         * CASSANDRA-9067: 4.0 bloom filter representation changed (two longs just swapped)
         * have no 'static' bits caused by using the same upper bits for both bloom filter and token distribution.
         */
        private final boolean hasOldBfFormat;

        BigVersion(String version)
        {
            super(instance, version);

            isLatestVersion = version.compareTo(current_version) == 0;
            correspondingMessagingVersion = MessagingService.VERSION_30;

            hasCommitLogLowerBound = version.compareTo("mb") >= 0;
            hasCommitLogIntervals = version.compareTo("mc") >= 0;
            hasAccurateMinMax = version.compareTo("md") >= 0;
            hasOriginatingHostId = version.matches("(m[e-z])|(n[b-z])");
            hasMaxCompressedLength = version.compareTo("na") >= 0;
            hasPendingRepair = version.compareTo("na") >= 0;
            hasIsTransient = version.compareTo("na") >= 0;
            hasMetadataChecksum = version.compareTo("na") >= 0;
            hasOldBfFormat = version.compareTo("na") < 0;
        }

        @Override
        public boolean isLatestVersion()
        {
            return isLatestVersion;
        }

        @Override
        public boolean hasCommitLogLowerBound()
        {
            return hasCommitLogLowerBound;
        }

        @Override
        public boolean hasCommitLogIntervals()
        {
            return hasCommitLogIntervals;
        }

        public boolean hasPendingRepair()
        {
            return hasPendingRepair;
        }

        @Override
        public boolean hasIsTransient()
        {
            return hasIsTransient;
        }

        @Override
        public int correspondingMessagingVersion()
        {
            return correspondingMessagingVersion;
        }

        @Override
        public boolean hasMetadataChecksum()
        {
            return hasMetadataChecksum;
        }

        @Override
        public boolean hasAccurateMinMax()
        {
            return hasAccurateMinMax;
        }

        public boolean isCompatible()
        {
            return version.compareTo(earliest_supported_version) >= 0 && version.charAt(0) <= current_version.charAt(0);
        }

        @Override
        public boolean isCompatibleForStreaming()
        {
            return isCompatible() && version.charAt(0) == current_version.charAt(0);
        }

        public boolean hasOriginatingHostId()
        {
            return hasOriginatingHostId;
        }

        @Override
        public boolean hasMaxCompressedLength()
        {
            return hasMaxCompressedLength;
        }

        @Override
        public boolean hasOldBfFormat()
        {
            return hasOldBfFormat;
        }
    }

    @Override
    public IScrubber getScrubber(ColumnFamilyStore cfs, LifecycleTransaction transaction, boolean skipCorrupted, OutputHandler outputHandler, boolean checkData, boolean reinsertOverflowedTTLRows)
    {
        Preconditions.checkArgument(transaction.onlyOne().descriptor.formatType == getType());
        return new Scrubber(cfs, transaction, skipCorrupted, outputHandler, checkData, reinsertOverflowedTTLRows);
    }

    private static class BigTableGaugeProvider<T extends Number> extends GaugeProvider<T, BigTableReader>
    {
        public BigTableGaugeProvider(String name, T neutralValue, Function<BigTableReader, T> extractor, BiFunction<T, T, T> combiner)
        {
            super(BigFormat.instance, name, neutralValue, extractor, combiner);
        }
    }

    private static class BigTableSpecificMetricsProviders implements FormatSpecificMetricsProviders
    {
        private final static BigTableSpecificMetricsProviders instance = new BigTableSpecificMetricsProviders();

        private final BigTableGaugeProvider<Long> indexSummaryOffHeapMemoryUsed = new BigTableGaugeProvider<>("IndexSummaryOffHeapMemoryUsed",
                                                                                                              0L,
                                                                                                              r -> r.getIndexSummary().getOffHeapSize(),
                                                                                                              Long::sum);

        private final List<GaugeProvider<?, ?>> gaugeProviders = Arrays.asList(indexSummaryOffHeapMemoryUsed);

        @Override
        public List<GaugeProvider<?, ?>> getGaugeProviders()
        {
            return gaugeProviders;
        }
    }
}
