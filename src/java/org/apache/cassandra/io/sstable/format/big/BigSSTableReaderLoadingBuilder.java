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
import java.util.OptionalInt;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.Downsampling;
import org.apache.cassandra.io.sstable.IndexSummary;
import org.apache.cassandra.io.sstable.IndexSummaryBuilder;
import org.apache.cassandra.io.sstable.KeyReader;
import org.apache.cassandra.io.sstable.format.CompressionInfoComponent;
import org.apache.cassandra.io.sstable.format.FilterComponent;
import org.apache.cassandra.io.sstable.format.SSTableReaderLoadingBuilder;
import org.apache.cassandra.io.sstable.format.StatsComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.io.util.DiskOptimizationStrategy;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.AlwaysPresentFilter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;

import static com.google.common.base.Preconditions.checkNotNull;

public class BigSSTableReaderLoadingBuilder extends SSTableReaderLoadingBuilder<BigTableReader, BigTableReaderBuilder>
{
    private final static Logger logger = LoggerFactory.getLogger(BigSSTableReaderLoadingBuilder.class);

    private final BigTableOptions options;

    private final ChunkCache chunkCache;

    private FileHandle.Builder dataFileBuilder;
    private FileHandle.Builder indexFileBuilder;

    public BigSSTableReaderLoadingBuilder(Descriptor descriptor,
                                          Set<Component> components,
                                          TableMetadataRef tableMetadataRef,
                                          BigTableOptions options,
                                          ChunkCache chunkCache)
    {
        super(descriptor, components, tableMetadataRef);
        this.options = options;
        this.chunkCache = chunkCache;
    }

    @Override
    protected void openComponents(BigTableReaderBuilder builder, boolean validate, boolean online) throws IOException
    {
        try
        {
            StatsComponent statsComponent = StatsComponent.load(descriptor, MetadataType.STATS, MetadataType.HEADER, MetadataType.VALIDATION);
            builder.setSerializationHeader(statsComponent.serializationHeader(builder.getTableMetadataRef().getLocal()));
            assert !online || builder.getSerializationHeader() != null;

            builder.setStatsMetadata(statsComponent.statsMetadata());
            ValidationMetadata validationMetadata = statsComponent.validationMetadata();
            validatePartitioner(builder.getTableMetadataRef().getLocal(), validationMetadata);

            boolean filterNeeded = online && builder.getComponents().contains(Component.FILTER);
            if (filterNeeded)
                builder.setFilter(loadFilter(builder, validationMetadata));
            boolean rebuildFilter = filterNeeded && builder.getFilter() == null;

            boolean summaryNeeded = builder.getComponents().contains(Component.SUMMARY);
            if (summaryNeeded)
            {
                IndexSummaryComponent summaryComponent = loadSummary(builder);
                builder.setFirst(summaryComponent.first);
                builder.setLast(summaryComponent.last);
                builder.setIndexSummary(summaryComponent.indexSummary);
            }
            boolean rebuildSummary = summaryNeeded && builder.getIndexSummary() == null;

            if (rebuildFilter || rebuildSummary)
            {
                Pair<IFilter, IndexSummaryComponent> filterAndSummary = buildSummaryAndBloomFilter(builder, rebuildFilter, rebuildSummary);
                IFilter filter = filterAndSummary.left;
                IndexSummaryComponent summaryComponent = filterAndSummary.right;

                if (summaryComponent != null)
                {
                    builder.setFirst(summaryComponent.first);
                    builder.setLast(summaryComponent.last);
                    builder.setIndexSummary(summaryComponent.indexSummary);

                    if (online)
                        summaryComponent.save(descriptor);
                }

                if (filter != null)
                {
                    builder.setFilter(filter);

                    if (online)
                        FilterComponent.save(filter, descriptor);
                }
            }

            assert !filterNeeded || builder.getFilter() != null;
            assert !summaryNeeded || builder.getIndexSummary() != null;

            if (builder.getFilter() == null)
                builder.setFilter(new AlwaysPresentFilter());

            builder.setDataFile(dataFileBuilder(builder.getStatsMetadata()).complete());
            builder.setIndexFile(indexFileBuilder(builder.getIndexSummary()).complete());
        }
        catch (IOException | RuntimeException | Error ex)
        {
            Throwables.closeAndAddSuppressed(ex, builder.getDataFile(), builder.getIndexFile(), builder.getFilter(), builder.getIndexSummary());
            throw ex;
        }
    }

    /**
     * Check if sstable is created using same partitioner.
     * Partitioner can be null, which indicates older version of sstable or no stats available.
     * In that case, we skip the check.
     */
    private void validatePartitioner(TableMetadata metadata, ValidationMetadata validationMetadata)
    {
        String partitionerName = metadata.partitioner.getClass().getCanonicalName();
        if (validationMetadata != null && !partitionerName.equals(validationMetadata.partitioner))
        {
            throw new CorruptSSTableException(new IOException(String.format("Cannot open %s; partitioner %s does not match system partitioner %s. " +
                                                                            "Note that the default partitioner starting with Cassandra 1.2 is Murmur3Partitioner, " +
                                                                            "so you will need to edit that to match your old partitioner if upgrading.",
                                                                            descriptor, validationMetadata.partitioner, partitionerName)),
                                              descriptor.filenameFor(Component.STATS));
        }
    }

    @Override
    public KeyReader buildKeyReader() throws IOException
    {
        StatsComponent statsComponent = StatsComponent.load(descriptor, MetadataType.STATS, MetadataType.HEADER, MetadataType.VALIDATION);
        SerializationHeader header = statsComponent.serializationHeader(tableMetadataRef.getLocal());
        try (FileHandle indexFile = indexFileBuilder(null).complete())
        {
            return createKeyReader(indexFile, header);
        }
    }

    private KeyReader createKeyReader(FileHandle indexFile, SerializationHeader serializationHeader) throws IOException
    {
        checkNotNull(indexFile);
        checkNotNull(serializationHeader);

        RowIndexEntry.IndexSerializer serializer = new RowIndexEntry.Serializer(descriptor.version, serializationHeader);
        return BigTableKeyReader.create(indexFile, serializer);
    }

    /**
     * Go through the index and optionally rebuild the index summary and Bloom filter.
     *
     * @param rebuildFilter  true if Bloom filter should be rebuilt
     * @param rebuildSummary true if index summary, first and last keys should be rebuilt
     * @return
     */
    private Pair<IFilter, IndexSummaryComponent> buildSummaryAndBloomFilter(BigTableReaderBuilder builder, boolean rebuildFilter, boolean rebuildSummary) throws IOException
    {
        checkNotNull(builder.getIndexFile());
        checkNotNull(builder.getSerializationHeader());

        DecoratedKey first = null, key = null;
        IFilter bf = null;
        IndexSummary indexSummary = null;

        // we read the positions in a BRAF, so we don't have to worry about an entry spanning a mmap boundary.
        try (KeyReader keyReader = createKeyReader(builder.getIndexFile(), builder.getSerializationHeader()))
        {
            long estimatedRowsNumber = rebuildFilter || rebuildSummary ? estimateRowsFromIndex(builder) : 0;

            if (rebuildFilter)
                bf = FilterFactory.getFilter(estimatedRowsNumber, builder.getTableMetadataRef().get().params.bloomFilterFpChance);

            try (IndexSummaryBuilder summaryBuilder = !rebuildSummary ? null : new IndexSummaryBuilder(estimatedRowsNumber,
                                                                                                       builder.getTableMetadataRef().get().params.minIndexInterval,
                                                                                                       Downsampling.BASE_SAMPLING_LEVEL))
            {
                while (!keyReader.isExhausted())
                {
                    key = builder.getTableMetadataRef().get().partitioner.decorateKey(keyReader.key());
                    if (rebuildSummary)
                    {
                        if (first == null)
                            first = key;
                        summaryBuilder.maybeAddEntry(key, keyReader.keyPositionForSecondaryIndex());
                    }

                    if (rebuildFilter)
                        bf.add(key);
                }

                if (rebuildSummary)
                    indexSummary = summaryBuilder.build(builder.getTableMetadataRef().get().partitioner);
            }
        }
        catch (IOException | RuntimeException | Error ex)
        {
            Throwables.closeAndAddSuppressed(ex, indexSummary, bf);
            throw ex;
        }

        return Pair.create(bf, new IndexSummaryComponent(indexSummary, first, key));
    }

    private IFilter loadFilter(BigTableReaderBuilder builder, ValidationMetadata validationMetadata)
    {
        return FilterComponent.maybeLoadBloomFilter(descriptor,
                                                    builder.getComponents(),
                                                    builder.getTableMetadataRef().get(),
                                                    validationMetadata);
    }

    /**
     * Load index summary, first key and last key from Summary.db file if it exists.
     * <p>
     * if loaded index summary has different index interval from current value stored in schema,
     * then Summary.db file will be deleted and need to be rebuilt.
     */
    private IndexSummaryComponent loadSummary(BigTableReaderBuilder builder)
    {
        IndexSummaryComponent summaryComponent = null;
        try
        {
            summaryComponent = IndexSummaryComponent.loadOrDeleteCorrupted(descriptor, builder.getTableMetadataRef().get());
            if (summaryComponent == null)
            {
                logger.debug("Index summary file is missing: {}", descriptor.filenameFor(Component.SUMMARY));
            }
        }
        catch (IOException ex)
        {
            logger.debug("Index summary file is corrupted: " + descriptor.filenameFor(Component.SUMMARY), ex);
        }

        return summaryComponent;
    }

    private long calculateEstimatedKeys(BigTableReaderBuilder builder) throws IOException
    {
        checkNotNull(builder.getStatsMetadata());

        StatsMetadata statsMetadata = builder.getStatsMetadata();
        if (statsMetadata.totalRows > 0)
            return statsMetadata.totalRows;
        long histogramCount = statsMetadata.estimatedPartitionSize.count();
        return histogramCount > 0 && !statsMetadata.estimatedPartitionSize.isOverflowed() ? histogramCount : estimateRowsFromIndex(builder);
    }

    /**
     * @return An estimate of the number of keys contained in the given index file.
     */
    public long estimateRowsFromIndex(BigTableReaderBuilder builder) throws IOException
    {
        checkNotNull(builder.getIndexFile());

        try (RandomAccessReader indexReader = builder.getIndexFile().createReader())
        {
            // collect sizes for the first 10000 keys, or first 10 mebibytes of data
            final int samplesCap = 10000;
            final int bytesCap = (int) Math.min(10000000, indexReader.length());
            int keys = 0;
            while (indexReader.getFilePointer() < bytesCap && keys < samplesCap)
            {
                ByteBufferUtil.skipShortLength(indexReader);
                RowIndexEntry.Serializer.skip(indexReader, descriptor.version);
                keys++;
            }
            assert keys > 0 && indexReader.getFilePointer() > 0 && indexReader.length() > 0 : "Unexpected empty index file: " + indexReader;
            long estimatedRows = indexReader.length() / (indexReader.getFilePointer() / keys);
            indexReader.seek(0);
            return estimatedRows;
        }
    }

    private FileHandle.Builder dataFileBuilder(StatsMetadata statsMetadata)
    {
        assert this.dataFileBuilder == null || this.dataFileBuilder.file.equals(descriptor.fileFor(Component.DATA));

        logger.info("Opening {} ({})", descriptor, FBUtilities.prettyPrintMemory(descriptor.fileFor(Component.DATA).length()));

        long recordSize = statsMetadata.estimatedPartitionSize.percentile(options.diskOptimizationEstimatePercentile);
        int bufferSize = options.diskOptimizationStrategy.bufferSize(recordSize);

        if (dataFileBuilder == null)
            dataFileBuilder = new FileHandle.Builder(descriptor.fileFor(Component.DATA));

        dataFileBuilder.bufferSize(bufferSize);
        dataFileBuilder.withChunkCache(chunkCache);
        dataFileBuilder.mmapped(options.defaultDiskAccessMode);
        if (components.contains(Component.COMPRESSION_INFO))
            dataFileBuilder.withCompressionMetadata(CompressionInfoComponent.load(descriptor));

        return dataFileBuilder;
    }

    private FileHandle.Builder indexFileBuilder(IndexSummary indexSummary)
    {
        assert this.indexFileBuilder == null || this.indexFileBuilder.file.equals(descriptor.fileFor(Component.PRIMARY_INDEX));

        long indexFileLength = descriptor.fileFor(Component.PRIMARY_INDEX).length();
        OptionalInt indexBufferSize = indexSummary != null ? OptionalInt.of(options.diskOptimizationStrategy.bufferSize(indexFileLength / indexSummary.size()))
                                                           : OptionalInt.empty();

        if (indexFileBuilder == null)
            indexFileBuilder = new FileHandle.Builder(descriptor.fileFor(Component.PRIMARY_INDEX)).bufferSize(indexBufferSize.orElse(DiskOptimizationStrategy.MAX_BUFFER_SIZE));

        indexBufferSize.ifPresent(indexFileBuilder::bufferSize);
        indexFileBuilder.withChunkCache(chunkCache);
        indexFileBuilder.mmapped(options.indexDiskAccessMode);

        return indexFileBuilder;
    }
}


