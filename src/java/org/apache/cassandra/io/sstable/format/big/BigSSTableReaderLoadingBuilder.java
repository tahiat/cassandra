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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Downsampling;
import org.apache.cassandra.io.sstable.IndexSummary;
import org.apache.cassandra.io.sstable.IndexSummaryBuilder;
import org.apache.cassandra.io.sstable.KeyReader;
import org.apache.cassandra.io.sstable.SSTableBuilder;
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
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class BigSSTableReaderLoadingBuilder extends SSTableReaderLoadingBuilder<BigTableReader, BigTableReaderBuilder>
{
    private final static Logger logger = LoggerFactory.getLogger(BigSSTableReaderLoadingBuilder.class);

    private FileHandle.Builder dataFileBuilder;
    private FileHandle.Builder indexFileBuilder;

    public BigSSTableReaderLoadingBuilder(SSTableBuilder<?, ?> descriptor)
    {
        super(descriptor);
    }

    @Override
    protected void openComponents(BigTableReaderBuilder builder, boolean validate, boolean online) throws IOException
    {
        try
        {
            StatsComponent statsComponent = StatsComponent.load(descriptor, MetadataType.STATS, MetadataType.HEADER, MetadataType.VALIDATION);
            builder.setSerializationHeader(statsComponent.serializationHeader(builder.getTableMetadataRef().getLocal()));
            checkArgument(!online || builder.getSerializationHeader() != null);

            builder.setStatsMetadata(statsComponent.statsMetadata());
            ValidationMetadata validationMetadata = statsComponent.validationMetadata();
            validatePartitioner(builder.getTableMetadataRef().getLocal(), validationMetadata);

            boolean filterNeeded = online;
            if (filterNeeded)
                builder.setFilter(loadFilter(validationMetadata));
            boolean rebuildFilter = filterNeeded && builder.getFilter() == null;

            boolean summaryNeeded = true;
            if (summaryNeeded)
            {
                IndexSummaryComponent summaryComponent = loadSummary();
                if (summaryComponent != null)
                {
                    builder.setFirst(summaryComponent.first);
                    builder.setLast(summaryComponent.last);
                    builder.setIndexSummary(summaryComponent.indexSummary);
                }
            }
            boolean rebuildSummary = summaryNeeded && builder.getIndexSummary() == null;

            if (builder.getComponents().contains(Component.PRIMARY_INDEX) && (rebuildFilter || rebuildSummary))
            {
                try (FileHandle indexFile = indexFileBuilder(builder.getIndexSummary()).complete())
                {
                    Pair<IFilter, IndexSummaryComponent> filterAndSummary = buildSummaryAndBloomFilter(indexFile, builder.getSerializationHeader(), rebuildFilter, rebuildSummary);
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
            }

            builder.setDataFile(dataFileBuilder(builder.getStatsMetadata()).complete());

            if (builder.getFilter() == null)
                builder.setFilter(FilterFactory.AlwaysPresent);

            if (builder.getComponents().contains(Component.PRIMARY_INDEX))
                builder.setIndexFile(indexFileBuilder(builder.getIndexSummary()).complete());
        }
        catch (IOException | RuntimeException | Error ex)
        {
            Throwables.closeAndAddSuppressed(ex, builder.getDataFile(), builder.getIndexFile(), builder.getFilter(), builder.getIndexSummary());
            throw ex;
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
     * @return a pair of created filter and index summary component (or nulls if some of them were not created)
     */
    private Pair<IFilter, IndexSummaryComponent> buildSummaryAndBloomFilter(FileHandle indexFile,
                                                                            SerializationHeader serializationHeader,
                                                                            boolean rebuildFilter,
                                                                            boolean rebuildSummary) throws IOException
    {
        checkNotNull(indexFile);
        checkNotNull(serializationHeader);

        DecoratedKey first = null, key = null;
        IFilter bf = null;
        IndexSummary indexSummary = null;

        // we read the positions in a BRAF, so we don't have to worry about an entry spanning a mmap boundary.
        try (KeyReader keyReader = createKeyReader(indexFile, serializationHeader))
        {
            long estimatedRowsNumber = rebuildFilter || rebuildSummary ? estimateRowsFromIndex(indexFile) : 0;

            if (rebuildFilter)
                bf = FilterFactory.getFilter(estimatedRowsNumber, tableMetadataRef.getLocal().params.bloomFilterFpChance);

            try (IndexSummaryBuilder summaryBuilder = !rebuildSummary ? null : new IndexSummaryBuilder(estimatedRowsNumber,
                                                                                                       tableMetadataRef.getLocal().params.minIndexInterval,
                                                                                                       Downsampling.BASE_SAMPLING_LEVEL))
            {
                while (!keyReader.isExhausted())
                {
                    key = tableMetadataRef.getLocal().partitioner.decorateKey(keyReader.key());
                    if (rebuildSummary)
                    {
                        if (first == null)
                            first = key;
                        summaryBuilder.maybeAddEntry(key, keyReader.keyPositionForSecondaryIndex());
                    }

                    if (rebuildFilter)
                        bf.add(key);

                    keyReader.advance();
                }

                if (rebuildSummary)
                    indexSummary = summaryBuilder.build(tableMetadataRef.getLocal().partitioner);
            }
        }
        catch (IOException | RuntimeException | Error ex)
        {
            Throwables.closeAndAddSuppressed(ex, indexSummary, bf);
            throw ex;
        }

        return Pair.create(bf, rebuildSummary ? new IndexSummaryComponent(indexSummary, first, key) : null);
    }

    private IFilter loadFilter(ValidationMetadata validationMetadata)
    {
        return FilterComponent.maybeLoadBloomFilter(descriptor,
                                                    components,
                                                    tableMetadataRef.getLocal(),
                                                    validationMetadata);
    }

    /**
     * Load index summary, first key and last key from Summary.db file if it exists.
     * <p>
     * if loaded index summary has different index interval from current value stored in schema,
     * then Summary.db file will be deleted and need to be rebuilt.
     */
    private IndexSummaryComponent loadSummary()
    {
        IndexSummaryComponent summaryComponent = null;
        try
        {
            if (components.contains(Component.SUMMARY))
                summaryComponent = IndexSummaryComponent.loadOrDeleteCorrupted(descriptor, tableMetadataRef.get());

            if (summaryComponent == null)
                logger.debug("Index summary file is missing: {}", descriptor.filenameFor(Component.SUMMARY));
        }
        catch (IOException ex)
        {
            logger.debug("Index summary file is corrupted: " + descriptor.filenameFor(Component.SUMMARY), ex);
        }

        return summaryComponent;
    }

    /**
     * @return An estimate of the number of keys contained in the given index file.
     */
    public long estimateRowsFromIndex(FileHandle indexFile) throws IOException
    {
        checkNotNull(indexFile);

        try (RandomAccessReader indexReader = indexFile.createReader())
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

        long recordSize = statsMetadata.estimatedPartitionSize.percentile(ioOptions.diskOptimizationEstimatePercentile);
        int bufferSize = ioOptions.diskOptimizationStrategy.bufferSize(recordSize);

        if (dataFileBuilder == null)
            dataFileBuilder = new FileHandle.Builder(descriptor.fileFor(Component.DATA));

        dataFileBuilder.bufferSize(bufferSize);
        dataFileBuilder.withChunkCache(chunkCache);
        dataFileBuilder.mmapped(ioOptions.defaultDiskAccessMode);
        if (components.contains(Component.COMPRESSION_INFO))
            dataFileBuilder.withCompressionMetadata(CompressionInfoComponent.load(descriptor));

        return dataFileBuilder;
    }

    private FileHandle.Builder indexFileBuilder(IndexSummary indexSummary)
    {
        assert this.indexFileBuilder == null || this.indexFileBuilder.file.equals(descriptor.fileFor(Component.PRIMARY_INDEX));

        long indexFileLength = descriptor.fileFor(Component.PRIMARY_INDEX).length();
        OptionalInt indexBufferSize = indexSummary != null ? OptionalInt.of(ioOptions.diskOptimizationStrategy.bufferSize(indexFileLength / indexSummary.size()))
                                                           : OptionalInt.empty();

        if (indexFileBuilder == null)
            indexFileBuilder = new FileHandle.Builder(descriptor.fileFor(Component.PRIMARY_INDEX)).bufferSize(indexBufferSize.orElse(DiskOptimizationStrategy.MAX_BUFFER_SIZE));

        indexBufferSize.ifPresent(indexFileBuilder::bufferSize);
        indexFileBuilder.withChunkCache(chunkCache);
        indexFileBuilder.mmapped(ioOptions.indexDiskAccessMode);

        return indexFileBuilder;
    }
}