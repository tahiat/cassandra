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
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.Downsampling;
import org.apache.cassandra.io.sstable.IndexSummary;
import org.apache.cassandra.io.sstable.IndexSummaryBuilder;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.CompressionInfoComponent;
import org.apache.cassandra.io.sstable.format.FilterComponent;
import org.apache.cassandra.io.sstable.format.SSTableReaderBuilder;
import org.apache.cassandra.io.sstable.format.StatsComponent;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.AlwaysPresentFilter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.Throwables;

public class BigTableReaderBuilder extends SSTableReaderBuilder<BigTableReader, BigTableReaderBuilder>
{
    private static final Logger logger = LoggerFactory.getLogger(BigTableReaderBuilder.class);

    private IndexSummary indexSummary;
    private FileHandle indexFile;
    private DiskAccessMode indexFileAccessMode = DatabaseDescriptor.getIndexAccessMode();

    public BigTableReaderBuilder(Descriptor descriptor)
    {
        super(descriptor);
    }

    public BigTableReaderBuilder setIndexFile(FileHandle indexFile)
    {
        this.indexFile = indexFile;
        return this;
    }

    public BigTableReaderBuilder setIndexSummary(IndexSummary indexSummary)
    {
        this.indexSummary = indexSummary;
        return this;
    }

    public BigTableReaderBuilder setIndexFileAccessMode(DiskAccessMode indexFileAccessMode)
    {
        this.indexFileAccessMode = indexFileAccessMode;
        return this;
    }

    public IndexSummary getIndexSummary()
    {
        return indexSummary;
    }

    public FileHandle getIndexFile()
    {
        return indexFile;
    }

    public DiskAccessMode getIndexFileAccessMode()
    {
        return indexFileAccessMode;
    }

    @Override
    protected void openComponents() throws IOException
    {
        try
        {
            StatsComponent statsComponent = StatsComponent.load(descriptor);
            setStatsMetadata(statsComponent.statsMetadata());
            setSerializationHeader(statsComponent.serializationHeader(getTableMetadataRef().get()));

            validatePartitioner(statsComponent);

            assert !isOnline() || getSerializationHeader() != null;

            boolean filterNeeded = isOnline() && getComponents().contains(Component.FILTER);
            if (filterNeeded)
                loadFilter(statsComponent);
            boolean rebuildFilter = filterNeeded && getFilter() == null;

            boolean summaryNeeded = getComponents().contains(Component.SUMMARY);
            if (summaryNeeded)
                loadSummary();
            boolean rebuildSummary = summaryNeeded && getIndexSummary() == null;

            if (rebuildFilter || rebuildSummary)
            {
                buildSummaryAndBloomFilter(rebuildFilter, rebuildSummary, this);

                if (isOnline())
                {
                    if (rebuildSummary)
                        new IndexSummaryComponent(getIndexSummary(), getFirst(), getLast()).save(descriptor);
                    if (rebuildFilter)
                        FilterComponent.save(getFilter(), descriptor);
                }
            }

            assert !filterNeeded || getFilter() != null;
            assert !summaryNeeded || getIndexSummary() != null;

            if (getFilter() == null)
                setFilter(new AlwaysPresentFilter());

            setupDataFile();
            setupIndexFile();
        }
        catch (IOException | RuntimeException | Error ex)
        {
            Throwable additionalExceptions = Throwables.close(null, Arrays.asList(getFilter(), getIndexSummary(), getDataFile(), getIndexFile()));
            if (additionalExceptions != null)
                ex.addSuppressed(additionalExceptions);
            throw ex;
        }
    }

    @Override
    public BigTableReader build()
    {
        BigTableReader reader = new BigTableReader(this);

        if (isSuspected())
            reader.markSuspect();

        reader.setup(isOnline());

        return reader;
    }

    /**
     * Go through the index and optionally rebuild the index summary and Bloom filter.
     *
     * @param rebuildFilter  true if Bloom filter should be rebuilt
     * @param rebuildSummary true if index summary, first and last keys should be rebuilt
     */
    private void buildSummaryAndBloomFilter(boolean rebuildFilter, boolean rebuildSummary, BigTableReaderBuilder builder) throws IOException
    {
        if (!builder.getComponents().contains(Component.PRIMARY_INDEX)) return;

        if (logger.isDebugEnabled()) logger.debug("Attempting to build index summary for {}", builder.descriptor);

        DecoratedKey first = null;
        DecoratedKey last = null;
        IFilter bf = null;

        // we read the positions in a BRAF so we don't have to worry about an entry spanning a mmap boundary.
        try (RandomAccessReader primaryIndex = RandomAccessReader.open(builder.descriptor.fileFor(Component.PRIMARY_INDEX)))
        {
            long indexSize = primaryIndex.length();
            long estimatedKeys = calculateEstimatedKeys(primaryIndex);

            if (rebuildFilter)
                bf = FilterFactory.getFilter(estimatedKeys, builder.getTableMetadataRef().get().params.bloomFilterFpChance);

            try (IndexSummaryBuilder summaryBuilder = !rebuildSummary ? null : new IndexSummaryBuilder(estimatedKeys, builder.getTableMetadataRef().get().params.minIndexInterval, Downsampling.BASE_SAMPLING_LEVEL))
            {
                long indexPosition;

                while ((indexPosition = primaryIndex.getFilePointer()) != indexSize)
                {
                    ByteBuffer key = ByteBufferUtil.readWithShortLength(primaryIndex);
                    RowIndexEntry.Serializer.skip(primaryIndex, builder.descriptor.version);
                    DecoratedKey decoratedKey = builder.getTableMetadataRef().get().partitioner.decorateKey(key);

                    if (rebuildSummary)
                    {
                        if (first == null) first = decoratedKey;
                        last = decoratedKey;
                        summaryBuilder.maybeAddEntry(decoratedKey, indexPosition);
                    }

                    if (rebuildFilter) bf.add(decoratedKey);
                }

                if (rebuildSummary)
                    builder.setIndexSummary(summaryBuilder.build(builder.getTableMetadataRef().get().partitioner));
            }
        }

        if (rebuildSummary)
        {
            builder.setFirst(first);
            builder.setLast(last);
        }
    }

    private void loadFilter(StatsComponent statsComponent)
    {
        IFilter filter = FilterComponent.maybeLoadBloomFilter(descriptor,
                                                              getComponents(),
                                                              getTableMetadataRef().get(),
                                                              statsComponent != null ? statsComponent.validationMetadata() : null);
        setFilter(filter);
    }

    /**
     * Load index summary, first key and last key from Summary.db file if it exists.
     * <p>
     * if loaded index summary has different index interval from current value stored in schema,
     * then Summary.db file will be deleted and need to be rebuilt.
     */
    private void loadSummary()
    {
        try
        {
            IndexSummaryComponent summaryComponent = IndexSummaryComponent.loadOrDeleteCorrupted(descriptor, getTableMetadataRef().get());
            if (summaryComponent != null)
            {
                setFirst(summaryComponent.first);
                setLast(summaryComponent.last);
                setIndexSummary(summaryComponent.indexSummary);
            }
            else
            {
                logger.debug("Index summary file is missing: {}", descriptor.filenameFor(Component.SUMMARY));
            }
        }
        catch (IOException ex)
        {
            logger.debug("Index summary file is corrupted: " + descriptor.filenameFor(Component.SUMMARY), ex);
        }
    }

    private long calculateEstimatedKeys(RandomAccessReader indexReader) throws IOException
    {
        StatsMetadata statsMetadata = getStatsMetadata();
        long histogramCount = statsMetadata.estimatedPartitionSize.count();
        return histogramCount > 0 && !statsMetadata.estimatedPartitionSize.isOverflowed() ? histogramCount : SSTable.estimateRowsFromIndex(indexReader, descriptor);
    }

    private void setupDataFile()
    {
        logger.info("Opening {} ({})", descriptor, FBUtilities.prettyPrintMemory(descriptor.fileFor(Component.DATA).length()));
        boolean compression = getComponents().contains(Component.COMPRESSION_INFO);
        FileHandle.Builder dbuilder = new FileHandle.Builder(descriptor.fileFor(Component.DATA));
        int dataBufferSize = getDiskOptimizationStrategy().bufferSize(getStatsMetadata().estimatedPartitionSize.percentile(getDiskOptimizationEstimatePercentile()));
        dbuilder.mmapped(getDataFileAccessMode() == DiskAccessMode.mmap);
        dbuilder.withChunkCache(ChunkCache.instance);
        dbuilder.bufferSize(dataBufferSize);
        if (compression)
            dbuilder.withCompressionMetadata(CompressionInfoComponent.load(descriptor));
        setDataFile(dbuilder.complete());
    }

    private void setupIndexFile()
    {
        FileHandle.Builder ibuilder = new FileHandle.Builder(descriptor.fileFor(Component.PRIMARY_INDEX));
        long indexFileLength = descriptor.fileFor(Component.PRIMARY_INDEX).length();
        int indexBufferSize = getDiskOptimizationStrategy().bufferSize(indexFileLength / getIndexSummary().size());
        setIndexFile(ibuilder.mmapped(getIndexFileAccessMode() == DiskAccessMode.mmap).withChunkCache(ChunkCache.instance).bufferSize(indexBufferSize).complete());
    }

    private void validatePartitioner(StatsComponent statsComponent)
    {
        // Check if sstable is created using same partitioner.
        // Partitioner can be null, which indicates older version of sstable or no stats available.
        // In that case, we skip the check.
        String partitionerName = getTableMetadataRef().get().partitioner.getClass().getCanonicalName();
        if (statsComponent.validationMetadata() != null && !partitionerName.equals(statsComponent.validationMetadata().partitioner))
        {
            throw new CorruptSSTableException(new IOException(String.format("Cannot open %s; partitioner %s does not match system partitioner %s. " +
                                                                            "Note that the default partitioner starting with Cassandra 1.2 is Murmur3Partitioner, " +
                                                                            "so you will need to edit that to match your old partitioner if upgrading.",
                                                                            descriptor, statsComponent.validationMetadata().partitioner, partitionerName)),
                                              descriptor.filenameFor(Component.STATS));
        }
    }
}
