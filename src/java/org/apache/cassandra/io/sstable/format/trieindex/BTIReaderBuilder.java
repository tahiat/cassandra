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

package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.KeyReader;
import org.apache.cassandra.io.sstable.format.CompressionInfoComponent;
import org.apache.cassandra.io.sstable.format.FilterComponent;
import org.apache.cassandra.io.sstable.format.SSTableReaderBuilder;
import org.apache.cassandra.io.sstable.format.StatsComponent;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.AlwaysPresentFilter;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.Throwables;

public class BTIReaderBuilder extends SSTableReaderBuilder<TrieIndexSSTableReader, BTIReaderBuilder>
{
    private static final Logger logger = LoggerFactory.getLogger(BTIReaderBuilder.class);

    private PartitionIndex partitionIndex;
    private FileHandle rowIndexFile;
    private DiskAccessMode indexFileAccessMode = DatabaseDescriptor.getIndexAccessMode();

    public BTIReaderBuilder(Descriptor descriptor)
    {
        super(descriptor);
    }

    public BTIReaderBuilder setRowIndexFile(FileHandle rowIndexFile)
    {
        this.rowIndexFile = rowIndexFile;
        return this;
    }

    public BTIReaderBuilder setPartitionIndex(PartitionIndex partitionIndex)
    {
        this.partitionIndex = partitionIndex;
        return this;
    }

    public BTIReaderBuilder setIndexFileAccessMode(DiskAccessMode indexFileAccessMode)
    {
        this.indexFileAccessMode = indexFileAccessMode;
        return this;
    }

    public PartitionIndex getPartitionIndex()
    {
        return partitionIndex;
    }

    public FileHandle getRowIndexFile()
    {
        return rowIndexFile;
    }

    public DiskAccessMode getIndexFileAccessMode()
    {
        return indexFileAccessMode;
    }

    public Set<AutoCloseable> getCloseables()
    {
        return Stream.of(getDataFile(), getRowIndexFile(), getPartitionIndex(), getFilter())
                     .filter(Objects::nonNull)
                     .collect(Collectors.toSet());
    }

    @Override
    protected void openComponents() throws IOException
    {
        Set<AutoCloseable> existingComponents = getCloseables();
        try
        {
            StatsComponent statsComponent = StatsComponent.load(descriptor);
            setStatsMetadata(statsComponent.statsMetadata());
            setSerializationHeader(statsComponent.serializationHeader(getTableMetadataRef().get()));

            validatePartitioner(statsComponent);

            assert !isOnline() || getSerializationHeader() != null;

            boolean filterNeeded = getFilter() == null && isOnline() && getComponents().contains(Component.FILTER);
            if (filterNeeded)
                loadFilter(statsComponent);
            boolean rebuildFilter = filterNeeded && getFilter() == null;

            if (rebuildFilter)
            {
                buildBloomFilter();

                if (isOnline())
                {
                    FilterComponent.save(getFilter(), descriptor);
                }
            }

            assert !filterNeeded || getFilter() != null;

            if (getFilter() == null)
                setFilter(FilterFactory.AlwaysPresent);

            if (getComponents().contains(Component.ROW_INDEX) && getRowIndexFile() == null)
                setupRowIndexFile();

            if (getComponents().contains(Component.PARTITION_INDEX) && getPartitionIndex() == null)
                loadPartitionIndex();

            if (getDataFile() == null)
                setupDataFile();
        }
        catch (IOException | RuntimeException | Error ex)
        {
            // in case of failure, close only those components which have been opened in this try-catch block
            Throwables.closeAndAddSuppressed(ex, Throwables.refBasedSetsDiff(getCloseables(), existingComponents));
            throw ex;
        }
    }

    @Override
    protected TrieIndexSSTableReader buildInternal()
    {
        return new TrieIndexSSTableReader(this);
    }

    private void buildBloomFilter() throws IOException
    {
        IFilter bf;

        try (KeyReader keyReader = descriptor.getFormat().getReaderFactory().openKeyReader(descriptor, getTableMetadataRef().getLocal(), getComponents()))
        {
            if (keyReader == null)
                return;

            bf = FilterFactory.getFilter(getStatsMetadata().totalRows, getTableMetadataRef().get().params.bloomFilterFpChance);
            setFilter(bf);

            while (!keyReader.isExhausted())
            {
                DecoratedKey key = getTableMetadataRef().getLocal().partitioner.decorateKey(keyReader.key());
                bf.add(key);
            }
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

    private void loadPartitionIndex()
    {
        FileHandle.Builder builder = defaultFileHandleBuilder(getDescriptor().fileFor(Component.PARTITION_INDEX));
        builder.mmapped(getIndexFileAccessMode());

        try (FileHandle indexFile = builder.complete())
        {
            PartitionIndex partitionIndex = PartitionIndex.load(indexFile, getTableMetadataRef().get().partitioner, getFilter() instanceof AlwaysPresentFilter);
            setPartitionIndex(partitionIndex);
            setFirst(partitionIndex.firstKey());
            setLast(partitionIndex.lastKey());
        }
        catch (IOException ex)
        {
            logger.debug("Partition index file is corrupted: " + descriptor.filenameFor(Component.PARTITION_INDEX), ex);
        }
    }

    private void setupDataFile()
    {
        logger.info("Opening {} ({})", descriptor, FBUtilities.prettyPrintMemory(descriptor.fileFor(Component.DATA).length()));
        int bufferSize = getDiskOptimizationStrategy().bufferSize(getStatsMetadata().estimatedPartitionSize.percentile(getDiskOptimizationEstimatePercentile()));

        FileHandle.Builder builder = defaultFileHandleBuilder(descriptor.fileFor(Component.DATA));
        builder.bufferSize(bufferSize);
        if (getComponents().contains(Component.COMPRESSION_INFO))
            builder.withCompressionMetadata(CompressionInfoComponent.load(descriptor));
        setDataFile(builder.complete());
    }

    private void setupRowIndexFile()
    {
        FileHandle.Builder builder = defaultFileHandleBuilder(descriptor.fileFor(Component.ROW_INDEX));
        builder.mmapped(getIndexFileAccessMode());
        setRowIndexFile(builder.complete());
    }
}
