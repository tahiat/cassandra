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

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.Set;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.rows.PartitionSerializationException;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Transactional;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class SortedTableWriter<P extends SortedTablePartitionWriter, RIE extends AbstractRowIndexEntry> extends SSTableWriter<RIE>
{
    private final static Logger logger = LoggerFactory.getLogger(SortedTableWriter.class);

    protected final SequentialWriter dataWriter;
    protected final P partitionWriter;
    private final FileHandle.Builder dataFileBuilder = new FileHandle.Builder(descriptor.fileFor(Component.DATA));
    private DecoratedKey lastWrittenKey;
    private DataPosition dataMark;
    private long lastEarlyOpenLength;

    public SortedTableWriter(SortedTableWriterBuilder<RIE, P, ?, ?> builder, LifecycleNewTracker lifecycleNewTracker)
    {
        super(builder, lifecycleNewTracker);
        checkNotNull(builder.getDataWriter());
        checkNotNull(builder.getPartitionWriter());

        this.dataWriter = builder.getDataWriter();
        this.partitionWriter = builder.getPartitionWriter();
    }

    /**
     * Appends partition data to this writer.
     *
     * @param partition the partition to write
     * @return the created index entry if something was written, that is if {@code iterator}
     * wasn't empty, {@code null} otherwise.
     * @throws FSWriteError if write to the dataFile fails
     */
    @Override
    public final RIE append(UnfilteredRowIterator partition)
    {
        if (partition.isEmpty())
            return null;

        try
        {
            if (!verifyPartition(partition.partitionKey()))
                return null;

            startPartition(partition.partitionKey(), partition.partitionLevelDeletion());

            RIE indexEntry;
            if (header.hasStatic())
                addStaticRow(partition.partitionKey(), partition.staticRow());

            while (partition.hasNext())
                addUnfiltered(partition.partitionKey(), partition.next());

            indexEntry = endPartition(partition.partitionKey(), partition.partitionLevelDeletion());

            return indexEntry;
        }
        catch (BufferOverflowException boe)
        {
            throw new PartitionSerializationException(partition, boe);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getFilename());
        }
    }

    private boolean verifyPartition(DecoratedKey key)
    {
        assert key != null : "Keys must not be null"; // empty keys ARE allowed b/c of indexed column values

        if (key.getKey().remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            logger.error("Key size {} exceeds maximum of {}, skipping row", key.getKey().remaining(), FBUtilities.MAX_UNSIGNED_SHORT);
            return false;
        }

        if (lastWrittenKey != null && lastWrittenKey.compareTo(key) >= 0)
            throw new RuntimeException(String.format("Last written key %s >= current key %s, writing into %s", lastWrittenKey, key, getFilename()));

        return true;
    }

    private void startPartition(DecoratedKey key, DeletionTime partitionLevelDeletion) throws IOException
    {
        partitionWriter.start(key, partitionLevelDeletion);
        metadataCollector.update(partitionLevelDeletion);

        onStartPartition(key);
    }

    private void addStaticRow(DecoratedKey key, Row row) throws IOException
    {
        guardCollectionSize(metadata(), key, row);

        partitionWriter.addStaticRow(row);
        if (!row.isEmpty())
            Rows.collectStats(row, metadataCollector);

        onStaticRow(row);
    }

    private void addUnfiltered(DecoratedKey key, Unfiltered unfiltered) throws IOException
    {
        if (unfiltered.isRow())
        {
            addRow(key, (Row) unfiltered);
        }
        else
        {
            assert unfiltered.isRangeTombstoneMarker();
            addRangeTomstoneMarker((RangeTombstoneMarker) unfiltered);
        }
    }

    private void addRow(DecoratedKey key, Row row) throws IOException
    {
        guardCollectionSize(metadata(), key, row);

        partitionWriter.addUnfiltered(row);
        metadataCollector.updateClusteringValues(row.clustering());
        Rows.collectStats(row, metadataCollector);

        onRow(row);
    }

    private void addRangeTomstoneMarker(RangeTombstoneMarker marker) throws IOException
    {
        partitionWriter.addUnfiltered(marker);

        metadataCollector.updateClusteringValues(marker.clustering());
        if (marker.isBoundary())
        {
            RangeTombstoneBoundaryMarker bm = (RangeTombstoneBoundaryMarker) marker;
            metadataCollector.update(bm.endDeletionTime());
            metadataCollector.update(bm.startDeletionTime());
        }
        else
        {
            metadataCollector.update(((RangeTombstoneBoundMarker) marker).deletionTime());
        }

        onRangeTombstoneMarker(marker);
    }

    private RIE endPartition(DecoratedKey key, DeletionTime partitionLevelDeletion) throws IOException
    {
        long finishResult = partitionWriter.finish();

        long endPosition = dataWriter.position();
        long rowSize = endPosition - partitionWriter.getInitialPosition();
        maybeLogLargePartitionWarning(key, rowSize);
        maybeLogManyTombstonesWarning(key, metadataCollector.totalTombstones);
        metadataCollector.addPartitionSizeInBytes(rowSize);
        metadataCollector.addKey(key.getKey());
        metadataCollector.addCellPerPartitionCount();

        lastWrittenKey = key;
        last = lastWrittenKey;
        if (first == null)
            first = lastWrittenKey;

        if (logger.isTraceEnabled())
            logger.trace("wrote {} at {}", key, endPosition);

        return createRowIndexEntry(key, partitionLevelDeletion, finishResult);
    }

    protected void onStartPartition(DecoratedKey key)
    {
        notifyObservers(o -> o.startPartition(key, partitionWriter.getInitialPosition(), partitionWriter.getInitialPosition()));
    }

    protected void onStaticRow(Row row)
    {
        notifyObservers(o -> o.staticRow(row));
    }

    protected void onRow(Row row)
    {
        notifyObservers(o -> o.nextUnfilteredCluster(row));
    }

    protected void onRangeTombstoneMarker(RangeTombstoneMarker marker)
    {
        notifyObservers(o -> o.nextUnfilteredCluster(marker));
    }

    protected abstract RIE createRowIndexEntry(DecoratedKey key, DeletionTime partitionLevelDeletion, long finishResult) throws IOException;

    protected final void notifyObservers(Consumer<SSTableFlushObserver> action)
    {
        if (observers != null && !observers.isEmpty())
            observers.forEach(action);
    }

    @Override
    public void mark()
    {
        dataMark = dataWriter.mark();
    }

    @Override
    public void resetAndTruncate()
    {
        dataWriter.resetAndTruncate(dataMark);
        partitionWriter.reset();
    }

    @Override
    public long getFilePointer()
    {
        return dataWriter.position();
    }

    @Override
    public long getOnDiskFilePointer()
    {
        return dataWriter.getOnDiskFilePointer();
    }

    @Override
    public long getEstimatedOnDiskBytesWritten()
    {
        return dataWriter.getEstimatedOnDiskBytesWritten();
    }

    protected FileHandle openDataFile(long lengthOverride, StatsMetadata statsMetadata)
    {
        int dataBufferSize = ioOptions.diskOptimizationStrategy.bufferSize(statsMetadata.estimatedPartitionSize.percentile(ioOptions.diskOptimizationEstimatePercentile));

        FileHandle dataFile = dataFileBuilder.mmapped(ioOptions.defaultDiskAccessMode == Config.DiskAccessMode.mmap)
                                             .withChunkCache(chunkCache)
                                             .withCompressionMetadata(compression ? ((CompressedSequentialWriter) dataWriter).open(lengthOverride) : null)
                                             .bufferSize(dataBufferSize)
                                             .withLengthOverride(lengthOverride)
                                             .complete();

        try
        {
            if (chunkCache != null)
            {
                if (lastEarlyOpenLength != 0 && dataFile.dataLength() > lastEarlyOpenLength)
                    chunkCache.invalidatePosition(dataFile, lastEarlyOpenLength);
            }
            lastEarlyOpenLength = dataFile.dataLength();
        }
        catch (RuntimeException | Error ex)
        {
            Throwables.closeAndAddSuppressed(ex, dataFile);
            throw ex;
        }

        return dataFile;
    }

    private void maybeLogLargePartitionWarning(DecoratedKey key, long rowSize)
    {
        if (rowSize > DatabaseDescriptor.getCompactionLargePartitionWarningThreshold())
        {
            String keyString = metadata().partitionKeyType.getString(key.getKey());
            logger.warn("Writing large partition {}/{}:{} ({}) to sstable {}", metadata.keyspace, metadata.name, keyString, FBUtilities.prettyPrintMemory(rowSize), getFilename());
        }
    }

    private void maybeLogManyTombstonesWarning(DecoratedKey key, int tombstoneCount)
    {
        if (tombstoneCount > DatabaseDescriptor.getCompactionTombstoneWarningThreshold())
        {
            String keyString = metadata().partitionKeyType.getString(key.getKey());
            logger.warn("Writing {} tombstones to {}/{}:{} in sstable {}", tombstoneCount, metadata.keyspace, metadata.name, keyString, getFilename());
        }
    }

    protected static abstract class AbstractIndexWriter extends AbstractTransactional implements Transactional
    {
        protected final Descriptor descriptor;
        protected final TableMetadataRef metadata;
        protected final Set<Component> components;

        protected final IFilter bf;

        protected AbstractIndexWriter(SortedTableWriterBuilder<?, ?, ?, ?> b)
        {
            this.descriptor = b.descriptor;
            this.metadata = b.getTableMetadataRef();
            this.components = b.getComponents();

            bf = FilterFactory.getFilter(b.getKeyCount(), b.getTableMetadataRef().getLocal().params.bloomFilterFpChance);
        }

        protected void flushBf()
        {
            if (components.contains(Component.FILTER))
            {
                try
                {
                    FilterComponent.saveOrDeleteCorrupted(descriptor, bf);
                }
                catch (IOException ex)
                {
                    throw new FSWriteError(ex, descriptor.fileFor(Component.FILTER));
                }
            }
        }

        protected void doPrepare()
        {
            flushBf();
        }

        @Override
        protected Throwable doPostCleanup(Throwable accumulate)
        {
            accumulate = bf.close(accumulate);
            return accumulate;
        }

        public IFilter getFilterCopy()
        {
            return bf.sharedCopy();
        }
    }

}