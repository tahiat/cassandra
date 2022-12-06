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
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.format.FilterComponent;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.TOCComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.concurrent.Transactional;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public class BigTableWriter extends SSTableWriter
{
    private static final Logger logger = LoggerFactory.getLogger(BigTableWriter.class);

    private final BigFormatPartitionWriter partitionWriter;
    private final IndexWriter indexWriter;
    private final SequentialWriter dataWriter;

    private DecoratedKey lastWrittenKey;
    private DataPosition dataMark;
    private long lastEarlyOpenLength = 0;
    private final RowIndexEntry.IndexSerializer rowIndexEntrySerializer;

    private final BigTableOptions options = new BigTableOptions();

    public BigTableWriter(BigTableWriterBuilder builder, LifecycleNewTracker lifecycleNewTracker)
    {
        super(builder, lifecycleNewTracker);

        rowIndexEntrySerializer = new RowIndexEntry.Serializer(descriptor.version, header);

        dataWriter = DataComponent.buildWriter(builder.getDescriptor(),
                                               builder.getTableMetadataRef().getLocal(),
                                               options.writerOptions,
                                               builder.getMetadataCollector(),
                                               lifecycleNewTracker.opType(),
                                               options.flushCompression);

        indexWriter = new IndexWriter(builder.getKeyCount());

        partitionWriter = new BigFormatPartitionWriter(builder.getSerializationHeader(),
                                                       dataWriter,
                                                       builder.getDescriptor().version,
                                                       builder.getFlushObservers(),
                                                       rowIndexEntrySerializer.indexInfoSerializer());
    }

    public void mark()
    {
        dataMark = dataWriter.mark();
        indexWriter.mark();
    }

    public void resetAndTruncate()
    {
        dataWriter.resetAndTruncate(dataMark);
        indexWriter.resetAndTruncate();
    }

    @VisibleForTesting
    protected long getDataWriterPosition()
    {
        return dataWriter.position();
    }

    /**
     * Perform sanity checks on @param decoratedKey and @return the position in the data file before any data is written
     */
    protected long beforeAppend(DecoratedKey decoratedKey)
    {
        assert decoratedKey != null : "Keys must not be null"; // empty keys ARE allowed b/c of indexed column values
        if (lastWrittenKey != null && lastWrittenKey.compareTo(decoratedKey) >= 0)
            throw new RuntimeException("Last written key " + lastWrittenKey + " >= current key " + decoratedKey + " writing into " + getFilename());
        return (lastWrittenKey == null) ? 0 : dataWriter.position();
    }

    private void afterAppend(DecoratedKey decoratedKey, long dataEnd, RowIndexEntry index, ByteBuffer indexInfo) throws IOException
    {
        metadataCollector.addKey(decoratedKey.getKey());
        lastWrittenKey = decoratedKey;
        last = lastWrittenKey;
        if (first == null)
            first = lastWrittenKey;

        if (logger.isTraceEnabled())
            logger.trace("wrote {} at {}", decoratedKey, dataEnd);
        indexWriter.append(decoratedKey, index, dataEnd, indexInfo);
    }

    /**
     * Appends partition data to this writer.
     *
     * @param iterator the partition to write
     * @return the created index entry if something was written, that is if {@code iterator}
     * wasn't empty, {@code null} otherwise.
     *
     * @throws FSWriteError if a write to the dataFile fails
     */
    public RowIndexEntry append(UnfilteredRowIterator iterator)
    {
        DecoratedKey key = iterator.partitionKey();

        if (key.getKey().remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            logger.error("Key size {} exceeds maximum of {}, skipping row", key.getKey().remaining(), FBUtilities.MAX_UNSIGNED_SHORT);
            return null;
        }

        if (iterator.isEmpty())
            return null;

        long startPosition = beforeAppend(key);
        observers.forEach((o) -> o.startPartition(key, indexWriter.writer.position()));

        //Reuse the writer for each row
        partitionWriter.reset(DatabaseDescriptor.getColumnIndexCacheSize(), DatabaseDescriptor.getColumnIndexSize());

        try (UnfilteredRowIterator collecting = Transformation.apply(iterator, new StatsCollector(metadataCollector)))
        {
            partitionWriter.buildRowIndex(collecting);

            // afterAppend() writes the partition key before the first RowIndexEntry - so we have to add it's
            // serialized size to the index-writer position
            long indexFilePosition = ByteBufferUtil.serializedSizeWithShortLength(key.getKey()) + indexWriter.writer.position();

            RowIndexEntry entry = RowIndexEntry.create(startPosition, indexFilePosition,
                                                       collecting.partitionLevelDeletion(),
                                                       partitionWriter.headerLength,
                                                       partitionWriter.columnIndexCount,
                                                       partitionWriter.indexInfoSerializedSize(),
                                                       partitionWriter.indexSamples(),
                                                       partitionWriter.offsets(),
                                                       rowIndexEntrySerializer.indexInfoSerializer());

            long endPosition = dataWriter.position();
            long rowSize = endPosition - startPosition;
            maybeLogLargePartitionWarning(key, rowSize);
            maybeLogManyTombstonesWarning(key, metadataCollector.totalTombstones);
            metadataCollector.addPartitionSizeInBytes(rowSize);
            afterAppend(key, endPosition, entry, partitionWriter.buffer());
            return entry;
        }
        catch (BufferOverflowException boe)
        {
            throw new PartitionSerializationException(iterator, boe);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, dataWriter.getPath());
        }
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

    private static class StatsCollector extends Transformation
    {
        private final MetadataCollector collector;
        private int cellCount;

        StatsCollector(MetadataCollector collector)
        {
            this.collector = collector;
        }

        @Override
        public Row applyToStatic(Row row)
        {
            if (!row.isEmpty())
                cellCount += Rows.collectStats(row, collector);
            return row;
        }

        @Override
        public Row applyToRow(Row row)
        {
            collector.updateClusteringValues(row.clustering());
            cellCount += Rows.collectStats(row, collector);
            return row;
        }

        @Override
        public RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
        {
            collector.updateClusteringValues(marker.clustering());
            if (marker.isBoundary())
            {
                RangeTombstoneBoundaryMarker bm = (RangeTombstoneBoundaryMarker)marker;
                collector.update(bm.endDeletionTime());
                collector.update(bm.startDeletionTime());
            }
            else
            {
                collector.update(((RangeTombstoneBoundMarker)marker).deletionTime());
            }
            return marker;
        }

        @Override
        public void onPartitionClose()
        {
            collector.addCellPerPartitionCount(cellCount);
        }

        @Override
        public DeletionTime applyToDeletion(DeletionTime deletionTime)
        {
            collector.update(deletionTime);
            return deletionTime;
        }
    }

    private BigTableReader openInternal(IndexSummaryBuilder.ReadableBoundary boundary, SSTableReader.OpenReason openReason)
    {
        assert boundary == null || (boundary.indexLength > 0 && boundary.dataLength > 0);

        BigTableReaderBuilder builder = new BigTableReaderBuilder(descriptor).setComponents(components)
                                                                             .setTableMetadataRef(metadata)
                                                                             .setMaxDataAge(maxDataAge)
                                                                             .setSerializationHeader(header)
                                                                             .setOpenReason(openReason)
                                                                             .setFirst(first)
                                                                             .setLast(boundary != null ? boundary.lastKey : last);

        try
        {
            builder.setStatsMetadata(statsMetadata());

            EstimatedHistogram partitionSizeHistogram = builder.getStatsMetadata().estimatedPartitionSize;
            if (boundary != null)
            {
                if (partitionSizeHistogram.isOverflowed())
                {
                    logger.warn("Estimated partition size histogram for '{}' is overflowed ({} values greater than {}). " +
                                "Clearing the overflow bucket to allow for degraded mean and percentile calculations...",
                                descriptor, partitionSizeHistogram.overflowCount(), partitionSizeHistogram.getLargestBucketOffset());
                    partitionSizeHistogram.clearOverflow();
                }
            }

            builder.setIndexSummary(indexWriter.summary.build(metadata().partitioner, boundary));

            long indexFileLength = descriptor.fileFor(Component.PRIMARY_INDEX).length();
            int indexBufferSize = options.diskOptimizationStrategy.bufferSize(indexFileLength / builder.getIndexSummary().size());
            FileHandle.Builder indexFileBuilder = indexWriter.builder;
            FileHandle indexFile = indexFileBuilder.bufferSize(indexBufferSize)
                                                   .withLengthOverride(boundary != null ? boundary.indexLength : -1)
                                                   .complete();
            builder.setIndexFile(indexFile);

            int dataBufferSize = options.diskOptimizationStrategy.bufferSize(partitionSizeHistogram.percentile(options.diskOptimizationEstimatePercentile));
            FileHandle.Builder dataFileBuilder = new FileHandle.Builder(descriptor.fileFor(Component.DATA));
            FileHandle dataFile = dataFileBuilder.mmapped(options.defaultDiskAccessMode == Config.DiskAccessMode.mmap)
                                                 .withChunkCache(chunkCache)
                                                 .withCompressionMetadata(compression ? ((CompressedSequentialWriter) dataWriter).open(boundary != null ? boundary.dataLength : 0) : null)
                                                 .bufferSize(dataBufferSize)
                                                 .withLengthOverride(boundary != null ? boundary.dataLength : -1)
                                                 .complete();
            builder.setDataFile(dataFile);
            invalidateCacheAtBoundary(dataFile);

            return builder.build(true, true);
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            Throwables.closeAndAddSuppressed(t, builder.getDataFile(), builder.getIndexFile(), builder.getIndexSummary(), builder.getFilter());
            throw t;
        }
    }

    @Override
    public void openEarly(Consumer<SSTableReader> doWhenReady)
    {
        // find the max (exclusive) readable key
        IndexSummaryBuilder.ReadableBoundary boundary = indexWriter.getMaxReadable();
        if (boundary == null)
            return;

        doWhenReady.accept(openInternal(boundary, SSTableReader.OpenReason.EARLY));
    }

    void invalidateCacheAtBoundary(FileHandle dfile)
    {
        if (chunkCache != null)
        {
            if (lastEarlyOpenLength != 0 && dfile.dataLength() > lastEarlyOpenLength)
                chunkCache.invalidatePosition(dfile, lastEarlyOpenLength);
        }
        lastEarlyOpenLength = dfile.dataLength();
    }

    public SSTableReader openFinalEarly()
    {
        // we must ensure the data is completely flushed to disk
        dataWriter.sync();
        indexWriter.writer.sync();

        return openFinal(SSTableReader.OpenReason.EARLY);
    }

    @SuppressWarnings("resource")
    private SSTableReader openFinal(SSTableReader.OpenReason openReason)
    {
        if (maxDataAge < 0)
            maxDataAge = currentTimeMillis();

        return openInternal(null, openReason);
    }

    protected SSTableWriter.TransactionalProxy txnProxy()
    {
        return new TransactionalProxy();
    }

    class TransactionalProxy extends SSTableWriter.TransactionalProxy
    {
        // finalise our state on disk, including renaming
        protected void doPrepare()
        {
            indexWriter.prepareToCommit();

            // write sstable statistics
            dataWriter.prepareToCommit();
            writeMetadata(descriptor, finalizeMetadata());

            // save the table of components
            TOCComponent.appendTOC(descriptor, components);

            if (openResult)
                finalReader = openFinal(SSTableReader.OpenReason.NORMAL);
        }

        protected Throwable doCommit(Throwable accumulate)
        {
            accumulate = dataWriter.commit(accumulate);
            accumulate = indexWriter.commit(accumulate);
            return accumulate;
        }

        protected Throwable doAbort(Throwable accumulate)
        {
            accumulate = indexWriter.abort(accumulate);
            accumulate = dataWriter.abort(accumulate);
            return accumulate;
        }
    }

    private void writeMetadata(Descriptor desc, Map<MetadataType, MetadataComponent> components)
    {
        File file = new File(desc.filenameFor(Component.STATS));
        try (SequentialWriter out = new SequentialWriter(file, options.writerOptions))
        {
            desc.getMetadataSerializer().serialize(components, out, desc.version);
            out.finish();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, file.path());
        }
    }

    public long getFilePointer()
    {
        return dataWriter.position();
    }

    public long getOnDiskFilePointer()
    {
        return dataWriter.getOnDiskFilePointer();
    }

    public long getEstimatedOnDiskBytesWritten()
    {
        return dataWriter.getEstimatedOnDiskBytesWritten();
    }

    /**
     * Encapsulates writing the index and filter for an SSTable. The state of this object is not valid until it has been closed.
     */
    class IndexWriter extends AbstractTransactional implements Transactional
    {
        private final SequentialWriter writer;
        public final FileHandle.Builder builder;
        public final IndexSummaryBuilder summary;
        public final IFilter bf;
        private DataPosition mark;

        IndexWriter(long keyCount)
        {
            writer = new SequentialWriter(new File(descriptor.filenameFor(Component.PRIMARY_INDEX)), options.writerOptions);
            builder = new FileHandle.Builder(descriptor.fileFor(Component.PRIMARY_INDEX)).mmapped(DatabaseDescriptor.getIndexAccessMode() == Config.DiskAccessMode.mmap);
            builder.withChunkCache(chunkCache);
            summary = new IndexSummaryBuilder(keyCount, metadata().params.minIndexInterval, Downsampling.BASE_SAMPLING_LEVEL);
            bf = FilterFactory.getFilter(keyCount, metadata().params.bloomFilterFpChance);
            // register listeners to be alerted when the data files are flushed
            writer.setPostFlushListener(() -> summary.markIndexSynced(writer.getLastFlushOffset()));
            dataWriter.setPostFlushListener(() -> summary.markDataSynced(dataWriter.getLastFlushOffset()));
        }

        // finds the last (-offset) decorated key that can be guaranteed to occur fully in the flushed portion of the index file
        IndexSummaryBuilder.ReadableBoundary getMaxReadable()
        {
            return summary.getLastReadableBoundary();
        }

        public void append(DecoratedKey key, RowIndexEntry indexEntry, long dataEnd, ByteBuffer indexInfo) throws IOException
        {
            bf.add(key);
            long indexStart = writer.position();
            try
            {
                ByteBufferUtil.writeWithShortLength(key.getKey(), writer);
                rowIndexEntrySerializer.serialize(indexEntry, writer, indexInfo);
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, writer.getPath());
            }
            long indexEnd = writer.position();

            if (logger.isTraceEnabled())
                logger.trace("wrote index entry: {} at {}", indexEntry, indexStart);

            summary.maybeAddEntry(key, indexStart, indexEnd, dataEnd);
        }

        /**
         * Closes the index and bloomfilter, making the public state of this writer valid for consumption.
         */
        void flushBf()
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

        public void mark()
        {
            mark = writer.mark();
        }

        public void resetAndTruncate()
        {
            // we can't un-set the bloom filter addition, but extra keys in there are harmless.
            // we can't reset dbuilder either, but that is the last thing called in afterappend so
            // we assume that if that worked then we won't be trying to reset.
            writer.resetAndTruncate(mark);
        }

        protected void doPrepare()
        {
            flushBf();

            // truncate index file
            long position = writer.position();
            writer.prepareToCommit();
            FileUtils.truncate(writer.getPath(), position);

            // save summary
            summary.prepareToCommit();
            try (IndexSummary indexSummary = summary.build(getPartitioner()))
            {
                new IndexSummaryComponent(indexSummary, first, last).saveOrDeleteCorrupted(descriptor);
            }
            catch (IOException ex)
            {
                logger.warn("Failed to save index summary", ex);
            }
        }

        protected Throwable doCommit(Throwable accumulate)
        {
            return writer.commit(accumulate);
        }

        protected Throwable doAbort(Throwable accumulate)
        {
            return summary.close(writer.abort(accumulate));
        }

        @Override
        protected Throwable doPostCleanup(Throwable accumulate)
        {
            accumulate = summary.close(accumulate);
            accumulate = bf.close(accumulate);
            return accumulate;
        }
    }

}
