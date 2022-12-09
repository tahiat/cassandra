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
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.IndexComponent;
import org.apache.cassandra.io.sstable.format.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SortedTableWriter;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Throwables;

@VisibleForTesting
public class BtiTableWriter extends SortedTableWriter<BtiFormatPartitionWriter, TrieIndexEntry>
{
    private static final Logger logger = LoggerFactory.getLogger(BtiTableWriter.class);

    private final BtiFormatPartitionWriter partitionWriter;
    private final IndexWriter iwriter;
    private final TransactionalProxy txnProxy;

    public BtiTableWriter(BtiTableWriterBuilder builder, LifecycleNewTracker lifecycleNewTracker)
    {
        super(builder, lifecycleNewTracker);
        this.dataWriter = builder.getDataWriter();
        this.iwriter = builder.getIndexWriter();
        this.partitionWriter = builder.getPartitionWriter();

        txnProxy = new TransactionalProxy();
    }

    @Override
    public void mark()
    {
        super.mark();
        iwriter.mark();
    }

    @Override
    public void resetAndTruncate()
    {
        super.resetAndTruncate();
        iwriter.resetAndTruncate();
    }

    @Override
    protected TrieIndexEntry createRowIndexEntry(DecoratedKey key, DeletionTime partitionLevelDeletion, long finishResult) throws IOException
    {
        TrieIndexEntry entry = TrieIndexEntry.create(partitionWriter.getInitialPosition(),
                                                     trieRoot,
                                                     partitionLevelDeletion,
                                                     partitionWriter.rowIndexCount);
        iwriter.append(key, entry);
        return entry;
    }

    public void openEarly(Consumer<SSTableReader> callWhenReady)
    {
        long dataLength = dataWriter.position();

        return iwriter.buildPartial(dataLength, partitionIndex ->
        {
            StatsMetadata stats = statsMetadata();
            FileHandle ifile = iwriter.rowIndexFHBuilder.complete(iwriter.rowIndexFile.getLastFlushOffset());
            if (compression)
                dbuilder.withCompressionMetadata(((CompressedSequentialWriter) dataWriter).open(dataWriter.getLastFlushOffset()));
            int dataBufferSize = ioOptions.diskOptimizationStrategy.bufferSize(stats.estimatedPartitionSize.percentile(DatabaseDescriptor.getDiskOptimizationEstimatePercentile()));
            FileHandle dfile = dbuilder.bufferSize(dataBufferSize).complete(dataWriter.getLastFlushOffset());
            invalidateCacheAtBoundary(dfile);
            SSTableReader sstable = BtiTableReader.internalOpen(descriptor,
                                                                components, metadata,
                                                                ifile, dfile, partitionIndex, iwriter.bf.sharedCopy(),
                                                                maxDataAge, stats, SSTableReader.OpenReason.EARLY, header);

            sstable.first = getMinimalKey(partitionIndex.firstKey());
            sstable.last = getMinimalKey(partitionIndex.lastKey());
            sstable.setup(true);
            callWhenReady.accept(sstable);
        });
    }

    public SSTableReader openFinalEarly()
    {
        // we must ensure the data is completely flushed to disk
        iwriter.complete(); // This will be called by completedPartitionIndex() below too, but we want it done now to
        // ensure outstanding openEarly actions are not triggered.
        dataWriter.sync();
        iwriter.rowIndexFile.sync();
        // Note: Nothing must be written to any of the files after this point, as the chunk cache could pick up and
        // retain a partially-written page (see DB-2446).

        return openFinal(SSTableReader.OpenReason.EARLY);
    }

    @SuppressWarnings("resource")
    protected SSTableReader openFinal(SSTableReader.OpenReason openReason)
    {
        if (maxDataAge < 0)
            maxDataAge = System.currentTimeMillis();

        StatsMetadata stats = statsMetadata();
        // finalize in-memory state for the reader
        PartitionIndex partitionIndex = iwriter.completedPartitionIndex();
        FileHandle rowIndexFile = iwriter.rowIndexFHBuilder.complete();
        int dataBufferSize = ioOptions.diskOptimizationStrategy.bufferSize(stats.estimatedPartitionSize.percentile(DatabaseDescriptor.getDiskOptimizationEstimatePercentile()));
        if (compression)
            dbuilder.withCompressionMetadata(((CompressedSequentialWriter) dataWriter).open(dataWriter.getLastFlushOffset()));
        FileHandle dfile = dbuilder.bufferSize(dataBufferSize).complete();
        invalidateCacheAtBoundary(dfile);
        SSTableReader sstable = BtiTableReader.internalOpen(descriptor,
                                                            components,
                                                            this.metadata,
                                                            rowIndexFile,
                                                            dfile,
                                                            partitionIndex,
                                                            iwriter.bf.sharedCopy(),
                                                            maxDataAge,
                                                            stats,
                                                            openReason,
                                                            header);
        sstable.first = getMinimalKey(first);
        sstable.last = getMinimalKey(last);
        sstable.setup(true);
        return sstable;
    }

    protected SortedTableWriter.TransactionalProxy txnProxy()
    {
        return txnProxy;
    }

    class TransactionalProxy extends SortedTableWriter.TransactionalProxy
    {
        // finalise our state on disk, including renaming
        @Override
        protected void doPrepare()
        {
            iwriter.prepareToCommit();
            super.doPrepare();
        }

        @Override
        protected Throwable doCommit(Throwable accumulate)
        {
            accumulate = super.doCommit(accumulate);
            accumulate = iwriter.commit(accumulate);
            return accumulate;
        }

        @Override
        protected Throwable doPostCleanup(Throwable accumulate)
        {
            accumulate = Throwables.close(accumulate, partitionWriter);
            accumulate = super.doPostCleanup(accumulate);
            return accumulate;
        }

        @Override
        protected Throwable doAbort(Throwable accumulate)
        {
            accumulate = iwriter.abort(accumulate);
            accumulate = super.doAbort(accumulate);
            return accumulate;
        }
    }

    /**
     * Encapsulates writing the index and filter for an SSTable. The state of this object is not valid until it has been closed.
     */
    static class IndexWriter extends SortedTableWriter.AbstractIndexWriter
    {
        private final SequentialWriter rowIndexFile;
        private final FileHandle.Builder rowIndexFHBuilder;
        private final SequentialWriter partitionIndexFile;
        private final FileHandle.Builder partitionIndexFHBuilder;
        private final PartitionIndexBuilder partitionIndex;
        boolean partitionIndexCompleted = false;
        private DataPosition riMark;
        private DataPosition piMark;

        IndexWriter(BtiTableWriterBuilder b)
        {
            super(b);
            rowIndexFile = new SequentialWriter(descriptor.fileFor(Component.ROW_INDEX), b.getIOOptions().writerOptions);
            rowIndexFHBuilder = IndexComponent.fileBuilder(Component.ROW_INDEX, b);
            partitionIndexFile = new SequentialWriter(descriptor.fileFor(Component.PARTITION_INDEX), b.getIOOptions().writerOptions);
            partitionIndexFHBuilder = IndexComponent.fileBuilder(Component.PARTITION_INDEX, b);
            partitionIndex = new PartitionIndexBuilder(partitionIndexFile, partitionIndexFHBuilder);
            // register listeners to be alerted when the data files are flushed
            partitionIndexFile.setPostFlushListener(() -> partitionIndex.markPartitionIndexSynced(partitionIndexFile.getLastFlushOffset()));
            rowIndexFile.setPostFlushListener(() -> partitionIndex.markRowIndexSynced(rowIndexFile.getLastFlushOffset()));
            b.getDataWriter().setPostFlushListener(() -> partitionIndex.markDataSynced(b.getDataWriter().getLastFlushOffset()));
        }

        public long append(DecoratedKey key, AbstractRowIndexEntry indexEntry) throws IOException
        {
            bf.add(key);
            long position;
            if (indexEntry.isIndexed())
            {
                long indexStart = rowIndexFile.position();
                try
                {
                    ByteBufferUtil.writeWithShortLength(key.getKey(), rowIndexFile);
                    ((TrieIndexEntry) indexEntry).serialize(rowIndexFile, rowIndexFile.position());
                }
                catch (IOException e)
                {
                    throw new FSWriteError(e, rowIndexFile.getFile());
                }

                if (logger.isTraceEnabled())
                    logger.trace("wrote index entry: {} at {}", indexEntry, indexStart);
                position = indexStart;
            }
            else
            {
                // Write data position directly in trie.
                position = ~indexEntry.position;
            }
            partitionIndex.addEntry(key, position);
            return position;
        }

        public boolean buildPartial(long dataPosition, Consumer<PartitionIndex> callWhenReady)
        {
            return partitionIndex.buildPartial(callWhenReady, rowIndexFile.position(), dataPosition);
        }

        public void mark()
        {
            riMark = rowIndexFile.mark();
            piMark = partitionIndexFile.mark();
        }

        public void resetAndTruncate()
        {
            // we can't un-set the bloom filter addition, but extra keys in there are harmless.
            // we can't reset dbuilder either, but that is the last thing called in afterappend so
            // we assume that if that worked then we won't be trying to reset.
            rowIndexFile.resetAndTruncate(riMark);
            partitionIndexFile.resetAndTruncate(piMark);
        }

        protected void doPrepare()
        {
            flushBf();

            // truncate index file
            rowIndexFile.prepareToCommit();
            rowIndexFHBuilder.withLengthOverride(rowIndexFile.getLastFlushOffset());

            complete();
        }

        void complete() throws FSWriteError
        {
            if (partitionIndexCompleted)
                return;

            try
            {
                partitionIndex.complete();
                partitionIndexCompleted = true;
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, partitionIndexFile.getFile());
            }
        }

        PartitionIndex completedPartitionIndex()
        {
            complete();
            try
            {
                return PartitionIndex.load(partitionIndexFHBuilder, metadata.getLocal().partitioner, false);
            }
            catch (IOException e)
            {
                throw new FSReadError(e, partitionIndexFile.getFile());
            }
        }

        protected Throwable doCommit(Throwable accumulate)
        {
            return rowIndexFile.commit(accumulate);
        }

        protected Throwable doAbort(Throwable accumulate)
        {
            return rowIndexFile.abort(accumulate);
        }

        @Override
        protected Throwable doPostCleanup(Throwable accumulate)
        {
            return Throwables.close(accumulate, bf, partitionIndex, rowIndexFile, partitionIndexFile);
        }
    }
}
