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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.MmappedRegionsCache;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Transactional;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This is the API all table writers must implement.
 *
 * TableWriter.create() is the primary way to create a writer for a particular format.
 * The format information is part of the Descriptor.
 */
public abstract class SSTableWriter extends SSTable implements Transactional
{
    protected long repairedAt;
    protected TimeUUID pendingRepair;
    protected boolean isTransient;
    protected long maxDataAge = -1;
    protected final long keyCount;
    protected final MetadataCollector metadataCollector;
    protected final SerializationHeader header;
    protected final Collection<SSTableFlushObserver> observers;
    protected final MmappedRegionsCache mmappedRegionsCache;
    protected final TransactionalProxy txnProxy = txnProxy();

    protected abstract TransactionalProxy txnProxy();

    protected SSTableWriter(SSTableWriterBuilder<?, ?> builder, LifecycleNewTracker lifecycleNewTracker)
    {
        super(builder);
        checkNotNull(builder.getFlushObservers());
        checkNotNull(builder.getMetadataCollector());
        checkNotNull(builder.getSerializationHeader());

        this.keyCount = builder.getKeyCount();
        this.repairedAt = builder.getRepairedAt();
        this.pendingRepair = builder.getPendingRepair();
        this.isTransient = builder.isTransientSSTable();
        this.metadataCollector = builder.getMetadataCollector();
        this.header = builder.getSerializationHeader();
        this.observers = builder.getFlushObservers();
        this.mmappedRegionsCache = builder.getMmappedRegionsCache();

        lifecycleNewTracker.trackNew(this);
    }

    public static SSTableWriter create(Descriptor descriptor,
                                       Long keyCount,
                                       Long repairedAt,
                                       TimeUUID pendingRepair,
                                       boolean isTransient,
                                       TableMetadataRef metadata,
                                       MetadataCollector metadataCollector,
                                       SerializationHeader header,
                                       Collection<Index> indexes,
                                       LifecycleNewTracker lifecycleNewTracker)
    {
        Factory writerFactory = descriptor.getFormat().getWriterFactory();
        return writerFactory.open(descriptor, keyCount, repairedAt, pendingRepair, isTransient, metadata, metadataCollector, header, observers(descriptor, indexes, lifecycleNewTracker.opType()), lifecycleNewTracker);
    }

    public static SSTableWriter create(Descriptor descriptor,
                                       long keyCount,
                                       long repairedAt,
                                       TimeUUID pendingRepair,
                                       boolean isTransient,
                                       int sstableLevel,
                                       SerializationHeader header,
                                       Collection<Index> indexes,
                                       LifecycleNewTracker lifecycleNewTracker)
    {
        TableMetadataRef metadata = Schema.instance.getTableMetadataRef(descriptor);
        return create(metadata, descriptor, keyCount, repairedAt, pendingRepair, isTransient, sstableLevel, header, indexes, lifecycleNewTracker);
    }

    public static SSTableWriter create(TableMetadataRef metadata,
                                       Descriptor descriptor,
                                       long keyCount,
                                       long repairedAt,
                                       TimeUUID pendingRepair,
                                       boolean isTransient,
                                       int sstableLevel,
                                       SerializationHeader header,
                                       Collection<Index> indexes,
                                       LifecycleNewTracker lifecycleNewTracker)
    {
        MetadataCollector collector = new MetadataCollector(metadata.get().comparator).sstableLevel(sstableLevel);
        return create(descriptor, keyCount, repairedAt, pendingRepair, isTransient, metadata, collector, header, indexes, lifecycleNewTracker);
    }

    @VisibleForTesting
    public static SSTableWriter create(Descriptor descriptor,
                                       long keyCount,
                                       long repairedAt,
                                       TimeUUID pendingRepair,
                                       boolean isTransient,
                                       SerializationHeader header,
                                       Collection<Index> indexes,
                                       LifecycleNewTracker lifecycleNewTracker)
    {
        return create(descriptor, keyCount, repairedAt, pendingRepair, isTransient, 0, header, indexes, lifecycleNewTracker);
    }

    private static Collection<SSTableFlushObserver> observers(Descriptor descriptor,
                                                              Collection<Index> indexes,
                                                              OperationType operationType)
    {
        if (indexes == null)
            return Collections.emptyList();

        List<SSTableFlushObserver> observers = new ArrayList<>(indexes.size());
        for (Index index : indexes)
        {
            SSTableFlushObserver observer = index.getFlushObserver(descriptor, operationType);
            if (observer != null)
            {
                observer.begin();
                observers.add(observer);
            }
        }

        return ImmutableList.copyOf(observers);
    }

    public abstract void mark();

    /**
     * Appends partition data to this writer.
     *
     * @param iterator the partition to write
     * @return the created index entry if something was written, that is if {@code iterator}
     * wasn't empty, {@code null} otherwise.
     *
     * @throws FSWriteError if writing to the dataFile fails
     */
    public abstract AbstractRowIndexEntry append(UnfilteredRowIterator iterator);

    public abstract long getFilePointer();

    public abstract long getOnDiskFilePointer();

    public long getEstimatedOnDiskBytesWritten()
    {
        return getOnDiskFilePointer();
    }

    public abstract void resetAndTruncate();

    public void setRepairedAt(long repairedAt)
    {
        if (repairedAt > 0)
            this.repairedAt = repairedAt;
    }

    public void setMaxDataAge(long maxDataAge)
    {
        this.maxDataAge = maxDataAge;
    }

    public void setOpenResult(boolean openResult)
    {
        txnProxy.openResult = openResult;
    }

    /**
     * Open the resultant SSTableReader before it has been fully written.
     *
     * The passed consumer will be called when the necessary data has been flushed to disk/cache. This may never happen
     * (e.g. if the table was finished before the flushes materialized, or if this call returns false e.g. if a table
     * was already prepared but hasn't reached readiness yet).
     *
     * Uses callback instead of future because preparation and callback happen on the same thread.
     */

    public abstract void openEarly(Consumer<SSTableReader> doWhenReady);

    /**
     * Open the resultant SSTableReader once it has been fully written, but before the
     * _set_ of tables that are being written together as one atomic operation are all ready
     */
    public abstract SSTableReader openFinalEarly();

    protected abstract SSTableReader openFinal(SSTableReader.OpenReason openReason);

    public SSTableReader finish(boolean openResult)
    {
        this.setOpenResult(openResult);
        txnProxy.finish();
        observers.forEach(SSTableFlushObserver::complete);
        return finished();
    }

    /**
     * Open the resultant SSTableReader once it has been fully written, and all related state
     * is ready to be finalised including other sstables being written involved in the same operation
     */
    public SSTableReader finished()
    {
        txnProxy.finalReaderAccessed = true;
        return txnProxy.finalReader;
    }

    // finalise our state on disk, including renaming
    public final void prepareToCommit()
    {
        txnProxy.prepareToCommit();
    }

    public final Throwable commit(Throwable accumulate)
    {
        try
        {
            return txnProxy.commit(accumulate);
        }
        finally
        {
            observers.forEach(SSTableFlushObserver::complete);
        }
    }

    public final Throwable abort(Throwable accumulate)
    {
        return txnProxy.abort(accumulate);
    }

    public final void close()
    {
        txnProxy.close();
    }

    public final void abort()
    {
        txnProxy.abort();
    }

    protected Map<MetadataType, MetadataComponent> finalizeMetadata()
    {
        return metadataCollector.finalizeMetadata(getPartitioner().getClass().getCanonicalName(),
                                                  metadata().params.bloomFilterFpChance,
                                                  repairedAt,
                                                  pendingRepair,
                                                  isTransient,
                                                  header);
    }

    protected StatsMetadata statsMetadata()
    {
        return (StatsMetadata) finalizeMetadata().get(MetadataType.STATS);
    }

    public void releaseMetadataOverhead()
    {
        metadataCollector.release();
    }

    /**
     * Parameters for calculating the expected size of an SSTable. Exposed on memtable flush sets (i.e. collected
     * subsets of a memtable that will be written to sstables).
     */
    public interface SSTableSizeParameters
    {
        long partitionCount();
        long partitionKeysSize();
        long dataSize();
    }

    public static abstract class Factory
    {
        public abstract long estimateSize(SSTableSizeParameters parameters);

        public abstract SSTableWriter open(Descriptor descriptor,
                                           long keyCount,
                                           long repairedAt,
                                           TimeUUID pendingRepair,
                                           boolean isTransient,
                                           TableMetadataRef metadata,
                                           MetadataCollector metadataCollector,
                                           SerializationHeader header,
                                           Collection<SSTableFlushObserver> observers,
                                           LifecycleNewTracker lifecycleNewTracker);
    }

    public static void guardCollectionSize(TableMetadata metadata, DecoratedKey partitionKey, Unfiltered unfiltered)
    {
        if (!Guardrails.collectionSize.enabled() && !Guardrails.itemsPerCollection.enabled())
            return;

        if (!unfiltered.isRow() || SchemaConstants.isSystemKeyspace(metadata.keyspace))
            return;

        Row row = (Row) unfiltered;
        for (ColumnMetadata column : row.columns())
        {
            if (!column.type.isCollection() || !column.type.isMultiCell())
                continue;

            ComplexColumnData cells = row.getComplexColumnData(column);
            if (cells == null)
                continue;

            ComplexColumnData liveCells = cells.purge(DeletionPurger.PURGE_ALL, FBUtilities.nowInSeconds());
            if (liveCells == null)
                continue;

            int cellsSize = liveCells.dataSize();
            int cellsCount = liveCells.cellsCount();

            if (!Guardrails.collectionSize.triggersOn(cellsSize, null) &&
                !Guardrails.itemsPerCollection.triggersOn(cellsCount, null))
                continue;

            String keyString = metadata.primaryKeyAsCQLLiteral(partitionKey.getKey(), row.clustering());
            String msg = String.format("%s in row %s in table %s",
                                       column.name.toString(),
                                       keyString,
                                       metadata);
            Guardrails.collectionSize.guard(cellsSize, msg, true, null);
            Guardrails.itemsPerCollection.guard(cellsCount, msg, true, null);
        }
    }

    // due to lack of multiple inheritance, we use an inner class to proxy our Transactional implementation details
    protected class TransactionalProxy extends AbstractTransactional
    {
        // should be set during doPrepare()
        private final Supplier<ImmutableList<Transactional>> transactionals;

        private SSTableReader finalReader;
        private boolean openResult;
        private boolean finalReaderAccessed;

        public TransactionalProxy(Supplier<ImmutableList<Transactional>> transactionals)
        {
            this.transactionals = transactionals;
        }

        // finalise our state on disk, including renaming
        protected void doPrepare()
        {
            transactionals.get().forEach(Transactional::prepareToCommit);
            new StatsComponent(finalizeMetadata()).save(descriptor);

            // save the table of components
            TOCComponent.appendTOC(descriptor, components);

            if (openResult)
                finalReader = openFinal(SSTableReader.OpenReason.NORMAL);
        }

        protected Throwable doCommit(Throwable accumulate)
        {
            for (Transactional t : transactionals.get().reverse())
                accumulate = t.commit(accumulate);

            return accumulate;
        }

        protected Throwable doAbort(Throwable accumulate)
        {
            for (Transactional t : transactionals.get())
                accumulate = t.abort(accumulate);

            if (!finalReaderAccessed && finalReader != null)
            {
                accumulate = Throwables.perform(accumulate, () -> finalReader.selfRef().release());
                finalReader = null;
                finalReaderAccessed = false;
            }

            return accumulate;
        }

        @Override
        protected Throwable doPostCleanup(Throwable accumulate)
        {
            accumulate = super.doPostCleanup(accumulate);
            accumulate = Throwables.close(accumulate, Collections.singleton(mmappedRegionsCache));
            return accumulate;
        }
    }
}
