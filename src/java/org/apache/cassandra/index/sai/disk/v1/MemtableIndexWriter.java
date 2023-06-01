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
package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.LongArrayList;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.MemtableTermsIterator;
import org.apache.cassandra.index.sai.disk.PerIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.hnsw.CassandraOnHeapHnsw;
import org.apache.cassandra.index.sai.disk.v1.kdtree.ImmutableOneDimPointValues;
import org.apache.cassandra.index.sai.disk.v1.kdtree.NumericIndexWriter;
import org.apache.cassandra.index.sai.disk.v1.trie.InvertedIndexWriter;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.util.StringHelper;

/**
 * Column index writer that flushes indexed data directly from the corresponding Memtable index, without buffering index
 * data in memory.
 */
public class MemtableIndexWriter implements PerIndexWriter
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final IndexDescriptor indexDescriptor;
    private final IndexContext indexContext;
    private final MemtableIndex memtableIndex;
    private final RowMapping rowMapping;

    private final int nowInSec = FBUtilities.nowInSeconds();
    private final CassandraOnHeapHnsw<PrimaryKey> graphIndex;

    public MemtableIndexWriter(MemtableIndex memtableIndex,
                               IndexDescriptor indexDescriptor,
                               IndexContext indexContext,
                               RowMapping rowMapping)
    {
        assert rowMapping != null && rowMapping != RowMapping.DUMMY : "Row mapping must exist during FLUSH.";

        this.indexDescriptor = indexDescriptor;
        this.indexContext = indexContext;
        this.memtableIndex = memtableIndex;
        this.rowMapping = rowMapping;

        if (indexContext.isVector())
            graphIndex = new CassandraOnHeapHnsw<>(indexContext.getValidator(), indexContext.getIndexWriterConfig());
        else
            graphIndex = null;
    }

    @Override
    public IndexContext indexContext()
    {
        return indexContext;
    }

    @Override
    public void addRow(PrimaryKey key, Row row, long sstableRowId)
    {
        // Memtable indexes are flushed directly to disk with the aid of a mapping between primary
        // keys and row IDs in the flushing SSTable. This writer, therefore, does nothing in
        // response to the flushing of individual rows.
        if (indexContext.isVector())
        {
            ByteBuffer value = indexContext.getValueOf(key.partitionKey(), row, nowInSec);
            if (value != null)
                graphIndex.add(value, key);
        }
    }

    @Override
    public void abort(Throwable cause)
    {
        logger.warn(indexContext.logMessage("Aborting index memtable flush for {}..."), indexDescriptor.descriptor, cause);
        indexDescriptor.deleteColumnIndex(indexContext);
    }

    @Override
    public void complete(Stopwatch stopwatch) throws IOException
    {
        long start = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        try
        {
            if (!rowMapping.hasRows() || (memtableIndex == null) || memtableIndex.isEmpty())
            {
                logger.debug(indexContext.logMessage("No indexed rows to flush from SSTable {}."), indexDescriptor.descriptor);
                // Write a completion marker even though we haven't written anything to the index
                // so we won't try to build the index again for the SSTable
                indexDescriptor.createComponentOnDisk(IndexComponent.COLUMN_COMPLETION_MARKER, indexContext);
                return;
            }

            final DecoratedKey minKey = rowMapping.minKey.partitionKey();
            final DecoratedKey maxKey = rowMapping.maxKey.partitionKey();

            if (indexContext.isVector())
            {
                flushVectorIndex(minKey, maxKey, start, stopwatch);
            }
            else
            {
                final Iterator<Pair<ByteComparable, LongArrayList>> iterator = rowMapping.merge(memtableIndex);

                try (MemtableTermsIterator terms = new MemtableTermsIterator(memtableIndex.getMinTerm(), memtableIndex.getMaxTerm(), iterator))
                {
                    long cellCount = flush(minKey, maxKey, indexContext.getValidator(), terms, rowMapping.maxSegmentRowId);

                    completeIndexFlush(cellCount, start, stopwatch);
                }
            }
        }
        catch (Throwable t)
        {
            logger.error(indexContext.logMessage("Error while flushing index {}"), t.getMessage(), t);
            indexContext.getIndexMetrics().memtableIndexFlushErrors.inc();

            throw t;
        }
    }

    private long flush(DecoratedKey minKey, DecoratedKey maxKey, AbstractType<?> termComparator, MemtableTermsIterator terms, long maxSegmentRowId) throws IOException
    {
        long numRows;
        SegmentMetadata.ComponentMetadataMap indexMetas;

        if (TypeUtil.isLiteral(termComparator))
        {
            try (InvertedIndexWriter writer = new InvertedIndexWriter(indexDescriptor, indexContext, false))
            {
                indexMetas = writer.writeAll(terms);
                numRows = writer.getPostingsCount();
            }
        }
        else
        {
            try (NumericIndexWriter writer = new NumericIndexWriter(indexDescriptor,
                                                                    indexContext,
                                                                    TypeUtil.fixedSizeOf(termComparator),
                                                                    maxSegmentRowId,
                                                                    // Due to stale entries in IndexMemtable, we may have more indexed rows than num of rowIds.
                                                                    Integer.MAX_VALUE,
                                                                    indexContext.getIndexWriterConfig(),
                                                                    false))
            {
                indexMetas = writer.writeAll(ImmutableOneDimPointValues.fromTermEnum(terms, termComparator));
                numRows = writer.getPointCount();
            }
        }

        // If no rows were written we need to delete any created column index components
        // so that the index is correctly identified as being empty (only having a completion marker)
        if (numRows == 0)
        {
            indexDescriptor.deleteColumnIndex(indexContext);
            return 0;
        }

        // During index memtable flush, the data is sorted based on terms.
        SegmentMetadata metadata = new SegmentMetadata(0,
                                                       numRows,
                                                       terms.getMinSSTableRowId(),
                                                       terms.getMaxSSTableRowId(),
                                                       indexDescriptor.primaryKeyFactory.createPartitionKeyOnly(minKey),
                                                       indexDescriptor.primaryKeyFactory.createPartitionKeyOnly(maxKey),
                                                       terms.getMinTerm(),
                                                       terms.getMaxTerm(),
                                                       indexMetas);

        try (MetadataWriter writer = new MetadataWriter(indexDescriptor.openPerIndexOutput(IndexComponent.META, indexContext)))
        {
            SegmentMetadata.write(writer, Collections.singletonList(metadata));
        }

        return numRows;
    }

    private void flushVectorIndex(DecoratedKey minKey, DecoratedKey maxKey, long startTime, Stopwatch stopwatch) throws IOException
    {
        graphIndex.writeData(indexDescriptor, indexContext, rowMapping::get);

        completeIndexFlush(rowMapping.size(), startTime, stopwatch);

        SegmentMetadata.ComponentMetadataMap metadataMap = new SegmentMetadata.ComponentMetadataMap();

        // we don't care about root/offset/length for vector. segmentId is used in searcher
        Map<String, String> vectorConfigs = Map.of("SEGMENT_ID", ByteBufferUtil.bytesToHex(ByteBuffer.wrap(StringHelper.randomId())));
        metadataMap.put(IndexComponent.VECTOR, 0, 0, 0, vectorConfigs);

        SegmentMetadata metadata = new SegmentMetadata(0,
                                                       rowMapping.size(),
                                                       0,
                                                       rowMapping.maxSegmentRowId,
                                                       indexDescriptor.primaryKeyFactory.createPartitionKeyOnly(minKey),
                                                       indexDescriptor.primaryKeyFactory.createPartitionKeyOnly(maxKey),
                                                       ByteBufferUtil.bytes(0), // TODO by pass min max terms for vectors
                                                       ByteBufferUtil.bytes(0), // TODO by pass min max terms for vectors
                                                       metadataMap);

        try (MetadataWriter writer = new MetadataWriter(indexDescriptor.openPerIndexOutput(IndexComponent.META, indexContext)))
        {
            SegmentMetadata.write(writer, Collections.singletonList(metadata));
        }
    }

    private void completeIndexFlush(long cellCount, long startTime, Stopwatch stopwatch) throws IOException
    {
        indexDescriptor.createComponentOnDisk(IndexComponent.COLUMN_COMPLETION_MARKER, indexContext);

        indexContext.getIndexMetrics().memtableIndexFlushCount.inc();

        long elapsedTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        logger.debug(indexContext.logMessage("Completed flushing {} memtable index cells to SSTable {}. Duration: {} ms. Total elapsed: {} ms"),
                     cellCount,
                     indexDescriptor.descriptor,
                     elapsedTime - startTime,
                     elapsedTime);

        indexContext.getIndexMetrics().memtableFlushCellsPerSecond.update((long) (cellCount * 1000.0 / Math.max(1, elapsedTime - startTime)));
    }
}