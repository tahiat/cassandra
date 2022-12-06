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
import java.util.Collection;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class SortedTablePartitionWriter implements AutoCloseable
{
    protected final UnfilteredSerializer unfilteredSerializer;

    private final SerializationHeader header;
    private final SequentialWriter writer;
    private final Collection<SSTableFlushObserver> observers;
    private final SerializationHelper helper;
    private final int version;

    private long previousRowStart;
    private long initialPosition;
    private long headerLength;

    protected long startPosition;
    protected int written;

    protected ClusteringPrefix<?> firstClustering;
    protected ClusteringPrefix<?> lastClustering;

    protected DeletionTime openMarker = DeletionTime.LIVE;
    protected DeletionTime startOpenMarker = DeletionTime.LIVE;

    protected SortedTablePartitionWriter(SerializationHeader header,
                                         SequentialWriter writer,
                                         Version version,
                                         Collection<SSTableFlushObserver> observers)
    {
        this.header = header;
        this.writer = writer;
        this.observers = observers;
        this.unfilteredSerializer = UnfilteredSerializer.serializer;
        this.helper = new SerializationHelper(header);
        this.version = version.correspondingMessagingVersion();
    }

    protected void reset()
    {
        this.initialPosition = writer.position();
        this.startPosition = -1;
        this.previousRowStart = 0;
        this.written = 0;
        this.firstClustering = null;
        this.lastClustering = null;
        this.openMarker = DeletionTime.LIVE;
        this.headerLength = -1;
    }

    public long getHeaderLength()
    {
        return headerLength;
    }

    @VisibleForTesting
    public void writePartition(UnfilteredRowIterator iterator) throws IOException
    {
        reset();

        writePartitionHeader(iterator.partitionKey(), iterator.partitionLevelDeletion(), iterator.staticRow());
        this.headerLength = writer.position() - initialPosition;

        while (iterator.hasNext())
        {
            Unfiltered unfiltered = iterator.next();
            SSTableWriter.guardCollectionSize(iterator.metadata(), iterator.partitionKey(), unfiltered);
            add(unfiltered);
        }

        finish();
    }

    protected void writePartitionHeader(DecoratedKey key, DeletionTime partitionLevelDeletion, Row staticRow) throws IOException
    {
        ByteBufferUtil.writeWithShortLength(key.getKey(), writer);

        DeletionTime.serializer.serialize(partitionLevelDeletion, writer);
        if (!observers.isEmpty())
            observers.forEach((o) -> o.partitionLevelDeletion(partitionLevelDeletion));

        if (header.hasStatic() && !staticRow.isEmpty())
        {
            UnfilteredSerializer.serializer.serializeStaticRow(staticRow, helper, writer, version);
            if (!observers.isEmpty())
                observers.forEach(o -> o.staticRow(staticRow));
        }
    }

    protected void add(Unfiltered unfiltered) throws IOException
    {
        long pos = currentPosition();

        if (firstClustering == null)
        {
            // Beginning of an index block. Remember the start and position
            firstClustering = unfiltered.clustering();
            startOpenMarker = openMarker;
            startPosition = pos;
        }

        long unfilteredPosition = writer.position();
        unfilteredSerializer.serialize(unfiltered, helper, writer, pos - previousRowStart, version);

        // notify observers about each new row
        if (!observers.isEmpty())
            observers.forEach(o -> o.nextUnfilteredCluster(unfiltered));

        lastClustering = unfiltered.clustering();
        previousRowStart = pos;
        ++written;

        if (unfiltered.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
        {
            RangeTombstoneMarker marker = (RangeTombstoneMarker) unfiltered;
            openMarker = marker.isOpen(false) ? marker.openDeletionTime(false) : DeletionTime.LIVE;
        }
    }

    protected long finish() throws IOException
    {
        long endPosition = currentPosition();
        unfilteredSerializer.writeEndOfPartition(writer);

        return endPosition;
    }

    protected long currentPosition()
    {
        return writer.position() - initialPosition;
    }
}
