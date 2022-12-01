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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.config.Config.FlushCompression;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableBuilder;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.utils.TimeUUID;

public abstract class SSTableWriterBuilder<W extends SSTableWriter, B extends SSTableWriterBuilder<W, B>> extends SSTableBuilder<W, B>
{
    private SequentialWriterOption writerOptions = SequentialWriterOption.newBuilder()
                                                                         .trickleFsync(DatabaseDescriptor.getTrickleFsync())
                                                                         .trickleFsyncByteInterval(DatabaseDescriptor.getTrickleFsyncIntervalInKiB() * 1024)
                                                                         .build();
    private MetadataCollector metadataCollector;
    private long keyCount;
    private long repairedAt;
    private TimeUUID pendingRepair;
    private boolean transientSSTable;
    private SerializationHeader serializationHeader;
    private Collection<SSTableFlushObserver> flushObservers;
    private LifecycleNewTracker lifecycleNewTracker;
    private FlushCompression flushCompression = DatabaseDescriptor.getFlushCompression();

    public B setWriterOptions(SequentialWriterOption writerOptions)
    {
        this.writerOptions = writerOptions;
        return (B) this;
    }

    public B setMetadataCollector(MetadataCollector metadataCollector)
    {
        this.metadataCollector = metadataCollector;
        return (B) this;
    }

    public B setKeyCount(long keyCount)
    {
        this.keyCount = keyCount;
        return (B) this;
    }

    public B setRepairedAt(long repairedAt)
    {
        this.repairedAt = repairedAt;
        return (B) this;
    }

    public B setPendingRepair(TimeUUID pendingRepair)
    {
        this.pendingRepair = pendingRepair;
        return (B) this;
    }

    public B setTransientSSTable(boolean transientSSTable)
    {
        this.transientSSTable = transientSSTable;
        return (B) this;
    }

    public B setSerializationHeader(SerializationHeader serializationHeader)
    {
        this.serializationHeader = serializationHeader;
        return (B) this;
    }

    public B setFlushObservers(Collection<SSTableFlushObserver> flushObservers)
    {
        this.flushObservers = ImmutableList.copyOf(flushObservers);
        return (B) this;
    }

    public B setLifecycleNewTracker(LifecycleNewTracker lifecycleNewTracker)
    {
        this.lifecycleNewTracker = lifecycleNewTracker;
        return (B) this;
    }

    public B setFlushCompression(FlushCompression flushCompression)
    {
        this.flushCompression = flushCompression;
        return (B) this;
    }

    public B setDefaultComponents()
    {
        Set<Component> components = new HashSet<>(descriptor.getFormat().writeComponents());

        if (FilterComponent.shouldUseBloomFilter(getTableMetadataRef().getLocal().params.bloomFilterFpChance))
        {
            components.add(Component.FILTER);
        }

        if (getTableMetadataRef().getLocal().params.compression.isEnabled())
        {
            components.add(Component.COMPRESSION_INFO);
        }
        else
        {
            // it would feel safer to actually add this component later in maybeWriteDigest(),
            // but the components are unmodifiable after construction
            components.add(Component.CRC);
        }

        setComponents(components);

        return (B) this;
    }

    public SequentialWriterOption getWriterOptions()
    {
        return writerOptions;
    }

    public MetadataCollector getMetadataCollector()
    {
        return metadataCollector;
    }

    public long getKeyCount()
    {
        return keyCount;
    }

    public long getRepairedAt()
    {
        return repairedAt;
    }

    public TimeUUID getPendingRepair()
    {
        return pendingRepair;
    }

    public boolean isTransientSSTable()
    {
        return transientSSTable;
    }

    public SerializationHeader getSerializationHeader()
    {
        return serializationHeader;
    }

    public Collection<SSTableFlushObserver> getFlushObservers()
    {
        return flushObservers;
    }

    public LifecycleNewTracker getLifecycleNewTracker()
    {
        return lifecycleNewTracker;
    }

    public FlushCompression getFlushCompression()
    {
        return flushCompression;
    }

    public SSTableWriterBuilder(Descriptor descriptor)
    {
        super(descriptor);
    }

    public W build()
    {
        if (getComponents() == null)
            setDefaultComponents();

        SSTable.validateRepairedMetadata(getRepairedAt(), getPendingRepair(), isTransientSSTable());

        return buildInternal();
    }

    protected abstract W buildInternal();
}
