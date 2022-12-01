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

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableBuilder;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.apache.cassandra.io.sstable.format.SSTableReader.OpenReason.NORMAL;

public abstract class SSTableReaderBuilder<R extends SSTableReader, B extends SSTableReaderBuilder<R, B>> extends SSTableBuilder<R, B>
{
    private final static Logger logger = LoggerFactory.getLogger(SSTableReaderBuilder.class);

    private long maxDataAge;
    private StatsMetadata statsMetadata;
    private SSTableReader.OpenReason openReason;
    private SerializationHeader serializationHeader;
    private FileHandle dataFile;
    private IFilter filter;
    private DecoratedKey first;
    private DecoratedKey last;
    private boolean suspected;
    private boolean online;

    public SSTableReaderBuilder(Descriptor descriptor)
    {
        super(descriptor);
    }

    public B setMaxDataAge(long maxDataAge)
    {
        Preconditions.checkArgument(maxDataAge >= 0);
        this.maxDataAge = maxDataAge;
        return (B) this;
    }

    public B setStatsMetadata(StatsMetadata statsMetadata)
    {
        Preconditions.checkNotNull(statsMetadata);
        this.statsMetadata = statsMetadata;
        return (B) this;
    }

    public B setOpenReason(SSTableReader.OpenReason openReason)
    {
        Preconditions.checkNotNull(openReason);
        this.openReason = openReason;
        return (B) this;
    }

    public B setSerializationHeader(SerializationHeader serializationHeader)
    {
        this.serializationHeader = serializationHeader;
        return (B) this;
    }

    public B setDataFile(FileHandle dataFile)
    {
        this.dataFile = dataFile;
        return (B) this;
    }

    public B setFilter(IFilter filter)
    {
        this.filter = filter;
        return (B) this;
    }

    public B setFirst(DecoratedKey first)
    {
        this.first = first != null ? SSTable.getMinimalKey(first) : null;
        return (B) this;
    }

    public B setLast(DecoratedKey last)
    {
        this.last = last != null ? SSTable.getMinimalKey(last) : null;
        return (B) this;
    }

    public B setSuspected(boolean suspected)
    {
        this.suspected = suspected;
        return (B) this;
    }

    public B setOnline(boolean online)
    {
        this.online = online;
        return (B) this;
    }

    public long getMaxDataAge()
    {
        return maxDataAge;
    }

    public StatsMetadata getStatsMetadata()
    {
        return statsMetadata;
    }

    public SSTableReader.OpenReason getOpenReason()
    {
        return openReason;
    }

    public SerializationHeader getSerializationHeader()
    {
        return serializationHeader;
    }

    public FileHandle getDataFile()
    {
        return dataFile;
    }

    public IFilter getFilter()
    {
        return filter;
    }

    public DecoratedKey getFirst()
    {
        return first;
    }

    public DecoratedKey getLast()
    {
        return last;
    }

    public boolean isSuspected()
    {
        return suspected;
    }

    public boolean isOnline()
    {
        return online;
    }

    public FileHandle.Builder defaultFileHandleBuilder(File file)
    {
        FileHandle.Builder builder = new FileHandle.Builder(file);
        builder.mmapped(getDiskAccessMode() == Config.DiskAccessMode.mmap);
        builder.withChunkCache(getChunkCache());
        return builder;
    }

    protected abstract void openComponents() throws IOException;

    protected abstract R buildInternal();

    public R build()
    {
        R reader = buildInternal();

        try
        {
            if (isSuspected())
                reader.markSuspect();

            reader.setup(isOnline());
        }
        catch (RuntimeException | Error ex)
        {
            JVMStabilityInspector.inspectThrowable(ex);
            reader.selfRef().release();
        }
        return reader;
    }

    public R open(boolean validate)
    {
        R reader = null;
        setOpenReason(NORMAL);
        setMaxDataAge(Clock.Global.currentTimeMillis());
        setOnline(online);

        if (getTableMetadataRef() == null)
            setDefaultTableMetadata();

        if (getComponents() == null)
            setComponents(TOCComponent.loadOrCreate(descriptor));

        // Minimum components without which we can't do anything
        assert getComponents().contains(Component.DATA) : "Data component is missing for sstable " + descriptor;
        assert !validate || getComponents().containsAll(descriptor.getFormat().primaryComponents()) : "Primary index component is missing for sstable " + descriptor;

        CompressionInfoComponent.verifyCompressionInfoExistenceIfApplicable(descriptor, getComponents());

        try
        {
            long t0 = Clock.Global.currentTimeMillis();

            openComponents();

            if (logger.isTraceEnabled())
                logger.trace("SSTable {} loaded in {}ms", descriptor, Clock.Global.currentTimeMillis() - t0);

            reader = build();

            if (validate)
                reader.validate();

            if (logger.isTraceEnabled() && reader.getKeyCache() != null)
                logger.trace("key cache contains {}/{} keys", reader.getKeyCache().size(), reader.getKeyCache().getCapacity());

            return reader;
        }
        catch (RuntimeException | IOException | Error ex)
        {
            if (reader != null)
                reader.selfRef().release();

            JVMStabilityInspector.inspectThrowable(ex);

            if (ex instanceof CorruptSSTableException)
                throw (CorruptSSTableException) ex;

            throw new CorruptSSTableException(ex, descriptor.fileFor(Component.DATA));
        }
    }
}
