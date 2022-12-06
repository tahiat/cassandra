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

import java.util.Set;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReaderLoadingBuilder;
import org.apache.cassandra.schema.TableMetadataRef;

class BigTableReaderFactory implements SSTableReader.Factory<BigTableReader, BigTableReaderBuilder>
{
    public final BigTableOptions options;
    public final ChunkCache chunkCache;

    public BigTableReaderFactory(BigTableOptions options, ChunkCache chunkCache)
    {
        this.options = options;
        this.chunkCache = chunkCache;
    }

    public BigTableReaderFactory()
    {
        this(new BigTableOptions(), ChunkCache.instance);
    }

    @Override
    public BigTableReaderBuilder builder(Descriptor descriptor)
    {
        return new BigTableReaderBuilder(descriptor);
    }

    @Override
    public SSTableReaderLoadingBuilder<BigTableReader, BigTableReaderBuilder> builder(Descriptor descriptor,
                                                                                      TableMetadataRef tableMetadataRef,
                                                                                      Set<Component> components)
    {
        return new BigSSTableReaderLoadingBuilder(descriptor, components, tableMetadataRef, options, chunkCache);
    }
}
