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

package org.apache.cassandra.io.sstable;

import java.util.Objects;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DiskOptimizationStrategy;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadataRef;

import static org.apache.cassandra.db.Directories.SECONDARY_INDEX_NAME_SEPARATOR;

public abstract class SSTableBuilder<S extends SSTable, B extends SSTableBuilder<S, B>>
{
    public SSTableBuilder(Descriptor descriptor)
    {
        this.descriptor = descriptor;
    }

    public final Descriptor descriptor;
    private Set<Component> components;
    private TableMetadataRef tableMetadataRef;
    private DiskOptimizationStrategy diskOptimizationStrategy = DatabaseDescriptor.getDiskOptimizationStrategy();
    private double diskOptimizationEstimatePercentile = DatabaseDescriptor.getDiskOptimizationEstimatePercentile();
    private Config.DiskAccessMode dataFileAccessMode = DatabaseDescriptor.getDiskAccessMode();

    public B setComponents(Set<Component> components)
    {
        Preconditions.checkArgument(components.stream().allMatch(Objects::nonNull));
        this.components = ImmutableSet.copyOf(components);
        return (B) this;
    }

    public B setTableMetadataRef(TableMetadataRef ref)
    {
        Preconditions.checkNotNull(ref);
        Preconditions.checkNotNull(ref.get());
        this.tableMetadataRef = ref;
        return (B) this;
    }

    public B setDiskOptimizationStrategy(DiskOptimizationStrategy diskOptimizationStrategy)
    {
        this.diskOptimizationStrategy = diskOptimizationStrategy;
        return (B) this;
    }

    public B setDiskOptimizationEstimatePercentile(double diskOptimizationEstimatePercentile)
    {
        this.diskOptimizationEstimatePercentile = diskOptimizationEstimatePercentile;
        return (B) this;
    }

    public B setDataFileAccessMode(Config.DiskAccessMode dataFileAccessMode)
    {
        this.dataFileAccessMode = dataFileAccessMode;
        return (B) this;
    }

    public Descriptor getDescriptor()
    {
        return descriptor;
    }

    public Set<Component> getComponents()
    {
        return components;
    }

    public TableMetadataRef getTableMetadataRef()
    {
        return tableMetadataRef;
    }

    public DiskOptimizationStrategy getDiskOptimizationStrategy()
    {
        return diskOptimizationStrategy;
    }

    public double getDiskOptimizationEstimatePercentile()
    {
        return diskOptimizationEstimatePercentile;
    }

    public Config.DiskAccessMode getDataFileAccessMode()
    {
        return dataFileAccessMode;
    }

    public B setDefaultTableMetadata()
    {
        TableMetadataRef metadata;
        if (descriptor.cfname.contains(SECONDARY_INDEX_NAME_SEPARATOR))
        {
            int i = descriptor.cfname.indexOf(SECONDARY_INDEX_NAME_SEPARATOR);
            String indexName = descriptor.cfname.substring(i + 1);
            metadata = Schema.instance.getIndexTableMetadataRef(descriptor.ksname, indexName);
            if (metadata == null)
                throw new AssertionError("Could not find index metadata for index cf " + i);
        }
        else
        {
            metadata = Schema.instance.getTableMetadataRef(descriptor.ksname, descriptor.cfname);
        }
        return setTableMetadataRef(metadata);
    }

}
