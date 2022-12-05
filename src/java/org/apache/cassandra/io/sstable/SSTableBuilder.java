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

import java.util.Collection;
import java.util.Objects;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.schema.TableMetadataRef;

public abstract class SSTableBuilder<S extends SSTable, B extends SSTableBuilder<S, B>>
{
    public SSTableBuilder(Descriptor descriptor)
    {
        this.descriptor = descriptor;
    }

    public final Descriptor descriptor;
    private Set<Component> components;
    private TableMetadataRef tableMetadataRef;

    public B setComponents(Collection<Component> components)
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

}
