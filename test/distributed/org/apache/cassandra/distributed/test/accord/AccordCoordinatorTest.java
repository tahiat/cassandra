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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.primitives.Unseekables;
import accord.topology.Topologies;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.impl.InstanceConfig;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.AccordTestUtils;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.distributed.action.GossipHelper.withProperty;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AccordCoordinatorTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordCoordinatorTest.class);

    @Override
    protected Logger logger()
    {
        return logger;
    }

    @BeforeClass
    public static void setupClass() throws IOException
    {
        AccordTestBase.setupClass();
        SHARED_CLUSTER.schemaChange("CREATE TYPE " + KEYSPACE + ".person (height int, age int)");
    }

    @Test
    public void testMultipleShards() throws Exception
    {
        String keyspace = "multipleShards";
        String currentTable = keyspace + ".tbl";
        List<String> ddls = Arrays.asList("CREATE KEYSPACE " + keyspace + " WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 1}",
                                          "CREATE TABLE " + currentTable + " (k int, c int, v int, primary key (k, c))");

        logger.info("Starting node 3");
        InstanceConfig node3Config = SHARED_CLUSTER.newInstanceConfig();
        IInvokableInstance node3 = SHARED_CLUSTER.bootstrap(node3Config);
        withProperty("cassandra.join_ring", false, () -> node3.startup(SHARED_CLUSTER));

        test(ddls, cluster -> {
            String query = "BEGIN TRANSACTION\n" +
                           "  INSERT INTO " + currentTable + " (k, c, v) VALUES (?, ?, ?);\n" +
                           "COMMIT TRANSACTION";
            SimpleQueryResult result = cluster.coordinator(3).executeWithResult(query, ConsistencyLevel.ANY, 0, 0, 1);
            assertFalse(result.hasNext());

            String check = "BEGIN TRANSACTION\n" +
                           "  SELECT * FROM " + currentTable + " WHERE k=? AND c=?;\n" +
                           "COMMIT TRANSACTION";
            result = cluster.coordinator(3).executeWithResult(check, ConsistencyLevel.ANY, 0, 0);
            assertTrue(result.hasNext());
        });
    }
}
