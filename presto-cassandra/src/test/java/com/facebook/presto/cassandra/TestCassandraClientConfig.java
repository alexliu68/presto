/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.cassandra;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestCassandraClientConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(CassandraClientConfig.class)
                .setLimitForPartitionKeySelect(200)
                .setFetchSizeForPartitionKeySelect(20_000)
                .setMaxSchemaRefreshThreads(10)
                .setSchemaCacheTtl(new Duration(1, TimeUnit.HOURS))
                .setSchemaRefreshInterval(new Duration(2, TimeUnit.MINUTES))
                .setFetchSize(5_000)
                .setConsistencyLevel(ConsistencyLevel.ONE)
                .setContactPoints("")
                .setNativeProtocolPort(9042)
                .setSplitSize(1_024)
                .setPartitioner("Murmur3Partitioner")
                .setThriftPort(9160)
                .setTransportFactoryOptions(null)
                .setThriftConnectionFactoryClassName("org.apache.cassandra.thrift.TFramedTransportFactory"));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("cassandra.limit-for-partition-key-select", "100")
                .put("cassandra.fetch-size-for-partition-key-select", "500")
                .put("cassandra.max-schema-refresh-threads", "2")
                .put("cassandra.schema-cache-ttl", "2h")
                .put("cassandra.schema-refresh-interval", "30m")
                .put("cassandra.contact-points", "host1,host2")
                .put("cassandra.native-protocol-port", "9999")
                .put("cassandra.fetch-size", "10000")
                .put("cassandra.consistency-level", "TWO")
                .put("cassandra.split-size", "1025")
                .put("cassandra.thrift-port", "9161")
                .put("cassandra.partitioner", "RandomPartitioner")
                .put("cassandra.transport-factory-options", "a=b")
                .put("cassandra.thrift-connection-factory-class", "org.apache.cassandra.thrift.TFramedTransportFactory1")
                .build();

        CassandraClientConfig expected = new CassandraClientConfig()
                .setLimitForPartitionKeySelect(100)
                .setFetchSizeForPartitionKeySelect(500)
                .setMaxSchemaRefreshThreads(2)
                .setSchemaCacheTtl(new Duration(2, TimeUnit.HOURS))
                .setSchemaRefreshInterval(new Duration(30, TimeUnit.MINUTES))
                .setContactPoints("host1", "host2")
                .setNativeProtocolPort(9999)
                .setFetchSize(10_000)
                .setConsistencyLevel(ConsistencyLevel.TWO)
                .setSplitSize(1_025)
                .setThriftPort(9161)
                .setPartitioner("RandomPartitioner")
                .setTransportFactoryOptions("a=b")
                .setThriftConnectionFactoryClassName("org.apache.cassandra.thrift.TFramedTransportFactory1");

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
