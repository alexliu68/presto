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
                .setPartitionSizeForBatchSelect(100)
                .setThriftConnectionFactoryClassName("org.apache.cassandra.thrift.TFramedTransportFactory"));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("cassandra.limit_for_partition_key_select", "100")
                .put("cassandra.fetch_size_for_partition_key_select", "500")
                .put("cassandra.max_schema_refresh_threads", "2")
                .put("cassandra.schema_cache_ttl", "2h")
                .put("cassandra.schema_refresh_interval", "30m")
                .put("cassandra.contact_points", "host1,host2")
                .put("cassandra.native_protocol_port", "9999")
                .put("cassandra.fetch_size", "10000")
                .put("cassandra.consistency_level", "TWO")
                .put("cassandra.split_size", "1025")
                .put("cassandra.thrift_port", "9161")
                .put("cassandra.partitioner", "RandomPartitioner")
                .put("cassandra.transport_factory_options", "a=b")
                .put("cassandra.partition_size_for_batch_select", "90")
                .put("cassandra.thrift_connection_factory_class", "org.apache.cassandra.thrift.TFramedTransportFactory1")
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
                .setPartitionSizeForBatchSelect(90)
                .setThriftConnectionFactoryClassName("org.apache.cassandra.thrift.TFramedTransportFactory1");

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
