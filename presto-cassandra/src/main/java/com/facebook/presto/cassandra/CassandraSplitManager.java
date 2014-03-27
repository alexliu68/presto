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

import com.datastax.driver.core.Host;
import com.facebook.presto.cassandra.util.CassandraCqlUtils;
import com.facebook.presto.cassandra.util.HostAddressFactory;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.PartitionResult;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.SplitSource;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.airlift.log.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;

public class CassandraSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(ConnectorSplitManager.class);

    private final String connectorId;
    private final CassandraSession cassandraSession;
    private final CachingCassandraSchemaProvider schemaProvider;
    private final CassandraTokenSplitManager tokenSplitMgr;
    private final int partitionSizeForBatchSelect;

    @Inject
    public CassandraSplitManager(CassandraConnectorId connectorId,
            CassandraClientConfig cassandraClientConfig,
            CassandraSession cassandraSession,
            CachingCassandraSchemaProvider schemaProvider,
            CassandraTokenSplitManager tokenSplitMgr)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.schemaProvider = checkNotNull(schemaProvider, "schemaProvider is null");
        this.cassandraSession = checkNotNull(cassandraSession, "cassandraSession is null");
        this.tokenSplitMgr = tokenSplitMgr;
        this.partitionSizeForBatchSelect = cassandraClientConfig.getPartitionSizeForBatchSelect();
    }

    @Override
    public String getConnectorId()
    {
        return connectorId;
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof CassandraTableHandle && ((CassandraTableHandle) tableHandle).getConnectorId().equals(connectorId);
    }

    @Override
    public PartitionResult getPartitions(TableHandle tableHandle, TupleDomain tupleDomain)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(tupleDomain, "tupleDomain is null");
        CassandraTableHandle cassandraTableHandle = (CassandraTableHandle) tableHandle;
        CassandraTable table = schemaProvider.getTable(cassandraTableHandle);
        List<CassandraColumnHandle> partitionKeys = table.getPartitionKeyColumns();

        // fetch the partitions
        List<CassandraPartition> allPartitions = getAllPartitions(table, tupleDomain);
        log.debug("%s.%s #partitions: %d", cassandraTableHandle.getSchemaName(), cassandraTableHandle.getTableName(), allPartitions.size());

        // do a final pass to filter based on fields that could not be used to build the prefix
        List<Partition> partitions = FluentIterable.from(allPartitions)
                .filter(partitionMatches(tupleDomain))
                .filter(Partition.class)
                .toList();

        // All partition key domains will be fully evaluated, so we don't need to include those
        TupleDomain remainingTupleDomain = TupleDomain.none();
        if (!tupleDomain.isNone()) {
            if (partitions.size() == 1 && ((CassandraPartition) partitions.get(0)).isUnpartitioned()) {
                remainingTupleDomain = tupleDomain;
            }
            else {
                @SuppressWarnings({"rawtypes", "unchecked"})
                List<ColumnHandle> partitionColumns = (List) partitionKeys;
                remainingTupleDomain = TupleDomain.withColumnDomains(Maps.filterKeys(tupleDomain.getDomains(), not(in(partitionColumns))));
            }
        }

        // push down indexed column fixed value predicates only for unpartitioned partition which uses token range query
        if (partitions.size() == 1 && ((CassandraPartition) partitions.get(0)).isUnpartitioned()) {
            Map<ColumnHandle, Domain> domains = tupleDomain.getDomains();
            List<ColumnHandle> indexedColumns = Lists.newArrayList();
            // compose partitionId by using indexed column
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
                CassandraColumnHandle column = (CassandraColumnHandle) entry.getKey();
                Domain domain = entry.getValue();
                if (column.isIndexed() && domain.isSingleValue()) {
                    sb.append(CassandraCqlUtils.validColumnName(column.getName()))
                      .append(" = ")
                      .append(CassandraCqlUtils.cqlValue(entry.getValue().getSingleValue().toString(), column.getCassandraType()));
                    indexedColumns.add(column);
                    // Only onde indexed column predicate can be pushed down.
                    break;
                }
            }
            if (sb.length() > 0) {
                CassandraPartition partition = (CassandraPartition) partitions.get(0);
                TupleDomain filterIndexedColumn = TupleDomain.withColumnDomains(Maps.filterKeys(remainingTupleDomain.getDomains(), not(in(indexedColumns))));
                partitions = Lists.newArrayList();
                partitions.add(new CassandraPartition(partition.getKey(), sb.toString(), filterIndexedColumn, true));
                return new PartitionResult(partitions, filterIndexedColumn);
            }
        }
        return new PartitionResult(partitions, remainingTupleDomain);
    }

    private List<CassandraPartition> getAllPartitions(CassandraTable table, TupleDomain tupleDomain)
    {
        List<CassandraColumnHandle> partitionKeys = table.getPartitionKeyColumns();
        List<Comparable<?>> filterPrefix = new ArrayList<>();
        for (int i = 0; i < partitionKeys.size(); i++) {
            CassandraColumnHandle columnHandle = partitionKeys.get(i);

            // only add to prefix if all previous keys have a value
            if (filterPrefix.size() == i && !tupleDomain.isNone()) {
                Domain domain = tupleDomain.getDomains().get(columnHandle);
                if (domain != null && domain.getRanges().getRangeCount() == 1) {
                    // We intentionally ignore whether NULL is in the domain since partition keys can never be NULL
                    Range range = Iterables.getOnlyElement(domain.getRanges());
                    if (range.isSingleValue()) {
                        Comparable<?> value = range.getLow().getValue();
                        checkArgument(value instanceof Boolean || value instanceof String || value instanceof Double || value instanceof Long,
                                "Only Boolean, String, Double and Long partition keys are supported");
                        filterPrefix.add(value);
                    }
                }
            }
        }

        // fetch the partitions
        List<CassandraPartition> allPartitions = schemaProvider.getPartitions(table, filterPrefix);
        return allPartitions;
    }

    @Override
    public SplitSource getPartitionSplits(TableHandle tableHandle, List<Partition> partitions)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof CassandraTableHandle, "tableHandle is not an instance of CassandraTableHandle");
        CassandraTableHandle cassandraTableHandle = (CassandraTableHandle) tableHandle;

        checkNotNull(partitions, "partitions is null");
        if (partitions.isEmpty()) {
            return new FixedSplitSource(connectorId, ImmutableList.<Split>of());
        }
        for (Partition partition : partitions) {
            CassandraPartition cassandraPartition = (CassandraPartition) partition;
        }

        // if this is an unpartitioned table, split into equal ranges
        if (partitions.size() == 1) {
            Partition partition = partitions.get(0);
            checkArgument(partition instanceof CassandraPartition, "partitions are no CassandraPartitions");
            CassandraPartition cassandraPartition = (CassandraPartition) partition;

            if (cassandraPartition.isUnpartitioned() || cassandraPartition.isIndexedColumnPredicatePushdown()) {
                CassandraTable table = schemaProvider.getTable(cassandraTableHandle);
                List<Split> splits = getSplitsByTokenRange(table, cassandraPartition.getPartitionId());
                return new FixedSplitSource(connectorId, splits);
            }
        }

        return new FixedSplitSource(connectorId, getSplitsForPartitions(cassandraTableHandle, partitions));
    }

    private List<Split> getSplitsByTokenRange(CassandraTable table, String partitionId)
    {
        String schema = table.getTableHandle().getSchemaName();
        String tableName = table.getTableHandle().getTableName();
        String tokenExpression = table.getTokenExpression();

        ImmutableList.Builder<Split> builder = ImmutableList.builder();
        List<CassandraTokenSplitManager.TokenSplit> tokenSplits = null;
        try {
            tokenSplits = tokenSplitMgr.getSplits(schema, tableName);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        for (CassandraTokenSplitManager.TokenSplit tokenSplit : tokenSplits) {
            String condition = buildTokenCondition(tokenExpression, tokenSplit.getStartToken(), tokenSplit.getEndToken());
            List<HostAddress> addresses = new HostAddressFactory().AddressNamesToHostAddressList(tokenSplit.getHosts());
            CassandraSplit split = new CassandraSplit(connectorId, schema, tableName, partitionId, condition, addresses);
            builder.add(split);
        }

        return builder.build();
    }

    private static String buildTokenCondition(String tokenExpression, String startToken, String endToken)
    {
        return tokenExpression + " > " + startToken + " AND " + tokenExpression + " <= " + endToken;
    }

    private List<Split> getSplitsForPartitions(CassandraTableHandle cassTableHandle, List<Partition> partitions)
    {
        String schema = cassTableHandle.getSchemaName();
        String table = cassTableHandle.getTableName();
        HostAddressFactory hostAddressFactory = new HostAddressFactory();
        ImmutableList.Builder<Split> builder = ImmutableList.builder();

        // For single partition key column table, we can merge multiple partitions into a single split
        // by using IN CLAUSE in a single select query if the partitions have the same host list.
        // For multiple partition key columns table, we can't merge them into a single select query, so
        // keep them in a separate split.
        boolean siglePartitionKeyColumn = true;
        String partitionKeyColumnName = null;
        if (!partitions.isEmpty()) {
            siglePartitionKeyColumn = partitions.get(0).getTupleDomain().getNullableColumnDomains().size() == 1;
            if (siglePartitionKeyColumn) {
                String partitionId = partitions.get(0).getPartitionId();
                partitionKeyColumnName = partitionId.substring(0, partitionId.lastIndexOf("=") - 1);
            }
        }
        Map<Set<String>, Set<String>> hostsToPartitionKeys = Maps.newHashMap();
        Map<Set<String>, List<HostAddress>> hostMap = Maps.newHashMap();

        for (Partition partition : partitions) {
            checkArgument(partition instanceof CassandraPartition, "partitions are no CassandraPartitions");
            CassandraPartition cassandraPartition = (CassandraPartition) partition;

            Set<Host> hosts = cassandraSession.getReplicas(schema, cassandraPartition.getKeyAsByteBuffer());
            List<HostAddress> addresses = hostAddressFactory.toHostAddressList(hosts);
            if (siglePartitionKeyColumn) {
                // host ip addresses
                Builder<String> sb = ImmutableSet.builder();
                for (HostAddress address : addresses) {
                    sb.add(address.getHostText());
                }
                Set<String> hostAddresses = sb.build();
                // partition key values
                Set<String> values = hostsToPartitionKeys.get(hostAddresses);
                if (values == null) {
                    values = Sets.newHashSet();
                }
                String partitionId = cassandraPartition.getPartitionId();
                values.add(partitionId.substring(partitionId.lastIndexOf("=") + 2));
                hostsToPartitionKeys.put(hostAddresses, values);
                hostMap.put(hostAddresses, addresses);
            }
            else {
                CassandraSplit split = new CassandraSplit(connectorId, schema, table, cassandraPartition.getPartitionId(), null, addresses);
                builder.add(split);
            }
        }
        if (siglePartitionKeyColumn) {
            for (Map.Entry<Set<String>, Set<String>> entry : hostsToPartitionKeys.entrySet()) {
                StringBuilder sb = new StringBuilder(partitionSizeForBatchSelect);
                int size = 0;
                for (String value : entry.getValue()) {
                    if (size > 0) {
                        sb.append(",");
                    }
                    sb.append(value);
                    size++;
                    if (size > partitionSizeForBatchSelect) {
                        String partitionId = String.format("%s in (%s)", partitionKeyColumnName, sb.toString());
                        CassandraSplit split = new CassandraSplit(connectorId, schema, table, partitionId, null, hostMap.get(entry.getKey()));
                        builder.add(split);
                        size = 0;
                        sb.setLength(0);
                        sb.trimToSize();
                    }
                }
                if (size > 0) {
                    String partitionId = String.format("%s in (%s)", partitionKeyColumnName, sb.toString());
                    CassandraSplit split = new CassandraSplit(connectorId, schema, table, partitionId, null, hostMap.get(entry.getKey()));
                    builder.add(split);
                }
            }
        }
        return builder.build();
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("clientId", connectorId)
                .toString();
    }

    public static Predicate<CassandraPartition> partitionMatches(final TupleDomain tupleDomain)
    {
        return new Predicate<CassandraPartition>()
        {
            @Override
            public boolean apply(CassandraPartition partition)
            {
                return tupleDomain.overlaps(partition.getTupleDomain());
            }
        };
    }
}
