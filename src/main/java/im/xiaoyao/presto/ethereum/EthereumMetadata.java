package im.xiaoyao.presto.ethereum;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;

import javax.inject.Inject;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static im.xiaoyao.presto.ethereum.EthereumHandleResolver.convertColumnHandle;
import static im.xiaoyao.presto.ethereum.EthereumHandleResolver.convertTableHandle;
import static java.util.Objects.requireNonNull;

public class EthereumMetadata implements ConnectorMetadata {
    private static final Logger log = Logger.get(EthereumMetadata.class);

    private static final String DEFAULT_SCHEMA = "default";
    private static final int H8_BYTE_HASH_STRING_LENGTH = 2 + 8 * 2;
    private static final int H32_BYTE_HASH_STRING_LENGTH = 2 + 32 * 2;
    private static final int H256_BYTE_HASH_STRING_LENGTH = 2 + 256 * 2;
    private static final int H20_BYTE_HASH_STRING_LENGTH = 2 + 20 * 2;

    private final String connectorId;
    private final Web3j web3j;

    @Inject
    public EthereumMetadata(
            EthereumConnectorId connectorId,
            EthereumWeb3jProvider provider
    ) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.web3j = requireNonNull(provider, "provider is null").getWeb3j();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return Collections.singletonList(DEFAULT_SCHEMA);
    }

    @Override
    public EthereumTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName) {
        if (EthereumTable.BLOCK.getName().equals(schemaTableName.getTableName())) {
            return new EthereumTableHandle(connectorId, DEFAULT_SCHEMA, EthereumTable.BLOCK.getName());
        } else if (EthereumTable.TRANSACTION.getName().equals(schemaTableName.getTableName())) {
            return new EthereumTableHandle(connectorId, DEFAULT_SCHEMA, EthereumTable.TRANSACTION.getName());
        } else {
            throw new IllegalArgumentException("Unknown Table Name " + schemaTableName.getTableName());
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle) {
        return getTableMetadata(convertTableHandle(tableHandle).toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        return ImmutableList.of(new SchemaTableName(DEFAULT_SCHEMA, EthereumTable.BLOCK.getName()),
                new SchemaTableName(DEFAULT_SCHEMA, EthereumTable.TRANSACTION.getName()));
    }

    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        EthereumTableHandle ethereumTableHandle = convertTableHandle(tableHandle);
        String tableName = ethereumTableHandle.getTableName();

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        if (EthereumTable.BLOCK.getName().equals(tableName)) {
            columnHandles.put("block_number", new EthereumColumnHandle(connectorId, index++, "block_number", BigintType.BIGINT));
            columnHandles.put("block_hash", new EthereumColumnHandle(connectorId, index++, "block_hash", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            columnHandles.put("block_parentHash", new EthereumColumnHandle(connectorId, index++, "block_parentHash", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            columnHandles.put("block_nonce", new EthereumColumnHandle(connectorId, index++, "block_nonce", VarcharType.createVarcharType(H8_BYTE_HASH_STRING_LENGTH)));
            columnHandles.put("block_sha3Uncles", new EthereumColumnHandle(connectorId, index++, "block_sha3Uncles", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            columnHandles.put("block_logsBloom", new EthereumColumnHandle(connectorId, index++, "block_logsBloom", VarcharType.createVarcharType(H256_BYTE_HASH_STRING_LENGTH)));
            columnHandles.put("block_transactionsRoot", new EthereumColumnHandle(connectorId, index++, "block_transactionsRoot", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            columnHandles.put("block_stateRoot", new EthereumColumnHandle(connectorId, index++, "block_stateRoot", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            columnHandles.put("block_miner", new EthereumColumnHandle(connectorId, index++, "block_miner", VarcharType.createVarcharType(H20_BYTE_HASH_STRING_LENGTH)));
            columnHandles.put("block_difficulty", new EthereumColumnHandle(connectorId, index++, "block_difficulty", BigintType.BIGINT));
            columnHandles.put("block_totalDifficulty", new EthereumColumnHandle(connectorId, index++, "block_totalDifficulty", BigintType.BIGINT));
            columnHandles.put("block_size", new EthereumColumnHandle(connectorId, index++, "block_size", IntegerType.INTEGER));
            columnHandles.put("block_extraData", new EthereumColumnHandle(connectorId, index++, "block_extraData", VarcharType.VARCHAR));
            columnHandles.put("block_gasLimit", new EthereumColumnHandle(connectorId, index++, "block_gasLimit", DoubleType.DOUBLE));
            columnHandles.put("block_gasUsed", new EthereumColumnHandle(connectorId, index++, "block_gasUsed", DoubleType.DOUBLE));
            columnHandles.put("block_timestamp", new EthereumColumnHandle(connectorId, index++, "block_timestamp", BigintType.BIGINT));
            columnHandles.put("block_transactions", new EthereumColumnHandle(connectorId, index++, "block_transactions", new ArrayType(VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH))));
            columnHandles.put("block_uncles", new EthereumColumnHandle(connectorId, index++, "block_uncles", new ArrayType(VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH))));
        } else if (EthereumTable.TRANSACTION.getName().equals(tableName)) {
            columnHandles.put("tx_hash", new EthereumColumnHandle(connectorId, index++, "tx_hash", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            columnHandles.put("tx_nonce", new EthereumColumnHandle(connectorId, index++, "tx_nonce", BigintType.BIGINT));
            columnHandles.put("tx_blockHash", new EthereumColumnHandle(connectorId, index++, "tx_blockHash", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            columnHandles.put("tx_blockNumber", new EthereumColumnHandle(connectorId, index++, "tx_blockNumber", BigintType.BIGINT));
            columnHandles.put("tx_transactionIndex", new EthereumColumnHandle(connectorId, index++, "tx_transactionIndex", IntegerType.INTEGER));
            columnHandles.put("tx_from", new EthereumColumnHandle(connectorId, index++, "tx_from", VarcharType.createVarcharType(H20_BYTE_HASH_STRING_LENGTH)));
            columnHandles.put("tx_to", new EthereumColumnHandle(connectorId, index++, "tx_to", VarcharType.createVarcharType(H20_BYTE_HASH_STRING_LENGTH)));
            columnHandles.put("tx_value", new EthereumColumnHandle(connectorId, index++, "tx_value", DoubleType.DOUBLE));
            columnHandles.put("tx_gas", new EthereumColumnHandle(connectorId, index++, "tx_gas", DoubleType.DOUBLE));
            columnHandles.put("tx_gasPrice", new EthereumColumnHandle(connectorId, index++, "tx_gasPrice", DoubleType.DOUBLE));
            columnHandles.put("tx_input", new EthereumColumnHandle(connectorId, index++, "tx_input", VarcharType.VARCHAR));
        } else {
            throw new IllegalArgumentException("Unknown Table Name " + tableName);
        }

        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        List<SchemaTableName> tableNames = prefix.getSchemaName() == null ? listTables(session, null) : ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));

        for (SchemaTableName tableName : tableNames) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle
    ) {
        convertTableHandle(tableHandle);
        return convertColumnHandle(columnHandle).getColumnMetadata();
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns
    ) {
        ImmutableList.Builder<EthereumBlockRange> builder = ImmutableList.builder();

        Optional<Map<ColumnHandle, Domain>> domains = constraint.getSummary().getDomains();
        if (domains.isPresent()) {
            Map<ColumnHandle, Domain> columnHandleDomainMap = domains.get();
            for (Map.Entry<ColumnHandle, Domain> entry : columnHandleDomainMap.entrySet()) {
                if (entry.getKey() instanceof EthereumColumnHandle
                        && (((EthereumColumnHandle) entry.getKey()).getName().equals("block_number")
                        || ((EthereumColumnHandle) entry.getKey()).getName().equals("tx_blockNumber"))) {
                    entry.getValue().getValues().getRanges().getOrderedRanges().forEach(r -> {
                        Marker low = r.getLow();
                        Marker high = r.getHigh();
                        builder.add(EthereumBlockRange.fromMarkers(low, high));
                    });
                } else if (entry.getKey() instanceof EthereumColumnHandle
                        && (((EthereumColumnHandle) entry.getKey()).getName().equals("block_hash")
                        || ((EthereumColumnHandle) entry.getKey()).getName().equals("tx_blockHash"))) {
                    entry.getValue().getValues().getRanges().getOrderedRanges().stream()
                            .filter(Range::isSingleValue).forEach(r -> {
                                String blockHash = ((Slice) r.getSingleValue()).toStringUtf8();
                                try {
                                    long blockNumber = web3j.ethGetBlockByHash(blockHash, true).send().getBlock().getNumber().longValue();
                                    builder.add(new EthereumBlockRange(blockNumber, blockNumber));
                                }
                                catch (IOException e) {
                                    throw new IllegalStateException("Unable to getting block by hash " + blockHash);
                                }
                            });
                    log.info(entry.getValue().getValues().toString(null));
                } else if (entry.getKey() instanceof EthereumColumnHandle
                        && (((EthereumColumnHandle) entry.getKey()).getName().equals("block_timestamp"))) {
                    entry.getValue().getValues().getRanges().getOrderedRanges().forEach(r -> {
                        Marker low = r.getLow();
                        Marker high = r.getHigh();
                        try {
                            long startBlock = low.isLowerUnbounded() ? 1L : findBlockByTimestamp((Long) low.getValue(), -1L);
                            long endBlock = high.isUpperUnbounded() ? -1L : findBlockByTimestamp((Long) high.getValue(), 1L);
                            builder.add(new EthereumBlockRange(startBlock, endBlock));
                        }
                        catch (IOException e) {
                            throw new IllegalStateException("Unable to find block by timestamp");
                        }
                    });
                    log.info(entry.getValue().getValues().toString(null));
                }
            }
        }

        EthereumTableHandle handle = convertTableHandle(table);
        ConnectorTableLayout layout = new ConnectorTableLayout(new EthereumTableLayoutHandle(handle, builder.build()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
        return new ConnectorTableLayout(handle);
    }

    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName) {
        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();

        if (EthereumTable.BLOCK.getName().equals(schemaTableName.getTableName())) {
            builder.add(new ColumnMetadata("block_number", BigintType.BIGINT));
            builder.add(new ColumnMetadata("block_hash", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            builder.add(new ColumnMetadata("block_parentHash", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            builder.add(new ColumnMetadata("block_nonce", VarcharType.createVarcharType(H8_BYTE_HASH_STRING_LENGTH)));
            builder.add(new ColumnMetadata("block_sha3Uncles", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            builder.add(new ColumnMetadata("block_logsBloom", VarcharType.createVarcharType(H256_BYTE_HASH_STRING_LENGTH)));
            builder.add(new ColumnMetadata("block_transactionsRoot", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            builder.add(new ColumnMetadata("block_stateRoot", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            builder.add(new ColumnMetadata("block_miner", VarcharType.createVarcharType(H20_BYTE_HASH_STRING_LENGTH)));
            builder.add(new ColumnMetadata("block_difficulty", BigintType.BIGINT));
            builder.add(new ColumnMetadata("block_totalDifficulty", BigintType.BIGINT));
            builder.add(new ColumnMetadata("block_size", IntegerType.INTEGER));
            builder.add(new ColumnMetadata("block_extraData", VarcharType.VARCHAR));
            builder.add(new ColumnMetadata("block_gasLimit", DoubleType.DOUBLE));
            builder.add(new ColumnMetadata("block_gasUsed", DoubleType.DOUBLE));
            builder.add(new ColumnMetadata("block_timestamp", BigintType.BIGINT));
            builder.add(new ColumnMetadata("block_transactions", new ArrayType(VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH))));
            builder.add(new ColumnMetadata("block_uncles", new ArrayType(VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH))));
        } else if (EthereumTable.TRANSACTION.getName().equals(schemaTableName.getTableName())) {
            builder.add(new ColumnMetadata("tx_hash", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            builder.add(new ColumnMetadata("tx_nonce", BigintType.BIGINT));
            builder.add(new ColumnMetadata("tx_blockHash", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            builder.add(new ColumnMetadata("tx_blockNumber", BigintType.BIGINT));
            builder.add(new ColumnMetadata("tx_transactionIndex", IntegerType.INTEGER));
            builder.add(new ColumnMetadata("tx_from", VarcharType.createVarcharType(H20_BYTE_HASH_STRING_LENGTH)));
            builder.add(new ColumnMetadata("tx_to", VarcharType.createVarcharType(H20_BYTE_HASH_STRING_LENGTH)));
            builder.add(new ColumnMetadata("tx_value", DoubleType.DOUBLE));
            builder.add(new ColumnMetadata("tx_gas", DoubleType.DOUBLE));
            builder.add(new ColumnMetadata("tx_gasPrice", DoubleType.DOUBLE));
            builder.add(new ColumnMetadata("tx_input", VarcharType.VARCHAR));
        } else {
            throw new IllegalArgumentException("Unknown Table Name " + schemaTableName.getTableName());
        }

        return new ConnectorTableMetadata(schemaTableName, builder.build());
    }

    private long findBlockByTimestamp(long timestamp, long offset) throws IOException {
        long startBlock = 1L;
        long currentBlock = web3j.ethBlockNumber().send().getBlockNumber().longValue();

        if (currentBlock <= 1) {
            return currentBlock;
        }

        long low = startBlock;
        long high = currentBlock;
        long middle = low + (high - low) / 2;

        while(low <= high) {
            middle = low + (high - low) / 2;
            long ts = web3j.ethGetBlockByNumber(DefaultBlockParameter.valueOf(BigInteger.valueOf(middle)), false).send().getBlock().getTimestamp().longValue();

            if (ts < timestamp) {
                low = middle + 1;
            } else if (ts > timestamp) {
                high = middle - 1;
            } else {
                return middle;
            }
        }
        return middle + offset;
    }
}
