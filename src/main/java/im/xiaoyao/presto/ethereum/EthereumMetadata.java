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
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static im.xiaoyao.presto.ethereum.EthereumHandleResolver.convertColumnHandle;
import static im.xiaoyao.presto.ethereum.EthereumHandleResolver.convertTableHandle;
import static java.util.Objects.requireNonNull;

public class EthereumMetadata implements ConnectorMetadata {
    private static final int H8_BYTE_HASH_STRING_LENGTH = 2 + 8 * 2;
    private static final int H32_BYTE_HASH_STRING_LENGTH = 2 + 32 * 2;
    private static final int H256_BYTE_HASH_STRING_LENGTH = 2 + 256 * 2;
    private static final int H20_BYTE_HASH_STRING_LENGTH = 2 + 20 * 2;

    private final String connectorId;

    @Inject
    public EthereumMetadata(
            EthereumConnectorId connectorId,
            EthereumConnectorConfig config
    ) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        requireNonNull(config, "config is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
//        return Arrays.stream(EthereumSchema.values()).map(EthereumSchema::getName).collect(Collectors.toList());
        return Collections.singletonList("default");
    }

    @Override
    public EthereumTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName) {
        return new EthereumTableHandle(connectorId, "default", "blockchain");
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle) {
        return getTableMetadata(convertTableHandle(tableHandle).toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        return ImmutableList.of(new SchemaTableName("default", "blockchain"));
    }

    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();

        int index = 0;

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
        columnHandles.put("block_gasLimit", new EthereumColumnHandle(connectorId, index++, "block_gasLimit", BigintType.BIGINT));
        columnHandles.put("block_gasUsed", new EthereumColumnHandle(connectorId, index++, "block_gasUsed", BigintType.BIGINT));
        columnHandles.put("block_timestamp", new EthereumColumnHandle(connectorId, index++, "block_timestamp", BigintType.BIGINT));
        columnHandles.put("block_transactions", new EthereumColumnHandle(connectorId, index++, "block_transactions", new ArrayType(new RowType(
                ImmutableList.of(
                        VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH),
                        BigintType.BIGINT,
                        VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH),
                        BigintType.BIGINT,
                        IntegerType.INTEGER,
                        VarcharType.createVarcharType(H20_BYTE_HASH_STRING_LENGTH),
                        VarcharType.createVarcharType(H20_BYTE_HASH_STRING_LENGTH),
                        BigintType.BIGINT,
                        BigintType.BIGINT,
                        BigintType.BIGINT,
                        VarcharType.VARCHAR),
                Optional.of(ImmutableList.of(
                        "tx_hash",
                        "tx_nonce",
                        "tx_blockHash",
                        "tx_blockNumber",
                        "tx_transactionIndex",
                        "tx_from",
                        "tx_to",
                        "tx_value",
                        "tx_gas",
                        "tx_gasPrice",
                        "tx_input")
                )))));
        columnHandles.put("block_uncles", new EthereumColumnHandle(connectorId, index++, "block_uncles", new ArrayType(VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH))));

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
        EthereumTableHandle handle = convertTableHandle(table);
        ConnectorTableLayout layout = new ConnectorTableLayout(new EthereumTableLayoutHandle(handle));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
        return new ConnectorTableLayout(handle);
    }

    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName)
    {
        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();

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
        builder.add(new ColumnMetadata("block_gasLimit", BigintType.BIGINT));
        builder.add(new ColumnMetadata("block_gasUsed", BigintType.BIGINT));
        builder.add(new ColumnMetadata("block_timestamp", BigintType.BIGINT));
        builder.add(new ColumnMetadata("block_transactions", new ArrayType(new RowType(
                ImmutableList.of(
                        VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH),
                        BigintType.BIGINT,
                        VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH),
                        BigintType.BIGINT,
                        IntegerType.INTEGER,
                        VarcharType.createVarcharType(H20_BYTE_HASH_STRING_LENGTH),
                        VarcharType.createVarcharType(H20_BYTE_HASH_STRING_LENGTH),
                        BigintType.BIGINT,
                        BigintType.BIGINT,
                        BigintType.BIGINT,
                        VarcharType.VARCHAR),
                Optional.of(ImmutableList.of(
                        "tx_hash",
                        "tx_nonce",
                        "tx_blockHash",
                        "tx_blockNumber",
                        "tx_transactionIndex",
                        "tx_from",
                        "tx_to",
                        "tx_value",
                        "tx_gas",
                        "tx_gasPrice",
                        "tx_input")
                )))));
        builder.add(new ColumnMetadata("block_uncles", new ArrayType(VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH))));

        return new ConnectorTableMetadata(schemaTableName, builder.build());
    }
}
