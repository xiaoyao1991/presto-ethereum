package nl.utwente.presto.tezos;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Marker;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.type.*;
import com.facebook.presto.spi.*;
import com.google.common.collect.ImmutableList;
import nl.utwente.presto.tezos.handle.TezosColumnHandle;
import nl.utwente.presto.tezos.handle.TezosTableHandle;
import nl.utwente.presto.tezos.handle.TezosTableLayoutHandle;
import io.airlift.slice.Slice;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;

import javax.inject.Inject;
import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static nl.utwente.presto.tezos.handle.TezosHandleResolver.convertTableHandle;
import static java.util.Objects.requireNonNull;

public class TezosMetadata extends BaseTezosMetadata {
    public static final int H8_BYTE_HASH_STRING_LENGTH = 2 + 8 * 2;
    public static final int H32_BYTE_HASH_STRING_LENGTH = 2 + 32 * 2;
    public static final int H256_BYTE_HASH_STRING_LENGTH = 2 + 256 * 2;
    public static final int H20_BYTE_HASH_STRING_LENGTH = 2 + 20 * 2;

    private final Web3j web3j;

    @Inject
    public TezosMetadata(
            TezosWeb3jProvider provider
    ) {
        this.web3j = requireNonNull(provider, "provider is null").getWeb3j();
    }

    /**
     * Get block ranges to search in based on query
     * @param session
     * @param table queried table
     * @param constraint query constraints
     * @param desiredColumns
     * @return
     */
    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns
    ) {
        ImmutableList.Builder<TezosBlockRange> builder = ImmutableList.builder();

        Optional<Map<ColumnHandle, Domain>> domains = constraint.getSummary().getDomains();
        if (domains.isPresent()) {
            Map<ColumnHandle, Domain> columnHandleDomainMap = domains.get();
            for (Map.Entry<ColumnHandle, Domain> entry : columnHandleDomainMap.entrySet()) {
                if (!(entry.getKey() instanceof TezosColumnHandle)) continue;

                String columnName = ((TezosColumnHandle) entry.getKey()).getName();
                List<Range> orderedRanges = entry.getValue().getValues().getRanges().getOrderedRanges();

                switch (columnName) {
                    case "block_number":
                    case "tx_blockNumber":
                    case "erc20_blockNumber":
                        // Limit query to block number range
                        orderedRanges.forEach(r -> {
                            Marker low = r.getLow();
                            Marker high = r.getHigh();
                            builder.add(TezosBlockRange.fromMarkers(low, high));
                        });
                        break;
                    case "block_hash":
                    case "tx_blockHash":
                        // Limit query to block hash range
                        orderedRanges.stream()
                                .filter(Range::isSingleValue).forEach(r -> {
                                    String blockHash = ((Slice) r.getSingleValue()).toStringUtf8();
                                    try {
                                        long blockNumber = web3j.ethGetBlockByHash(blockHash, true).send().getBlock().getNumber().longValue();
                                        builder.add(new TezosBlockRange(blockNumber, blockNumber));
                                    } catch (IOException e) {
                                        throw new IllegalStateException("Unable to getting block by hash " + blockHash);
                                    }
                                });
                        log.info(entry.getValue().getValues().toString(null));
                        break;
                    case "block_timestamp":
                        // Limit query to block timestamp range
                        orderedRanges.forEach(r -> {
                            Marker low = r.getLow();
                            Marker high = r.getHigh();
                            try {
                                long startBlock = low.isLowerUnbounded() ? 1L : findBlockByTimestamp((Long) low.getValue(), -1L);
                                long endBlock = high.isUpperUnbounded() ? -1L : findBlockByTimestamp((Long) high.getValue(), 1L);
                                builder.add(new TezosBlockRange(startBlock, endBlock));
                            } catch (IOException e) {
                                throw new IllegalStateException("Unable to find block by timestamp");
                            }
                        });
                        log.info(entry.getValue().getValues().toString(null));
                        break;
                }
            }
        }

        TezosTableHandle handle = convertTableHandle(table);
        ConnectorTableLayout layout = new ConnectorTableLayout(new TezosTableLayoutHandle(handle, builder.build()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    /**
     * Return the columns and their types
     * @param table table to get columns of
     * @return list of columns
     */
    @Override
    protected  List<Pair<String, Type>> getColumnsWithTypes(String table) {
        ImmutableList.Builder<Pair<String, Type>> builder = ImmutableList.builder();

        if (TezosTable.BLOCK.getName().equals(table)) {
            builder.add(new Pair<>("block_number", BigintType.BIGINT));
            builder.add(new Pair<>("block_hash", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("block_parentHash", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("block_nonce", VarcharType.createVarcharType(H8_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("block_sha3Uncles", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("block_logsBloom", VarcharType.createVarcharType(H256_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("block_transactionsRoot", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("block_stateRoot", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("block_miner", VarcharType.createVarcharType(H20_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("block_difficulty", BigintType.BIGINT));
            builder.add(new Pair<>("block_totalDifficulty", BigintType.BIGINT));
            builder.add(new Pair<>("block_size", IntegerType.INTEGER));
            builder.add(new Pair<>("block_extraData", VarcharType.VARCHAR));
            builder.add(new Pair<>("block_gasLimit", DoubleType.DOUBLE));
            builder.add(new Pair<>("block_gasUsed", DoubleType.DOUBLE));
            builder.add(new Pair<>("block_timestamp", BigintType.BIGINT));
            builder.add(new Pair<>("block_transactions", new ArrayType(VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH))));
            builder.add(new Pair<>("block_uncles", new ArrayType(VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH))));
        } else if (TezosTable.TRANSACTION.getName().equals(table)) {
            builder.add(new Pair<>("tx_hash", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("tx_nonce", BigintType.BIGINT));
            builder.add(new Pair<>("tx_blockHash", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("tx_blockNumber", BigintType.BIGINT));
            builder.add(new Pair<>("tx_transactionIndex", IntegerType.INTEGER));
            builder.add(new Pair<>("tx_from", VarcharType.createVarcharType(H20_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("tx_to", VarcharType.createVarcharType(H20_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("tx_value", DoubleType.DOUBLE));
            builder.add(new Pair<>("tx_gas", DoubleType.DOUBLE));
            builder.add(new Pair<>("tx_gasPrice", DoubleType.DOUBLE));
            builder.add(new Pair<>("tx_input", VarcharType.VARCHAR));
        } else if (TezosTable.ERC20.getName().equals(table)) {
            builder.add(new Pair<>("erc20_token", VarcharType.createUnboundedVarcharType()));
            builder.add(new Pair<>("erc20_from", VarcharType.createVarcharType(H20_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("erc20_to", VarcharType.createVarcharType(H20_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("erc20_value", DoubleType.DOUBLE));
            builder.add(new Pair<>("erc20_txHash", VarcharType.createVarcharType(H32_BYTE_HASH_STRING_LENGTH)));
            builder.add(new Pair<>("erc20_blockNumber", BigintType.BIGINT));
        } else {
            throw new IllegalArgumentException("Unknown Table Name " + table);
        }

        return builder.build();
    }

    /**
     * Get the ID of a block that was added at a given timestamp
     * @param timestamp timestamp of block
     * @param offset offset to middle block
     * @return ID of block
     */
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
