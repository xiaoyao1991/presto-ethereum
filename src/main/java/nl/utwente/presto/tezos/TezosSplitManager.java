package im.xiaoyao.presto.tezos;

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import im.xiaoyao.presto.tezos.connector.TezosConnectorConfig;
import im.xiaoyao.presto.tezos.handle.TezosTableLayoutHandle;
import io.airlift.log.Logger;
import org.web3j.protocol.Web3j;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static im.xiaoyao.presto.tezos.handle.TezosHandleResolver.convertLayout;
import static java.util.Objects.requireNonNull;

public class TezosSplitManager implements ConnectorSplitManager {
    private static final Logger log = Logger.get(TezosSplitManager.class);

    private final Web3j web3j;

    @Inject
    public TezosSplitManager(
            TezosConnectorConfig config,
            TezosWeb3jProvider web3jProvider
    ) {
        requireNonNull(web3jProvider, "web3j is null");
        requireNonNull(config, "config is null");
        this.web3j = web3jProvider.getWeb3j();
    }

    /**
     * Convert list of block ranges to a list of splits
     * @param transaction
     * @param session
     * @param layout table layout and block ranges
     * @param splitSchedulingContext
     * @return
     */
    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext
    ) {
        TezosTableLayoutHandle tableLayoutHandle = convertLayout(layout);
        TezosTable table = TezosTable.valueOf(tableLayoutHandle.getTable().getTableName().toUpperCase());

        try {
            long lastBlockNumber = web3j.ethBlockNumber().send().getBlockNumber().longValue();
            log.info("current block number: " + lastBlockNumber);

            List<ConnectorSplit> connectorSplits;
            if (tableLayoutHandle.getBlockRanges().isEmpty()) {
                connectorSplits = LongStream.range(0, lastBlockNumber + 1)
                        .boxed()
                        .map(blockNumber -> new TezosSplit(blockNumber, table))
                        .collect(Collectors.toList());
            } else {
                connectorSplits = tableLayoutHandle.getBlockRanges()
                        .stream()
                        .flatMap(blockRange ->
                                LongStream.range(
                                        blockRange.getStartBlock(),
                                        blockRange.getEndBlock() == -1 ? lastBlockNumber : blockRange.getEndBlock() + 1
                                ).boxed()
                        )
                        .map(blockNumber -> new TezosSplit(blockNumber, table))
                        .collect(Collectors.toList());
            }

            log.info("Built %d splits", connectorSplits.size());
            return new FixedSplitSource(connectorSplits);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Cannot get block number: ", e);
        }
    }
}
