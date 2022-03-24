package im.xiaoyao.presto.ethereum;

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import im.xiaoyao.presto.ethereum.connector.EthereumConnectorConfig;
import im.xiaoyao.presto.ethereum.handle.EthereumTableLayoutHandle;
import io.airlift.log.Logger;
import org.web3j.protocol.Web3j;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static im.xiaoyao.presto.ethereum.handle.EthereumHandleResolver.convertLayout;
import static java.util.Objects.requireNonNull;

public class EthereumSplitManager implements ConnectorSplitManager {
    private static final Logger log = Logger.get(EthereumSplitManager.class);

    private final Web3j web3j;

    @Inject
    public EthereumSplitManager(
            EthereumConnectorConfig config,
            EthereumWeb3jProvider web3jProvider
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
        EthereumTableLayoutHandle tableLayoutHandle = convertLayout(layout);
        EthereumTable table = EthereumTable.valueOf(tableLayoutHandle.getTable().getTableName().toUpperCase());

        try {
            long lastBlockNumber = web3j.ethBlockNumber().send().getBlockNumber().longValue();
            log.info("current block number: " + lastBlockNumber);

            List<ConnectorSplit> connectorSplits;
            if (tableLayoutHandle.getBlockRanges().isEmpty()) {
                connectorSplits = LongStream.range(0, lastBlockNumber + 1)
                        .boxed()
                        .map(blockNumber -> new EthereumSplit(blockNumber, table))
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
                        .map(blockNumber -> new EthereumSplit(blockNumber, table))
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
