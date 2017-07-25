package im.xiaoyao.presto.ethereum;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.methods.response.EthBlockNumber;

import javax.inject.Inject;
import java.io.IOException;

import static im.xiaoyao.presto.ethereum.EthereumHandleResolver.convertLayout;
import static java.util.Objects.requireNonNull;

public class EthereumSplitManager implements ConnectorSplitManager {
    private static final Logger log = Logger.get(EthereumSplitManager.class);

    private final String connectorId;
    private final Web3j web3j;

    @Inject
    public EthereumSplitManager(
            EthereumConnectorId connectorId,
            EthereumConnectorConfig config,
            Web3j web3j
    ) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.web3j = requireNonNull(web3j, "web3j is null");
        requireNonNull(config, "config is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout
    ) {
        EthereumTableLayoutHandle tableLayoutHandle = convertLayout(layout);
        EthereumTableHandle tableHandle = tableLayoutHandle.getTable();

        try {
            EthBlockNumber blockNumber = web3j.ethBlockNumber().send();
            log.info("current block number: " + blockNumber.getBlockNumber());
            ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

            for (EthereumBlockRange blockRange : tableLayoutHandle.getBlockRanges()) {
                log.info("start: %d\tend: %d", blockRange.getStartBlock(), blockRange.getEndBlock());
                for (long i = blockRange.getStartBlock(); i <= (blockRange.getEndBlock() == -1 ? blockNumber.getBlockNumber().longValue() : blockRange.getEndBlock()); i++) {
                    EthereumSplit split = new EthereumSplit(i, EthereumTable.valueOf(tableHandle.getTableName().toUpperCase()));
                    splits.add(split);
                }
            }

            return new FixedSplitSource(splits.build());
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Cannot get block number: ", e);
        }
    }
}
