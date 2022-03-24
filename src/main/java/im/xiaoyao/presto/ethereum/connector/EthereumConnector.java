package im.xiaoyao.presto.ethereum.connector;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import im.xiaoyao.presto.ethereum.EthereumMetadata;
import im.xiaoyao.presto.ethereum.EthereumRecordSetProvider;
import im.xiaoyao.presto.ethereum.EthereumSplitManager;
import im.xiaoyao.presto.ethereum.handle.EthereumTransactionHandle;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import javax.inject.Inject;

import static com.facebook.presto.spi.transaction.IsolationLevel.READ_COMMITTED;
import static com.facebook.presto.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class EthereumConnector implements Connector {
    private static final Logger log = Logger.get(EthereumConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final EthereumMetadata metadata;
    private final EthereumSplitManager splitManager;
    private final EthereumRecordSetProvider recordSetProvider;

    @Inject
    public EthereumConnector(
            LifeCycleManager lifeCycleManager,
            EthereumMetadata metadata,
            EthereumSplitManager splitManager,
            EthereumRecordSetProvider recordSetProvider
    ) {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly) {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        return EthereumTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider() {
        return recordSetProvider;
    }

    @Override
    public final void shutdown() {
        try {
            lifeCycleManager.stop();
        } catch (Exception e) {
            log.error("Error shutting down connector");
        }
    }
}
