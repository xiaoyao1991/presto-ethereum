package nl.utwente.presto.tezos.connector;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import nl.utwente.presto.tezos.TezosMetadata;
import nl.utwente.presto.tezos.TezosRecordSetProvider;
import nl.utwente.presto.tezos.TezosSplitManager;
import nl.utwente.presto.tezos.handle.TezosTransactionHandle;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import javax.inject.Inject;

import static com.facebook.presto.spi.transaction.IsolationLevel.READ_COMMITTED;
import static com.facebook.presto.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class TezosConnector implements Connector {
    private static final Logger log = Logger.get(TezosConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final TezosMetadata metadata;
    private final TezosSplitManager splitManager;
    private final TezosRecordSetProvider recordSetProvider;

    @Inject
    public TezosConnector(
            LifeCycleManager lifeCycleManager,
            TezosMetadata metadata,
            TezosSplitManager splitManager,
            TezosRecordSetProvider recordSetProvider
    ) {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly) {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        return TezosTransactionHandle.INSTANCE;
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
