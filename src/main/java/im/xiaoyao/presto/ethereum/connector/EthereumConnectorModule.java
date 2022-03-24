package im.xiaoyao.presto.ethereum.connector;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import im.xiaoyao.presto.ethereum.EthereumMetadata;
import im.xiaoyao.presto.ethereum.EthereumRecordSetProvider;
import im.xiaoyao.presto.ethereum.EthereumSplitManager;
import im.xiaoyao.presto.ethereum.EthereumWeb3jProvider;

import static io.airlift.configuration.ConfigBinder.configBinder;

/**
 * Handles dependency injection
 */
public class EthereumConnectorModule implements Module {
    @Override
    public void configure(Binder binder) {
        binder.bind(EthereumConnector.class).in(Scopes.SINGLETON);
        binder.bind(EthereumMetadata.class).in(Scopes.SINGLETON);
        binder.bind(EthereumWeb3jProvider.class).in(Scopes.SINGLETON);

        binder.bind(EthereumSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(EthereumRecordSetProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(EthereumConnectorConfig.class);
    }
}
