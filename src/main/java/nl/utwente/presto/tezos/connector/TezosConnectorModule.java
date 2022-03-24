package nl.utwente.presto.tezos.connector;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import nl.utwente.presto.tezos.TezosMetadata;
import nl.utwente.presto.tezos.TezosRecordSetProvider;
import nl.utwente.presto.tezos.TezosSplitManager;
import nl.utwente.presto.tezos.TezosWeb3jProvider;

import static io.airlift.configuration.ConfigBinder.configBinder;

/**
 * Handles dependency injection
 */
public class TezosConnectorModule implements Module {
    @Override
    public void configure(Binder binder) {
        binder.bind(TezosConnector.class).in(Scopes.SINGLETON);
        binder.bind(TezosMetadata.class).in(Scopes.SINGLETON);
        binder.bind(TezosWeb3jProvider.class).in(Scopes.SINGLETON);

        binder.bind(TezosSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(TezosRecordSetProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(TezosConnectorConfig.class);
    }
}
