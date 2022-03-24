package im.xiaoyao.presto.tezos.connector;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.inject.Injector;
import im.xiaoyao.presto.tezos.handle.TezosHandleResolver;
import io.airlift.bootstrap.Bootstrap;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class TezosConnectorFactory implements ConnectorFactory {
    @Override
    public String getName() {
        return "tezos";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver() {
        return new TezosHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config, ConnectorContext context) {
        requireNonNull(config, "config is null");

        try {
            Bootstrap app = new Bootstrap(
                    new TezosConnectorModule()
            );

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(TezosConnector.class);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
