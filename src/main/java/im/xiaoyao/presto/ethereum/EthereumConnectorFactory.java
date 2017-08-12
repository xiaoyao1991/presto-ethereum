package im.xiaoyao.presto.ethereum;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class EthereumConnectorFactory implements ConnectorFactory {
    @Override
    public String getName() {
        return "ethereum";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver() {
        return new EthereumHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config, ConnectorContext context) {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(config, "config is null");

        try {
            Bootstrap app = new Bootstrap(
//                    new JsonModule(),
                    new EthereumConnectorModule(),
                    binder -> {
                        binder.bind(EthereumConnectorId.class).toInstance(new EthereumConnectorId(connectorId));
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                    }
            );

            Injector injector = app.strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(EthereumConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
