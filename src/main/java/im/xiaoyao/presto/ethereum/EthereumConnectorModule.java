package im.xiaoyao.presto.ethereum;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.http.HttpService;

import javax.inject.Inject;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static java.util.Objects.requireNonNull;

/**
 * Created by xiaoyaoqian on 7/23/17.
 */
public class EthereumConnectorModule implements Module {
    @Override
    public void configure(Binder binder) {
        binder.bind(EthereumConnector.class).in(Scopes.SINGLETON);
        binder.bind(Web3j.class).toInstance(Web3j.build(new HttpService()));
//        binder.bind(EthereumConnectorId.class).toInstance(new EthereumConnectorId(connectorId));
        binder.bind(EthereumMetadata.class).in(Scopes.SINGLETON);

        binder.bind(EthereumSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(EthereumRecordSetProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(EthereumConnectorConfig.class);
        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
    }

    public static final class TypeDeserializer extends FromStringDeserializer<Type> {
        private static final long serialVersionUID = 1L;

        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager) {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context) {
            Type type = typeManager.getType(parseTypeSignature(value));
            checkArgument(type != null, "Unknown type %s", value);
            return type;
        }
    }
}
