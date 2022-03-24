package im.xiaoyao.presto.ethereum.handle;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static java.util.Objects.requireNonNull;

@EqualsAndHashCode
@ToString
public final class EthereumColumnHandle implements ColumnHandle {
    private final int ordinalPosition;

    private final String name;
    private final Type type;

    @JsonCreator
    public EthereumColumnHandle(
            @JsonProperty("ordinalPosition") int ordinalPosition,
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type
    ) {
        this.ordinalPosition = ordinalPosition;
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
    }

    @JsonProperty
    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public Type getType() {
        return type;
    }

    public ColumnMetadata getColumnMetadata() {
        return new ColumnMetadata(name, type);
    }
}
