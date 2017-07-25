package im.xiaoyao.presto.ethereum;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum EthereumTable {
    BLOCK("block"),
    TRANSACTION("transaction");

    @Getter
    private final String name;
}
