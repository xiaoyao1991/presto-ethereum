package im.xiaoyao.presto.ethereum;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum EthereumTable {
    BLOCK("block"),
    TRANSACTION("transaction"),
    ERC20("erc20");

    @Getter
    private final String name;
}
