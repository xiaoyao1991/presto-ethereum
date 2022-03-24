package im.xiaoyao.presto.tezos;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum TezosTable {
    BLOCK("block"),
    TRANSACTION("transaction"),
    ERC20("erc20");

    @Getter
    private final String name;
}
