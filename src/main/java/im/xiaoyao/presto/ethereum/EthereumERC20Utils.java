package im.xiaoyao.presto.ethereum;

import java.math.BigInteger;

public class EthereumERC20Utils {
    public static final String TRANSFER_EVENT_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

    public static double hexToDouble(String hex) {
        try {
            return new BigInteger(hex.substring(2), 16).doubleValue();
        } catch (NumberFormatException e) {
            return 0.0; //TEMP FIX in case of ERC-721
        }
    }
}
