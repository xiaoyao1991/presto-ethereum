package im.xiaoyao.presto.tezos.udfs;

public enum TezosUnit {
    WEI(1),
    KWEI(1E3), ADA(1E3),
    MWEI(1E6), BABBAGE(1E6),
    GWEI(1E9), SHANNON(1E9),
    SZABO(1E12),
    FINNEY(1E15),
    ETHER(1E18),
    KETHER(1E21), GRAND(1E21), EINSTEIN(1E21),
    METHER(1E24),
    GETHER(1E27),
    TETHER(1E30);

    private final double xwei;
    TezosUnit(double v) {
        this.xwei = v;
    }

    public double fromWei(double num) {
        return num / xwei;
    }

    public double toWei(double num) {
        return num * xwei;
    }
}
