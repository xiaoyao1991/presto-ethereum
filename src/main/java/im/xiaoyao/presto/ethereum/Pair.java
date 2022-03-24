package im.xiaoyao.presto.ethereum;

public final class Pair<T, U> {
    public final T first;
    public final U second;

    public Pair(T first, U second) {
        this.second = second;
        this.first = first;
    }
}

