package im.xiaoyao.presto.tezos.erc20;

import lombok.Getter;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum TezosERC20Token {
    QTUM("0x9a642d6b3368ddc662CA244bAdf32cDA716005BC"),
    BCAP("0xff3519eeeea3e76f1f699ccce5e23ee0bdda41ac"),
    Pluton("0xD8912C10681D8B21Fd3742244f44658dBA12264E"),
    NimiqNetwork("0xcfb98637bcae43C13323EAa1731cED2B716962fD"),
    SwarmCity("0xb9e7f8568e08d5659f5d29c4997173d84cdf2607"),
    Guppy("0xf7b098298f7c69fc14610bf71d5e02c60792894c"),
    TIME("0x6531f133e6deebe7f2dce5a0441aa7ef330b4e53"),
    SAN("0x7c5a0ce9267ed19b22f8cae653f198e3e8daf098"),
    Xaurum("0x4DF812F6064def1e5e029f1ca858777CC98D2D81"),
    TAAS("0xe7775a6e9bcf904eb39da2b68c5efb4f9360e08c"),
    Trustcoin("0xcb94be6f13a1182e4a4b6140cb7bf2025d28e41b"),
    Humaniq("0xcbcc0f036ed4788f63fc0fee32873d6a7487b908"),
    TokenCard("0xaaaf91d9b90df800df4f55c205fd6989c977e73a"),
    Lunyr("0xfa05A73FfE78ef8f1a739473e462c54bae6567D9"),
    Monaco("0xb63b606ac810a52cca15e44bb630fd42d8d1d83d"),
    vSlice("0x5c543e7AE0A1104f78406C340E9C64FD9fCE5170"),
    Bitquence("0x5af2be193a6abca9c8817001f45744777db30756"),
    Edgeless("0x08711d3b02c8758f2fb3ab4e80228418a7f8e39c"),
    AdToken("0xd0d6d6c5fe4a677d343cc433536bb717bae167dd"),
    district0x("0x0abdace70d3790235af448c88547603b945604ea"),
    Melon("0xBEB9eF514a379B997e0798FDcC901Ee474B6D9A1"),
    RLC("0x607F4C5BB672230e8672085532f7e901544a7375"),
    WINGS("0x667088b212ce3d06a1b553a7221E1fD19000d9aF"),
    DICE("0x2e071D2966Aa7D8dECB1005885bA1977D6038A65"),
    FirstBlood("0xaf30d2a7e90d7dc361c8c4585e9bb7d2f6f15bc7"),
    Aragon("0x960b236A07cf122663c4303350609A66A7B288C0"),
    Bancor("0x1f573d6fb3f13d689ff844b4ce37794d79a7ff1c"),
    FunFair("0x419d0d8bdd9af5e606ae2232ed285aff190e711b"),
    SNGLS("0xaec2e87e0a235266d9c5adc9deb4b2e29b54d009"),
    Storj("0xb64ef51c888972c908cfacf59b47c1afbc0ab8ac"),
    DGD("0xe0b7927c4af23765cb51314a0e0521a9645f0e2a"),
    Civic("0x41e5560054824ea6b0732e656e3ad64e20e94e45"),
    BAT("0x0d8775f648430679a709e98d2b0cb6250d2887ef"),
    MKR("0xc66ea802717bfb9833400264dd12c2bceaa34a6d"),
    Gnosis("0x6810e776880c02933d47db1b9fc05908e5386b96"),
    REP("0xe94327d07fc17907b4db788e5adf2ed424addff6"),
    StatusNetwork("0x744d70fdbe2ba4cf95131626614a1763df805b9e"),
    Golem("0xa74476443119A942dE498590Fe1f2454d7D4aC0d"),
    ICONOMI("0x888666CA69E0f178DED6D75b5726Cee99A87D698"),
    TenXPay("0xB97048628DB6B661D4C2aA833e95Dbe1A905B280"),
    OmiseGo("0xd26114cd6EE289AccF82350c8d8487fedB8A0C07"),
    EOS("0x86fa049857e0209aa7d9e616f7eb3b3b78ecfdb0");

    public static final Map<String, TezosERC20Token> lookup = Arrays.stream(TezosERC20Token.values())
            .collect(Collectors.toMap(TezosERC20Token::getTokenContractAddr, tezosERC20Token -> tezosERC20Token));

    @Getter private final String tokenContractAddr;
    TezosERC20Token(String tokenContractAddr) {
        this.tokenContractAddr = tokenContractAddr.toLowerCase();
    }
}
