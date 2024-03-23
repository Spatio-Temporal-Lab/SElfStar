package com.github.Cwida.alp;

public class ALPCombination {
    public byte e;  // exponent指数
    public byte f;  // factor因子
    public long bestCnt; // 作为最佳组合出现的次数

    public ALPCombination(byte exponent, byte factor, long bestCnt) {
        this.e = exponent;
        this.f = factor;
        this.bestCnt = bestCnt;
    }

    /**
     * 判断组合c1是否优于组合c2
     *
     * @param c1 combination 1
     * @param c2 combination 2
     * @return if c1 better than c2
     */
    public static boolean compareALPCombinations(ALPCombination c1, ALPCombination c2) {
        return (c1.bestCnt > c2.bestCnt) ||
                (c1.bestCnt == c2.bestCnt && c2.e < c1.e) ||
                ((c1.bestCnt == c2.bestCnt && c2.e == c1.e) && (c2.f < c1.f));
    }
}
