package org.urbcomp.startdb.selfStar.compressor32.xor;


import org.urbcomp.startdb.selfStar.utils.OutputBitStream;

public interface IXORCompressor {
    OutputBitStream getOutputStream();

    int addValue(int value);

    int close();

    byte[] getOut();

    default String getKey() {
        return getClass().getSimpleName();
    }

    void refresh();

    default void setDistribution(int[] leadDistribution, int[] trailDistribution) {
    }
}
