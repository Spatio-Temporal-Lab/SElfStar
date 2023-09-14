package org.urbcomp.startdb.selfstar.compressor.xor;


import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

public interface IXORCompressor {
    OutputBitStream getOutputStream();

    int addValue(long value);

    int close();

    byte[] getOut();

    default String getKey() {
        return getClass().getSimpleName();
    }

    void refresh();

    default void setDistribution(int[] leadDistribution, int[] trailDistribution) {
    }
}
