package org.urbcomp.startdb.selfstar.mixcompressor.xor;


import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

public interface IXORCompressor {

    void setOutputStream();

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
