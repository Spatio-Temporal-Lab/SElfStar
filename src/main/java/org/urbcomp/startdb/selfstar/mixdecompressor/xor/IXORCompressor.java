package org.urbcomp.startdb.selfstar.mixdecompressor.xor;


import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

public interface IXORCompressor {

    OutputBitStream getOutputStream();

    void setOutputBitStream(OutputBitStream out);

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
