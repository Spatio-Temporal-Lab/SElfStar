package org.urbcomp.startdb.selfstar.compressor32;

public interface ICompressor32 {
    void addValue(float v);

    byte[] getBytes();

    void close();

    double getCompressionRatio();

    long getCompressedSizeInBits();

    default String getKey() {
        return getClass().getSimpleName();
    }

    void refresh();

    default void setDistribution(int[] distribution1, int[] distribution2) {
    }
}
