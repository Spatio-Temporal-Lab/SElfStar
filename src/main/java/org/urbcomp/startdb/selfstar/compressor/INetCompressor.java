package org.urbcomp.startdb.selfstar.compressor;

public interface INetCompressor {
    byte[] compress(double v);

    default String getKey() {
        return getClass().getSimpleName();
    }
}
