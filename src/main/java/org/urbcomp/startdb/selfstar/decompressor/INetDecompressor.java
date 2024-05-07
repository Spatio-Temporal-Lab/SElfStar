package org.urbcomp.startdb.selfstar.decompressor;

public interface INetDecompressor {
    default double decompress(byte[] input) {
        return 0;
    }

    default void refresh() {
    }

}
