package org.urbcomp.startdb.selfstar.decompressor;

import java.io.IOException;

public interface INetDecompressor {
    default double decompress(byte[] input){return 0;}

    default void refresh(){}

    default double decompressLast(byte[] input){ return 0;}

    default double[] ALPNetDecompress(byte[] receivedData) throws IOException {
        return null;
    }
}
