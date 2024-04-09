package org.urbcomp.startdb.selfstar.decompressor;

import java.io.IOException;
import java.util.List;

public interface INetDecompressor {
    default double decompress(byte[] input){return 0;}

    default List<Double> decompressMiniBatch(byte[] input,int batchSize){return null;}

    default void refresh(){}

    default double decompressLast(byte[] input){ return 0;}

    default double[] ALPNetDecompress(byte[] receivedData) throws IOException {
        return null;
    }
}
