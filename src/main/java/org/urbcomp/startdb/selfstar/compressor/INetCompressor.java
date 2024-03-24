package org.urbcomp.startdb.selfstar.compressor;

import java.io.IOException;
import java.util.List;

public interface INetCompressor {
    default byte[] compress(double v) throws IOException {return null;}

    default void refresh(){}

    default String getKey() {
        return getClass().getSimpleName();
    }

    default void close(){}

    default byte[] compressAndClose(double v) throws IOException { return null;}

    default byte[] ALPNetCompress(List<Double> row) throws IOException {return null;}

}
