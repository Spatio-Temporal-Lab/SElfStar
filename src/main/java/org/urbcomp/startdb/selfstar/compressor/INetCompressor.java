package org.urbcomp.startdb.selfstar.compressor;

import java.io.IOException;

public interface INetCompressor {
    byte[] compress(double v) throws IOException;

    void refresh();

    default String getKey() {
        return getClass().getSimpleName();
    }

    void close();

    default byte[] compressAndClose(double v) throws IOException { return null;};

}
