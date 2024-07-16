package org.urbcomp.startdb.selfstar.compressor;

import java.io.IOException;

public interface INetCompressor {
    default byte[] compress(double v) throws IOException {
        return null;
    }

    default void refresh() {
    }

    default String getKey() {
        return getClass().getSimpleName();
    }

    default void close() {
    }

    default byte[] compressAndClose(double v) throws IOException {
        return compress(v);
    }

}
