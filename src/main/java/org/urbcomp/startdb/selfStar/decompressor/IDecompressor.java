package org.urbcomp.startdb.selfStar.decompressor;

import java.util.List;

public interface IDecompressor {
    Double nextValue();

    List<Double> decompress();

    void setBytes(byte[] bs);

    void refresh();
}
