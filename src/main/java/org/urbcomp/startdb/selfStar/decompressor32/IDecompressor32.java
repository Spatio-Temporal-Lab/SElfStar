package org.urbcomp.startdb.selfStar.decompressor32;

import java.util.List;

public interface IDecompressor32 {
    Float nextValue();

    List<Float> decompress();

    void setBytes(byte[] bs);

    void refresh();
}
