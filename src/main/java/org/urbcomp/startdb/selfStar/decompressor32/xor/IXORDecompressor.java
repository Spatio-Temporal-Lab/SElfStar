package org.urbcomp.startdb.selfStar.decompressor32.xor;

import org.urbcomp.startdb.selfStar.utils.InputBitStream;

public interface IXORDecompressor {
    Float readValue();

    InputBitStream getInputStream();

    void setBytes(byte[] bs);

    void refresh();
}
