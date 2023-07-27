package org.urbcomp.startdb.selfStar.decompressor.xor;

import org.urbcomp.startdb.selfStar.utils.InputBitStream;

public interface IXORDecompressor {
    Double readValue();

    InputBitStream getInputStream();

    void setBytes(byte[] bs);

    void refresh();
}
