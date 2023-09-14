package org.urbcomp.startdb.selfstar.decompressor32.xor;

import org.urbcomp.startdb.selfstar.utils.InputBitStream;

public interface IXORDecompressor32 {
    Float readValue();

    InputBitStream getInputStream();

    void setBytes(byte[] bs);

    void refresh();
}
