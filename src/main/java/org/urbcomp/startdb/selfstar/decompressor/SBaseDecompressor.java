package org.urbcomp.startdb.selfstar.decompressor;

import org.urbcomp.startdb.selfstar.decompressor.xor.IXORDecompressor;

public class SBaseDecompressor extends BaseDecompressor{
    public SBaseDecompressor(IXORDecompressor xorDecompressor) {
        super(xorDecompressor);
    }
}
