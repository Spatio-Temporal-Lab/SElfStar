package org.urbcomp.startdb.selfstar.decompressor;

import org.urbcomp.startdb.selfstar.decompressor.xor.IXORDecompressor;

// all ElfStar (batch and stream) share the same decompressor
public class ElfStarDecompressor extends org.urbcomp.startdb.selfstar.decompressor.ElfPlusDecompressor {
    public ElfStarDecompressor(IXORDecompressor xorDecompressor) {
        super(xorDecompressor);
    }
}
