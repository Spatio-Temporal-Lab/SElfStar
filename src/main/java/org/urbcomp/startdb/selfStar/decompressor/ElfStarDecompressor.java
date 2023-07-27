package org.urbcomp.startdb.selfStar.decompressor;

import org.urbcomp.startdb.selfStar.decompressor.xor.IXORDecompressor;

// all ElfStar (batch and stream) share the same decompressor
public class ElfStarDecompressor extends org.urbcomp.startdb.selfStar.decompressor.ElfPlusDecompressor {
    public ElfStarDecompressor(IXORDecompressor xorDecompressor) {
        super(xorDecompressor);
    }
}
