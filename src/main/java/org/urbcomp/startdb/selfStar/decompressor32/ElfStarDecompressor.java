package org.urbcomp.startdb.selfStar.decompressor32;

import org.urbcomp.startdb.selfStar.decompressor.ElfPlusDecompressor;
import org.urbcomp.startdb.selfStar.decompressor.xor.IXORDecompressor;

// all ElfStar (batch and stream) share the same decompressor
public class ElfStarDecompressor extends ElfPlusDecompressor {
    public ElfStarDecompressor(IXORDecompressor xorDecompressor) {
        super(xorDecompressor);
    }
}
