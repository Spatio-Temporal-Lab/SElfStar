package org.urbcomp.startdb.selfstar.decompressor;

import org.urbcomp.startdb.selfstar.decompressor.xor.IXORDecompressor;

// all ElfStar (batch and stream) share the same decompressor
public class ElfStarDecompressorNoHuff extends org.urbcomp.startdb.selfstar.decompressor.ElfPlusDecompressor {
    public ElfStarDecompressorNoHuff(IXORDecompressor xorDecompressor) {
        super(xorDecompressor);
    }
}
