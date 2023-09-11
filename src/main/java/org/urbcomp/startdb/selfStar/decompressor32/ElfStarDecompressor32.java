package org.urbcomp.startdb.selfStar.decompressor32;

import org.urbcomp.startdb.selfStar.decompressor32.xor.IXORDecompressor32;

// all ElfStar (batch and stream) share the same decompressor
public class ElfStarDecompressor32 extends ElfPlusDecompressor32 {
    public ElfStarDecompressor32(IXORDecompressor32 xorDecompressor) {
        super(xorDecompressor);
    }
}
