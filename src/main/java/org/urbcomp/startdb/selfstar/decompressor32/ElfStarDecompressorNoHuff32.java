package org.urbcomp.startdb.selfstar.decompressor32;

import org.urbcomp.startdb.selfstar.decompressor32.xor.IXORDecompressor32;

// all ElfStar (batch and stream) share the same decompressor
public class ElfStarDecompressorNoHuff32 extends ElfPlusDecompressor32 {
    public ElfStarDecompressorNoHuff32(IXORDecompressor32 xorDecompressor) {
        super(xorDecompressor);
    }
}
