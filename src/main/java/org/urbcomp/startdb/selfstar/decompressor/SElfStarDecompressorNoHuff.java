package org.urbcomp.startdb.selfstar.decompressor;

import org.urbcomp.startdb.selfstar.decompressor.xor.IXORDecompressor;

public class SElfStarDecompressorNoHuff extends ElfStarDecompressorNoHuff{
    public SElfStarDecompressorNoHuff(IXORDecompressor xorDecompressor) {
        super(xorDecompressor);
    }
}
