package org.urbcomp.startdb.selfstar.compressor32;

import org.urbcomp.startdb.selfstar.compressor32.xor.IXORCompressor32;
import org.urbcomp.startdb.selfstar.utils.Elf32Utils;
import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

public class ElfCompressor32 implements ICompressor32 {
    private final IXORCompressor32 xorCompressor;
    private int compressedSizeInBits = 0;
    private OutputBitStream os;


    private int numberOfValues = 0;

    public ElfCompressor32(IXORCompressor32 xorCompressor) {
        this.xorCompressor = xorCompressor;
        os = xorCompressor.getOutputStream();
    }

    @Override
    public void addValue(float v) {
        int vInt = Float.floatToRawIntBits(v);
        int vPrimeInt;
        numberOfValues++;

        if (v == 0.0 || Float.isInfinite(v)) {
            compressedSizeInBits += os.writeBit(false);
            vPrimeInt = vInt;
        } else if (Float.isNaN(v)) {
            compressedSizeInBits += os.writeBit(false);
            vPrimeInt = 0x7fc00000;
        } else {
            int[] alphaAndBetaStar = Elf32Utils.getAlphaAndBetaStar(v);
            int e = (vInt >> 23) & 0xff;
            int gAlpha = Elf32Utils.getFAlpha(alphaAndBetaStar[0]) + e - 127;
            int eraseBits = 23 - gAlpha;
            int mask = 0xffffffff << eraseBits;
            int delta = (~mask) & vInt;
            if (alphaAndBetaStar[1] < 8 && delta != 0 && eraseBits > 3) {
                compressedSizeInBits += os.writeInt(alphaAndBetaStar[1] | 0x8, 4);
                vPrimeInt = mask & vInt;
            } else {
                compressedSizeInBits += os.writeBit(false);
                vPrimeInt = vInt;
            }
        }
        compressedSizeInBits += xorCompressor.addValue(vPrimeInt);
    }

    @Override
    public byte[] getBytes() {
        return xorCompressor.getOut();
    }

    @Override
    public void close() {
        // we write one more bit here, for marking an end of the stream.
        compressedSizeInBits += os.writeBit(false);  // case 0
        compressedSizeInBits += xorCompressor.close();
    }

    @Override
    public double getCompressionRatio() {
        return compressedSizeInBits / (numberOfValues * 32.0);
    }

    @Override
    public long getCompressedSizeInBits() {
        return compressedSizeInBits;
    }

    @Override
    public void refresh() {
        xorCompressor.refresh();
        compressedSizeInBits = 0;
        numberOfValues = 0;
        os = xorCompressor.getOutputStream();
    }
}
