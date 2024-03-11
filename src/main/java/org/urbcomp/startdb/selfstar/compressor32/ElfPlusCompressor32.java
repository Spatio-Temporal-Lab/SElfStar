package org.urbcomp.startdb.selfstar.compressor32;

import org.urbcomp.startdb.selfstar.compressor32.xor.IXORCompressor32;
import org.urbcomp.startdb.selfstar.utils.Elf32Utils;
import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

import java.util.Arrays;

public class ElfPlusCompressor32 implements ICompressor32 {
    private final IXORCompressor32 xorCompressor;

    private OutputBitStream os;

    private int compressedSizeInBits = 0;

    private int lastBetaStar = Integer.MAX_VALUE;

    private int numberOfValues = 0;

    public ElfPlusCompressor32(IXORCompressor32 xorCompressor) {
        this.xorCompressor = xorCompressor;
        os = xorCompressor.getOutputStream();
    }

    public void addValue(float v) {
        int vInt = Float.floatToRawIntBits(v);
        int vPrimeInt;
        numberOfValues++;

        if (v == 0.0 || Float.isInfinite(v)) {
            compressedSizeInBits += os.writeInt(2, 2); // case 10
            vPrimeInt = vInt;
        } else if (Float.isNaN(v)) {
            compressedSizeInBits += os.writeInt(2, 2); // case 10
            vPrimeInt = 0x7fc00000;
        } else {
            //1
            // C1: v is a normal or subnormal
            int[] alphaAndBetaStar = Elf32Utils.getAlphaAndBetaStar(v, lastBetaStar);// 问题点1
            //5
            int e = (vInt >> 23) & 0xff;
            //4 up
            int gAlpha = Elf32Utils.getFAlpha(alphaAndBetaStar[0]) + e - 127;

            //3
            int eraseBits = 23 - gAlpha;
            int mask = 0xffffffff << eraseBits;
            int delta = (~mask) & vInt;
            //2

            if (delta != 0 && eraseBits > 3) {  // C2
                if (alphaAndBetaStar[1] == lastBetaStar) {
                    compressedSizeInBits += os.writeBit(false);    // case 0
                } else {
                    compressedSizeInBits += os.writeInt(alphaAndBetaStar[1] | 0x18, 5);  // case 11, 2 + 3 = 5
                    lastBetaStar = alphaAndBetaStar[1];
                }
                vPrimeInt = mask & vInt;
            } else {
                compressedSizeInBits += os.writeInt(2, 2); // case 10
                vPrimeInt = vInt;
            }
        }
        compressedSizeInBits += xorCompressor.addValue(vPrimeInt);
    }

    public double getCompressionRatio() {
        return compressedSizeInBits / (numberOfValues * 32.0);
    }

    @Override
    public long getCompressedSizeInBits() {
        return compressedSizeInBits;
    }

    public byte[] getBytes() {
        int byteCount = (int) Math.ceil(compressedSizeInBits / 8.0);
        return Arrays.copyOf(xorCompressor.getOut(), byteCount);
    }

    public void close() {
        // we write one more bit here, for marking an end of the stream.
        compressedSizeInBits += os.writeInt(2, 2);  // case 10
        compressedSizeInBits += xorCompressor.close();
    }

    public String getKey() {
        return xorCompressor.getKey();
    }

    @Override
    public void refresh() {
        xorCompressor.refresh();
        compressedSizeInBits = 0;
        lastBetaStar = Integer.MAX_VALUE;

        numberOfValues = 0;
        os = xorCompressor.getOutputStream();
    }
}
