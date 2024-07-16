package org.urbcomp.startdb.selfstar.compressor;

import org.urbcomp.startdb.selfstar.compressor.xor.IXORCompressor;
import org.urbcomp.startdb.selfstar.utils.Elf64Utils;
import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

import java.io.IOException;
import java.util.Arrays;

public class ElfCompressor implements ICompressor, INetCompressor {
    private final IXORCompressor xorCompressor;
    private int compressedSizeInBits = 0;
    private int byteCount = 0;
    private OutputBitStream os;

    private int numberOfValues = 0;

    public ElfCompressor(IXORCompressor xorCompressor) {
        this.xorCompressor = xorCompressor;
        os = xorCompressor.getOutputStream();
    }

    @Override
    public void addValue(double v) {
        long vLong = Double.doubleToRawLongBits(v);
        long vPrimeLong;
        numberOfValues++;

        if (v == 0.0 || Double.isInfinite(v)) {
            compressedSizeInBits += os.writeBit(false);
            vPrimeLong = vLong;
        } else if (Double.isNaN(v)) {
            compressedSizeInBits += os.writeBit(false);
            vPrimeLong = 0x7ff8000000000000L;
        } else {
            int[] alphaAndBetaStar = Elf64Utils.getAlphaAndBetaStar(v);
            int e = ((int) (vLong >> 52)) & 0x7ff;
            int gAlpha = Elf64Utils.getFAlpha(alphaAndBetaStar[0]) + e - 1023;
            int eraseBits = 52 - gAlpha;
            long mask = 0xffffffffffffffffL << eraseBits;
            long delta = (~mask) & vLong;
            if (alphaAndBetaStar[1] < 16 && delta != 0 && eraseBits > 4) {
                compressedSizeInBits += os.writeInt(alphaAndBetaStar[1] | 0x10, 5);
                vPrimeLong = mask & vLong;
            } else {
                compressedSizeInBits += os.writeBit(false);
                vPrimeLong = vLong;
            }
        }
        compressedSizeInBits += xorCompressor.addValue(vPrimeLong);
    }

    @Override
    public byte[] getBytes() {
        return xorCompressor.getOut();
    }

    private byte[] getSingleBytes() throws IOException {
        xorCompressor.getOutputStream().align();
        int preByteCnt = byteCount;
        byteCount += (int) Math.ceil(compressedSizeInBits / 8.0);
        compressedSizeInBits = 0;
        return Arrays.copyOfRange(xorCompressor.getOut(), preByteCnt, byteCount);
    }

    @Override
    public void close() {
        // we write one more bit here, for marking an end of the stream.
        compressedSizeInBits += os.writeBit(false);  // case 0
        compressedSizeInBits += xorCompressor.close();
    }

    @Override
    public double getCompressionRatio() {
        return compressedSizeInBits / (numberOfValues * 64.0);
    }

    @Override
    public long getCompressedSizeInBits() {
        return compressedSizeInBits;
    }

    @Override
    public String getKey() {
        return "Elf" + xorCompressor.getKey();
    }

    @Override
    public byte[] compress(double v) throws IOException {
        compressedSizeInBits += os.writeInt(0, 8);   // prepared for byteCnt in transmit test
        addValue(v);
        return getSingleBytes();
    }

    @Override
    public void refresh() {
        xorCompressor.refresh();
        compressedSizeInBits = 0;
        numberOfValues = 0;
        byteCount = 0;
        os = xorCompressor.getOutputStream();
    }
}
