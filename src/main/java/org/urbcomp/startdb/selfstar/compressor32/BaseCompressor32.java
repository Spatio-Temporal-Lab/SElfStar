package org.urbcomp.startdb.selfstar.compressor32;

import org.urbcomp.startdb.selfstar.compressor32.xor.IXORCompressor32;

import java.util.Arrays;

public class BaseCompressor32 implements ICompressor32 {
    private final IXORCompressor32 xorCompressor;
    private int compressedSizeInBits = 0;
    private int numberOfValues = 0;

    public BaseCompressor32(IXORCompressor32 xorCompressor) {
        this.xorCompressor = xorCompressor;
    }

    @Override
    public void addValue(float v) {
        numberOfValues++;
        compressedSizeInBits += xorCompressor.addValue(Float.floatToRawIntBits(v));
    }

    @Override
    public byte[] getBytes() {
        int byteCount = (int) Math.ceil(compressedSizeInBits / 8.0);
        return Arrays.copyOf(xorCompressor.getOut(), byteCount);
    }

    @Override
    public void close() {
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

    public String getKey() {
        return xorCompressor.getKey();
    }

    @Override
    public void refresh() {
        xorCompressor.refresh();
        compressedSizeInBits = 0;
        numberOfValues = 0;
    }
}
