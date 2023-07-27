package org.urbcomp.startdb.selfStar.compressor;

import org.urbcomp.startdb.selfStar.compressor.xor.IXORCompressor;

import java.util.Arrays;

public class BaseCompressor implements ICompressor {
    private final IXORCompressor xorCompressor;
    private int compressedSizeInBits = 0;
    private int numberOfValues = 0;

    public BaseCompressor(IXORCompressor xorCompressor) {
        this.xorCompressor = xorCompressor;
    }

    @Override
    public void addValue(double v) {
        numberOfValues++;
        compressedSizeInBits += xorCompressor.addValue(Double.doubleToRawLongBits(v));
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
        return compressedSizeInBits / (numberOfValues * 64.0);
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
