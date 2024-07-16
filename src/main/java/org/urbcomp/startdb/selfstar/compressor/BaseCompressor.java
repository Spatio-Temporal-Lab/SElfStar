package org.urbcomp.startdb.selfstar.compressor;

import org.urbcomp.startdb.selfstar.compressor.xor.IXORCompressor;

import java.io.IOException;
import java.util.Arrays;

public class BaseCompressor implements ICompressor, INetCompressor {
    private final IXORCompressor xorCompressor;
    private int compressedSizeInBits = 0;
    private int numberOfValues = 0;
    private int byteCount = 0;

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

    private byte[] getSingleBytes() throws IOException {
        xorCompressor.getOutputStream().align();
        int preByteCnt = byteCount;
        byteCount += (int) Math.ceil(compressedSizeInBits / 8.0);
        compressedSizeInBits=0;
        return Arrays.copyOfRange(xorCompressor.getOut(),preByteCnt,byteCount);
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

    public byte[] compress(double v) throws IOException {
        compressedSizeInBits += xorCompressor.getOutputStream().writeInt(0,8);   // prepared for byteCnt in transmit test
        addValue(v);
        return getSingleBytes();
    }

    @Override
    public void refresh() {
        xorCompressor.refresh();
        compressedSizeInBits = 0;
        numberOfValues = 0;
        byteCount=0;
    }
}
