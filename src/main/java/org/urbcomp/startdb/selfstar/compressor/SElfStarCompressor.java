package org.urbcomp.startdb.selfstar.compressor;

import org.urbcomp.startdb.selfstar.compressor.xor.IXORCompressor;
import org.urbcomp.startdb.selfstar.utils.Elf64Utils;
import org.urbcomp.startdb.selfstar.utils.Huffman.Code;
import org.urbcomp.startdb.selfstar.utils.Huffman.HuffmanEncode;
import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class SElfStarCompressor implements ICompressor, INetCompressor {
    private final IXORCompressor xorCompressor;

    private OutputBitStream os;

    private int compressedSizeInBits = 0;

    private int lastBetaStar = Integer.MAX_VALUE;

    private int numberOfValues = 0;

    private double storeCompressionRatio = 0;

    private int byteCount = 0;

    private boolean isFirstBlock = true; // mark if it is the first block

    private Code[] huffmanCode;

    private final int[] frequency = new int[17];

    public SElfStarCompressor(IXORCompressor xorCompressor) {
        this.xorCompressor = xorCompressor;
        os = xorCompressor.getOutputStream();
    }

    @Override
    public void addValue(double v) {
        if (!isFirstBlock) {
            addValueHuffman(v);
        } else {
            addValueFirst(v);
        }
    }

    private void addValueFirst(double v) {
        long vLong = Double.doubleToRawLongBits(v);
        long vPrimeLong;
        numberOfValues++;

        if (v == 0.0 || Double.isInfinite(v)) {
            compressedSizeInBits += os.writeInt(2, 2); // case 10
            vPrimeLong = vLong;
            frequency[16]++;
        } else if (Double.isNaN(v)) {
            compressedSizeInBits += os.writeInt(2, 2); // case 10
            vPrimeLong = 0xfff8000000000000L & vLong;
            frequency[16]++;
        } else {
            // C1: v is a normal or subnormal
            int[] alphaAndBetaStar = Elf64Utils.getAlphaAndBetaStar(v, lastBetaStar);
            int e = ((int) (vLong >> 52)) & 0x7ff;
            int gAlpha = Elf64Utils.getFAlpha(alphaAndBetaStar[0]) + e - 1023;
            int eraseBits = 52 - gAlpha;
            long mask = 0xffffffffffffffffL << eraseBits;
            long delta = (~mask) & vLong;
            if (delta != 0 && eraseBits > 4) {  // C2
                if (alphaAndBetaStar[1] == lastBetaStar) {
                    compressedSizeInBits += os.writeBit(false);    // case 0
                } else {
                    compressedSizeInBits += os.writeInt(alphaAndBetaStar[1] | 0x30, 6);  // case 11, 2 + 4 = 6
                    lastBetaStar = alphaAndBetaStar[1];
                }
                vPrimeLong = mask & vLong;
                frequency[alphaAndBetaStar[1]]++;
            } else {
                compressedSizeInBits += os.writeInt(2, 2); // case 10
                vPrimeLong = vLong;
                frequency[16]++;
            }
        }
        compressedSizeInBits += xorCompressor.addValue(vPrimeLong);
    }

    private void addValueHuffman(double v) {
        long vLong = Double.doubleToRawLongBits(v);
        long vPrimeLong;
        numberOfValues++;

        if (v == 0.0 || Double.isInfinite(v)) {
            compressedSizeInBits += os.writeLong(huffmanCode[16].code, huffmanCode[16].length); // not erase
            vPrimeLong = vLong;
            frequency[16]++;
        } else if (Double.isNaN(v)) {
            compressedSizeInBits += os.writeLong(huffmanCode[16].code, huffmanCode[16].length); // not erase
            vPrimeLong = 0x7ff8000000000000L;
            frequency[16]++;
        } else {
            // C1: v is a normal or subnormal
            int[] alphaAndBetaStar = Elf64Utils.getAlphaAndBetaStar(v, lastBetaStar);
            int e = ((int) (vLong >> 52)) & 0x7ff;
            int gAlpha = Elf64Utils.getFAlpha(alphaAndBetaStar[0]) + e - 1023;
            int eraseBits = 52 - gAlpha;
            long mask = 0xffffffffffffffffL << eraseBits;
            long delta = (~mask) & vLong;
            if (delta != 0 && eraseBits > 4) {  // C2
                compressedSizeInBits += os.writeLong(huffmanCode[alphaAndBetaStar[1]].code, huffmanCode[alphaAndBetaStar[1]].length);  // case 11, 2 + 4 = 6
                lastBetaStar = alphaAndBetaStar[1];
                vPrimeLong = mask & vLong;
                frequency[alphaAndBetaStar[1]]++;
            } else {
                compressedSizeInBits += os.writeLong(huffmanCode[16].code, huffmanCode[16].length); // not erase
                vPrimeLong = vLong;
                frequency[16]++;
            }
        }
        compressedSizeInBits += xorCompressor.addValue(vPrimeLong);
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
    public byte[] getBytes() {
        int byteCount = (int) Math.ceil(compressedSizeInBits / 8.0);
        return Arrays.copyOf(xorCompressor.getOut(), byteCount);
    }

    private byte[] getSingleBytes() throws IOException {
//        xorCompressor.getOutputStream().align();
        os.align();
        int preByteCnt = byteCount;
        byteCount += (int) Math.ceil(compressedSizeInBits / 8.0);
        compressedSizeInBits = 0;
        return Arrays.copyOfRange(xorCompressor.getOut(), preByteCnt, byteCount);
    }


    @Override
    public void setDistribution(int[] leadDistribution, int[] trailDistribution) {
        // for streaming scenarios, we do nothing here
    }

    @Override
    public void close() {
        double thisCompressionRatio = compressedSizeInBits / (numberOfValues * 64.0);
        if (storeCompressionRatio < thisCompressionRatio) {
            xorCompressor.setDistribution(null, null);
        }
        storeCompressionRatio = thisCompressionRatio;

        // we write one more bit here, for marking an end of the stream.
        if (isFirstBlock) {
            compressedSizeInBits += os.writeInt(2, 2);  // case 10
            isFirstBlock = false;
        } else {
            compressedSizeInBits += os.writeLong(huffmanCode[16].code, huffmanCode[16].length); // not erase
        }
        huffmanCode = HuffmanEncode.getHuffmanCodes(frequency);
        Arrays.fill(frequency, 0);
        compressedSizeInBits += xorCompressor.close();
    }


    public String getKey() {
        return "SelfStar" + xorCompressor.getKey();
    }

    @Override
    public byte[] compress(double v) throws IOException {
        compressedSizeInBits += os.writeInt(0, 8);   // prepared for byteCnt in transmit test
        addValue(v);
        return getSingleBytes();
    }


    @Override
    public byte[] compressAndClose(double v) throws IOException {
        compressedSizeInBits += os.writeInt(0, 8);   // prepared for byteCnt in transmit test
        addValue(v);
        close();
        return getSingleBytes();
    }


    @Override
    public void refresh() {
        compressedSizeInBits = 0;
        lastBetaStar = Integer.MAX_VALUE;
        numberOfValues = 0;
        byteCount = 0;

        xorCompressor.refresh();        // note this refresh should be at the last
        os = xorCompressor.getOutputStream();
    }
}
