package org.urbcomp.startdb.selfstar.compressor;

import org.urbcomp.startdb.selfstar.compressor.xor.IXORCompressor;
import org.urbcomp.startdb.selfstar.utils.Elf64Utils;
import org.urbcomp.startdb.selfstar.utils.Huffman.HuffmanEncode;
import org.urbcomp.startdb.selfstar.utils.Huffman.Code;
import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

import java.util.Arrays;

public class ElfStarCompressor implements ICompressor {
    private final IXORCompressor xorCompressor;
    private final int[] betaStarList;
    private final long[] vPrimeList;
    private final int[] leadDistribution = new int[64];
    private final int[] trailDistribution = new int[64];
    private OutputBitStream os;
    private int compressedSizeInBits = 0;
    private int lastBetaStar = Integer.MAX_VALUE;
    private int numberOfValues = 0;
    private final int[] frequency = new int[17];    // 0 is for 10-i, 16 is for not erasing
    private Code[] huffmanCode;

    public ElfStarCompressor(IXORCompressor xorCompressor, int window) {
        this.xorCompressor = xorCompressor;
        this.os = xorCompressor.getOutputStream();
        this.betaStarList = new int[window + 1];     // one for the end sign
        this.vPrimeList = new long[window + 1];      // one for the end sign
    }

    public ElfStarCompressor(IXORCompressor xorCompressor) {
        this.xorCompressor = xorCompressor;
        this.os = xorCompressor.getOutputStream();
        this.betaStarList = new int[1001];     // one for the end sign
        this.vPrimeList = new long[1001];      // one for the end sign
    }

    public void addValue(double v) {
        long vLong = Double.doubleToRawLongBits(v);

        if (v == 0.0 || Double.isInfinite(v)) {
            vPrimeList[numberOfValues] = vLong;
            betaStarList[numberOfValues] = Integer.MAX_VALUE;
            frequency[16]++;
        } else if (Double.isNaN(v)) {
            vPrimeList[numberOfValues] = 0x7ff8000000000000L;
            betaStarList[numberOfValues] = Integer.MAX_VALUE;
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
                lastBetaStar = alphaAndBetaStar[1];
                betaStarList[numberOfValues] = lastBetaStar;
                vPrimeList[numberOfValues] = mask & vLong;
                frequency[lastBetaStar]++;
            } else {
                betaStarList[numberOfValues] = Integer.MAX_VALUE;
                vPrimeList[numberOfValues] = vLong;
                frequency[16]++;
            }
        }
        numberOfValues++;
    }

    private void calculateDistribution() {
        long lastValue = vPrimeList[0];
        for (int i = 1; i < numberOfValues; i++) {
            long xor = lastValue ^ vPrimeList[i];
            if (xor != 0) {
                trailDistribution[Long.numberOfTrailingZeros(xor)]++;
                leadDistribution[Long.numberOfLeadingZeros(xor)]++;
                lastValue = vPrimeList[i];
            }
        }
    }

    private void compress() {
        huffmanCode = HuffmanEncode.getHuffmanCodes(frequency);
        compressedSizeInBits += HuffmanEncode.writeHuffmanCodes(os, huffmanCode);
        xorCompressor.setDistribution(leadDistribution, trailDistribution);
        for (int i = 0; i < numberOfValues; i++) {
            if (betaStarList[i] == Integer.MAX_VALUE) {
                compressedSizeInBits += os.writeLong(huffmanCode[16].code, huffmanCode[16].length); // not erase
            } else {
                compressedSizeInBits += os.writeLong(huffmanCode[betaStarList[i]].code, huffmanCode[betaStarList[i]].length);  // case 11, 2 + 4 = 6
            }
            compressedSizeInBits += xorCompressor.addValue(vPrimeList[i]);
        }
    }

    public double getCompressionRatio() {
        return compressedSizeInBits / (numberOfValues * 64.0);
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
        calculateDistribution();
        compress();
        // we write one more bit here, for marking an end of the stream.
        compressedSizeInBits += os.writeLong(huffmanCode[16].code, huffmanCode[16].length); // not erase
        compressedSizeInBits += xorCompressor.close();
    }

    public String getKey() {
        return "ElfStar" + xorCompressor.getKey();
    }

    public void refresh() {
        xorCompressor.refresh();
        compressedSizeInBits = 0;
        lastBetaStar = Integer.MAX_VALUE;
        numberOfValues = 0;
        os = xorCompressor.getOutputStream();
        Arrays.fill(frequency, 0);
        Arrays.fill(leadDistribution, 0);
        Arrays.fill(trailDistribution, 0);
    }
}
