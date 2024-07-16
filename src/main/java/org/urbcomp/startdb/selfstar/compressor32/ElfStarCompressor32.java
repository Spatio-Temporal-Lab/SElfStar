package org.urbcomp.startdb.selfstar.compressor32;

import org.urbcomp.startdb.selfstar.compressor32.xor.IXORCompressor32;
import org.urbcomp.startdb.selfstar.utils.Elf32Utils;
import org.urbcomp.startdb.selfstar.utils.Huffman.Code;
import org.urbcomp.startdb.selfstar.utils.Huffman.HuffmanEncode;
import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

import java.util.Arrays;

public class ElfStarCompressor32 implements ICompressor32 {
    private final IXORCompressor32 xorCompressor;
    private final int[] betaStarList;
    private final int[] vPrimeList;
    private final int[] leadDistribution = new int[32];
    private final int[] trailDistribution = new int[32];
    private OutputBitStream os;
    private int compressedSizeInBits = 0;
    private int lastBetaStar = Integer.MAX_VALUE;
    private int numberOfValues = 0;
    private final int[] frequency = new int[9];    // 0 is for 10-i, 16 is for not erasing
    private Code[] huffmanCode;

    public ElfStarCompressor32(IXORCompressor32 xorCompressor) {
        this.xorCompressor = xorCompressor;
        this.os = xorCompressor.getOutputStream();
        this.betaStarList = new int[1001];     // one for the end sign
        this.vPrimeList = new int[1001];      // one for the end sign
    }

    public void addValue(float v) {
        int vInt = Float.floatToRawIntBits(v);

        if (v == 0.0 || Float.isInfinite(v)) {
            vPrimeList[numberOfValues] = vInt;
            betaStarList[numberOfValues] = Integer.MAX_VALUE;
            frequency[8]++;
        } else if (Float.isNaN(v)) {
            vPrimeList[numberOfValues] = 0x7fc00000;
            betaStarList[numberOfValues] = Integer.MAX_VALUE;
            frequency[8]++;
        } else {
            // C1: v is a normal or subnormal
            int[] alphaAndBetaStar = Elf32Utils.getAlphaAndBetaStar(v, lastBetaStar);
            int e = ((vInt >> 23)) & 0xff;
            int gAlpha = Elf32Utils.getFAlpha(alphaAndBetaStar[0]) + e - 127;
            int eraseBits = 23 - gAlpha;
            int mask = 0xffffffff << eraseBits;
            int delta = (~mask) & vInt;
            if (delta != 0 && eraseBits > 3) {  // C2
                lastBetaStar = alphaAndBetaStar[1];
                betaStarList[numberOfValues] = lastBetaStar;
                vPrimeList[numberOfValues] = mask & vInt;
                frequency[lastBetaStar]++;
            } else {
                betaStarList[numberOfValues] = Integer.MAX_VALUE;
                vPrimeList[numberOfValues] = vInt;
                frequency[8]++;
            }
        }
        numberOfValues++;
    }

    private void calculateDistribution() {
        int lastValue = vPrimeList[0];
        for (int i = 1; i < numberOfValues; i++) {
            int xor = lastValue ^ vPrimeList[i];
            if (xor != 0) {
                trailDistribution[Integer.numberOfTrailingZeros(xor)]++;
                leadDistribution[Integer.numberOfLeadingZeros(xor)]++;
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
                compressedSizeInBits += os.writeInt(huffmanCode[8].code, huffmanCode[8].length); // not erase
            } else {
                compressedSizeInBits += os.writeInt(huffmanCode[betaStarList[i]].code, huffmanCode[betaStarList[i]].length);  // case 11, 2 + 4 = 6
            }
            compressedSizeInBits += xorCompressor.addValue(vPrimeList[i]);
        }
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
        calculateDistribution();
        compress();
        // we write one more bit here, for marking an end of the stream.
        compressedSizeInBits += os.writeInt(huffmanCode[8].code, huffmanCode[8].length); // not erase
        compressedSizeInBits += xorCompressor.close();
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
