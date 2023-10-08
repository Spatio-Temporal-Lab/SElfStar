package org.urbcomp.startdb.selfstar.compressor;

import javafx.util.Pair;
import org.urbcomp.startdb.selfstar.compressor.xor.IXORCompressor;
import org.urbcomp.startdb.selfstar.utils.Elf64Utils;
import org.urbcomp.startdb.selfstar.utils.Huffman.HuffmanEncode;
import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

import java.util.Arrays;
import java.util.HashMap;

public class ElfStar2DeltaCompressor implements ICompressor {
    private static final int STATES_NUM = 18;
    private static final int[] states = new int[]{
            0, 1, 2, 3, 4, 5, 6, 7,
            8, 9, 10, 11, 12, 13, 14, 15,
            16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26, 27, 28, 29, 30, 31,
            32, 33, 34};
    private final IXORCompressor xorCompressor;
    private final int[] betaStarList;
    private final long[] vPrimeList;
    private final int[] leadDistribution = new int[64];
    private final int[] trailDistribution = new int[64];
    private final long deltaOfBeta = 0;
    private final int[] frequency = new int[STATES_NUM];
    private OutputBitStream os;
    private int compressedSizeInBits = 0;
    private int lastBetaStar = 0;
    private int numberOfValues = 0;
    private HashMap<Integer, Pair<Long, Integer>> huffmanCode = new HashMap<>();

    public ElfStar2DeltaCompressor(IXORCompressor xorCompressor, int window) {
        this.xorCompressor = xorCompressor;
        this.os = xorCompressor.getOutputStream();
        this.betaStarList = new int[window + 1];     // one for the end sign
        this.vPrimeList = new long[window + 1];      // one for the end sign
    }

    public ElfStar2DeltaCompressor(IXORCompressor xorCompressor) {
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
            frequency[17]++;
        } else if (Double.isNaN(v)) {
            vPrimeList[numberOfValues] = 0xfff8000000000000L & vLong;
            betaStarList[numberOfValues] = Integer.MAX_VALUE;
            frequency[17]++;
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
            } else {
                betaStarList[numberOfValues] = Integer.MAX_VALUE;
                vPrimeList[numberOfValues] = vLong;
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
        HuffmanEncode huffmanEncode = new HuffmanEncode(states, frequency);
        huffmanCode = huffmanEncode.getHuffmanCodes();
        compressedSizeInBits += huffmanEncode.writeHuffmanCodes(os);
        xorCompressor.setDistribution(leadDistribution, trailDistribution);
        lastBetaStar = Integer.MAX_VALUE;
        for (int i = 0; i < numberOfValues; i++) {
            if (betaStarList[i] == Integer.MAX_VALUE) {
                compressedSizeInBits += os.writeLong(huffmanCode.get(17).getKey(), huffmanCode.get(17).getValue()); // not erase
            } else {
                compressedSizeInBits += os.writeLong(huffmanCode.get(betaStarList[i]).getKey(), huffmanCode.get(betaStarList[i]).getValue());  // case 11, 2 + 4 = 6
                lastBetaStar = betaStarList[i];
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
        compressedSizeInBits += os.writeLong(huffmanCode.get(17).getKey(), huffmanCode.get(17).getValue()); // not erase
        compressedSizeInBits += xorCompressor.close();
    }

    public String getKey() {
        return getClass().getSimpleName();
    }

    public void refresh() {
        xorCompressor.refresh();
        compressedSizeInBits = 0;
        lastBetaStar = Integer.MAX_VALUE;
        numberOfValues = 0;
        os = xorCompressor.getOutputStream();
        huffmanCode.clear();
        Arrays.fill(leadDistribution, 0);
        Arrays.fill(trailDistribution, 0);
    }
}
