package org.urbcomp.startdb.selfstar.compressor;

import org.urbcomp.startdb.selfstar.compressor.xor.IXORCompressor;
import org.urbcomp.startdb.selfstar.utils.Elf64Utils;
import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

import java.util.Arrays;

public class ElfStarCompressorNoHuff implements ICompressor {
    private final IXORCompressor xorCompressor;
    private final int[] betaStarList;
    private final long[] vPrimeList;
    private final int[] leadDistribution = new int[64];
    private final int[] trailDistribution = new int[64];
    private OutputBitStream os;
    private int compressedSizeInBits = 0;
    private int lastBetaStar = Integer.MAX_VALUE;
    private int numberOfValues = 0;

    public ElfStarCompressorNoHuff(IXORCompressor xorCompressor, int window) {
        this.xorCompressor = xorCompressor;
        this.os = xorCompressor.getOutputStream();
        this.betaStarList = new int[window + 1];     // one for the end sign
        this.vPrimeList = new long[window + 1];      // one for the end sign
    }

    public ElfStarCompressorNoHuff(IXORCompressor xorCompressor) {
        this(xorCompressor, 1000);
    }

    public void addValue(double v) {
        long vLong = Double.doubleToRawLongBits(v);

        if (v == 0.0 || Double.isInfinite(v)) {
            vPrimeList[numberOfValues] = vLong;
            betaStarList[numberOfValues] = Integer.MAX_VALUE;
        } else if (Double.isNaN(v)) {
            vPrimeList[numberOfValues] = 0x7ff8000000000000L;
            betaStarList[numberOfValues] = Integer.MAX_VALUE;
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
        xorCompressor.setDistribution(leadDistribution, trailDistribution);
        lastBetaStar = Integer.MAX_VALUE;
        for (int i = 0; i < numberOfValues; i++) {
            if (betaStarList[i] == Integer.MAX_VALUE) {
                compressedSizeInBits += os.writeInt(2, 2); // case 10
            } else if (betaStarList[i] == lastBetaStar) {
                compressedSizeInBits += os.writeBit(false);    // case 0
            } else {
                compressedSizeInBits += os.writeInt(betaStarList[i] | 0x30, 6);  // case 11, 2 + 4 = 6
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
        compressedSizeInBits += os.writeInt(2, 2);  // case 10
        compressedSizeInBits += xorCompressor.close();
    }

    public String getKey() {
        return "ElfStarNoHuff" + xorCompressor.getKey();
    }

    public void refresh() {
        xorCompressor.refresh();
        compressedSizeInBits = 0;
        lastBetaStar = Integer.MAX_VALUE;
        numberOfValues = 0;
        os = xorCompressor.getOutputStream();
        Arrays.fill(leadDistribution, 0);
        Arrays.fill(trailDistribution, 0);
    }
}
