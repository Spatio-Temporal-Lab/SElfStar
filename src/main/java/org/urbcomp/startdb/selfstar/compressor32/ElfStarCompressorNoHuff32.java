package org.urbcomp.startdb.selfstar.compressor32;

import org.urbcomp.startdb.selfstar.compressor32.xor.IXORCompressor32;
import org.urbcomp.startdb.selfstar.utils.Elf32Utils;
import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

import java.util.Arrays;

public class ElfStarCompressorNoHuff32 implements ICompressor32 {
    private final IXORCompressor32 xorCompressor;
    private final int[] betaStarList;
    private final int[] vPrimeList;
    private final int[] leadDistribution = new int[32];
    private final int[] trailDistribution = new int[32];
    private OutputBitStream os;
    private long compressedSizeInBits = 0;
    private int lastBetaStar = Integer.MAX_VALUE;
    private int numberOfValues = 0;

    public ElfStarCompressorNoHuff32(IXORCompressor32 xorCompressor) {
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
        } else if (Float.isNaN(v)) {
            vPrimeList[numberOfValues] = 0x7fc00000;
            betaStarList[numberOfValues] = Integer.MAX_VALUE;
        } else {
            // C1: v is a normal or subnormal
            int[] alphaAndBetaStar = Elf32Utils.getAlphaAndBetaStar(v, lastBetaStar);
            int e = ((vInt >> 23)) & 0xff;
            int gAlpha = Elf32Utils.getFAlpha(alphaAndBetaStar[0]) + e - 127;
            int eraseBits = 23 - gAlpha;
            int mask = 0xffffffff << eraseBits;
            int delta = (~mask) & vInt;
            if (delta != 0 && eraseBits > 3) {  // C2
                if (alphaAndBetaStar[1] != lastBetaStar) {
                    lastBetaStar = alphaAndBetaStar[1];
                }
                betaStarList[numberOfValues] = lastBetaStar;
                vPrimeList[numberOfValues] = mask & vInt;
            } else {
                betaStarList[numberOfValues] = Integer.MAX_VALUE;
                vPrimeList[numberOfValues] = vInt;
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
        xorCompressor.setDistribution(leadDistribution, trailDistribution);
        lastBetaStar = Integer.MAX_VALUE;
        for (int i = 0; i < numberOfValues; i++) {
            if (betaStarList[i] == Integer.MAX_VALUE) {
                compressedSizeInBits += os.writeInt(2, 2); // case 10
            } else if (betaStarList[i] == lastBetaStar) {
                compressedSizeInBits += os.writeBit(false);    // case 0
            } else {
                compressedSizeInBits += os.writeInt(betaStarList[i] | 0x18, 5);  // case 11, 2 + 3 = 5
                lastBetaStar = betaStarList[i];
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
        compressedSizeInBits += os.writeInt(2, 2);  // case 10
        compressedSizeInBits += xorCompressor.close();
    }

    public String getKey() {
        return xorCompressor.getKey();
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
