package org.urbcomp.startdb.selfstar.compressor;

import org.urbcomp.startdb.selfstar.compressor.xor.IXORCompressor;
import org.urbcomp.startdb.selfstar.utils.Elf64Utils;
import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

import java.util.Arrays;

public class SElfStar2Compressor implements ICompressor {
    private final IXORCompressor xorCompressor;
    private final int[] betaStarDistribution = new int[32];
    private OutputBitStream os;
    private int compressedSizeInBits = 0;
    private int lastBetaStar = Integer.MAX_VALUE;
    private int numberOfValues = 0;
    private double storeCompressionRatio = 0;

    public SElfStar2Compressor(IXORCompressor xorCompressor) {
        this.xorCompressor = xorCompressor;
        os = xorCompressor.getOutputStream();
    }

    public void addValue(double v) {
        long vLong = Double.doubleToRawLongBits(v);
        long vPrimeLong;
        numberOfValues++;

        if (v == 0.0 || Double.isInfinite(v)) {
            compressedSizeInBits += os.writeInt(2, 2); // case 10
            vPrimeLong = vLong;
        } else if (Double.isNaN(v)) {
            compressedSizeInBits += os.writeInt(2, 2); // case 10
            vPrimeLong = 0xfff8000000000000L & vLong;
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
                    int deltaBeta = alphaAndBetaStar[1] - lastBetaStar;
                    if (deltaBeta >= -2 && deltaBeta <= 2) {
                        compressedSizeInBits += os.writeInt(0b110, 3); //110
                        compressedSizeInBits += os.writeInt(Elf64Utils.zEncode(deltaBeta), 2); //110
                    } else {
                        compressedSizeInBits += os.writeInt(alphaAndBetaStar[1] | 0b1110000, 7);  // case 111, 3 + 4 = 7
                    }
                    lastBetaStar = alphaAndBetaStar[1];
                }
                vPrimeLong = mask & vLong;
            } else {
                compressedSizeInBits += os.writeInt(2, 2); // case 10
                vPrimeLong = vLong;
            }
        }
        compressedSizeInBits += xorCompressor.addValue(vPrimeLong);
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

    @Override
    public void setDistribution(int[] leadDistribution, int[] trailDistribution) {
        // for streaming scenarios, we do nothing here
    }


    public void close() {
        double thisCompressionRatio = compressedSizeInBits / (numberOfValues * 64.0);
        if (storeCompressionRatio < thisCompressionRatio) {
            xorCompressor.setDistribution(null, null);
        }
        storeCompressionRatio = thisCompressionRatio;

        // we write one more bit here, for marking an end of the stream.
        compressedSizeInBits += os.writeInt(2, 2);  // case 10
        compressedSizeInBits += xorCompressor.close();
    }


    public String getKey() {
        return getClass().getSimpleName();
    }

    public void refresh() {
        compressedSizeInBits = 0;
        lastBetaStar = Integer.MAX_VALUE;
        numberOfValues = 0;

        xorCompressor.refresh();        // note this refresh should be at the last
        os = xorCompressor.getOutputStream();
    }
}
