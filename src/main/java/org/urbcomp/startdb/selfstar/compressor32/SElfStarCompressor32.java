package org.urbcomp.startdb.selfstar.compressor32;

import org.urbcomp.startdb.selfstar.compressor32.xor.IXORCompressor32;
import org.urbcomp.startdb.selfstar.utils.Elf32Utils;
import org.urbcomp.startdb.selfstar.utils.Huffman.Code;
import org.urbcomp.startdb.selfstar.utils.Huffman.HuffmanEncode;
import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

import java.util.Arrays;

public class SElfStarCompressor32 implements ICompressor32 {
    private final IXORCompressor32 xorCompressor;

    private OutputBitStream os;

    private int compressedSizeInBits = 0;

    private int lastBetaStar = Integer.MAX_VALUE;

    private int numberOfValues = 0;

    private double storeCompressionRatio = 0;

    private boolean isFirstBlock = true; // mark if it is the first block

    private Code[] huffmanCode;

    private final int[] frequency = new int[9];

    public SElfStarCompressor32(IXORCompressor32 xorCompressor) {
        this.xorCompressor = xorCompressor;
        os = xorCompressor.getOutputStream();
    }

    @Override
    public void addValue(float v) {
        if (!isFirstBlock) {
            addValueHuffman(v);
        } else {
            addValueFirst(v);
        }
    }

    private void addValueFirst(float v) {
        int vInt = Float.floatToRawIntBits(v);
        int vPrimeInt;
        numberOfValues++;

        if (v == 0.0 || Float.isInfinite(v)) {
            compressedSizeInBits += os.writeInt(2, 2); // case 10
            vPrimeInt = vInt;
            frequency[8]++;
        } else if (Float.isNaN(v)) {
            compressedSizeInBits += os.writeInt(2, 2); // case 10
            vPrimeInt = 0x7fc00000 & vInt;
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
                if (alphaAndBetaStar[1] == lastBetaStar) {
                    compressedSizeInBits += os.writeBit(false);    // case 0
                } else {
                    compressedSizeInBits += os.writeInt(alphaAndBetaStar[1] | 0x18, 5);  // case 11, 2 + 4 = 6
                    lastBetaStar = alphaAndBetaStar[1];
                }
                vPrimeInt = mask & vInt;
                frequency[alphaAndBetaStar[1]]++;
            } else {
                compressedSizeInBits += os.writeInt(2, 2); // case 10
                vPrimeInt = vInt;
                frequency[8]++;
            }
        }
        compressedSizeInBits += xorCompressor.addValue(vPrimeInt);
    }

    private void addValueHuffman(float v) {
        int vInt = Float.floatToRawIntBits(v);
        int vPrimeInt;
        numberOfValues++;

        if (v == 0.0 || Float.isInfinite(v)) {
            compressedSizeInBits += os.writeInt(huffmanCode[8].code, huffmanCode[8].length); // not erase
            vPrimeInt = vInt;
            frequency[8]++;
        } else if (Float.isNaN(v)) {
            compressedSizeInBits += os.writeInt(huffmanCode[8].code, huffmanCode[8].length); // not erase
            vPrimeInt = 0x7fc00000;
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
                compressedSizeInBits += os.writeInt(huffmanCode[alphaAndBetaStar[1]].code, huffmanCode[alphaAndBetaStar[1]].length);  // case 11, 2 + 4 = 6
                lastBetaStar = alphaAndBetaStar[1];
                vPrimeInt = mask & vInt;
                frequency[alphaAndBetaStar[1]]++;
            } else {
                compressedSizeInBits += os.writeInt(huffmanCode[8].code, huffmanCode[8].length); // not erase
                vPrimeInt = vInt;
                frequency[8]++;
            }
        }
        compressedSizeInBits += xorCompressor.addValue(vPrimeInt);
    }

    @Override
    public double getCompressionRatio() {
        return compressedSizeInBits / (numberOfValues * 32.0);
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



    @Override
    public void setDistribution(int[] leadDistribution, int[] trailDistribution) {
        // for streaming scenarios, we do nothing here
    }

    @Override
    public void close() {
        double thisCompressionRatio = compressedSizeInBits / (numberOfValues * 32.0);
        if (storeCompressionRatio < thisCompressionRatio) {
            xorCompressor.setDistribution(null, null);
        }
        storeCompressionRatio = thisCompressionRatio;

        // we write one more bit here, for marking an end of the stream.
        if (isFirstBlock) {
            compressedSizeInBits += os.writeInt(2, 2);  // case 10
            isFirstBlock = false;
        } else {
            compressedSizeInBits += os.writeInt(huffmanCode[8].code, huffmanCode[8].length); // not erase
        }
        huffmanCode = HuffmanEncode.getHuffmanCodes(frequency);
        Arrays.fill(frequency, 0);
        compressedSizeInBits += xorCompressor.close();
    }


    @Override
    public void refresh() {
        compressedSizeInBits = 0;
        lastBetaStar = Integer.MAX_VALUE;
        numberOfValues = 0;

        xorCompressor.refresh();        // note this refresh should be at the last
        os = xorCompressor.getOutputStream();
    }
}
