package com.github.Cwida.alp;

import org.urbcomp.startdb.selfstar.utils.InputBitStream;

import java.io.IOException;

public class ALPrdDecompression32 {
    private int nValues;
    private byte rightBW;
    private final InputBitStream in;
    private int[] rightEncoded;
    private int[] leftEncoded;
    private int[] leftPartsDict;
    private int exceptionsCount;
    private int[] exceptions;
    private int[] exceptionsPositions;

    public ALPrdDecompression32(InputBitStream in) {
        this.in = in;
    }

    public void deserialize() {
        try {
            nValues = in.readInt(32);
            rightBW = (byte) in.readInt(8);
            leftEncoded = new int[nValues];
            rightEncoded = new int[nValues];
            for (int i = 0; i < nValues; i++) {
                leftEncoded[i] = in.readInt(ALPrdConstants.DICTIONARY_BW);
                rightEncoded[i] = in.readInt(rightBW);
            }
            int leftBW = 32 - rightBW;
            leftPartsDict = new int[ALPrdConstants.DICTIONARY_SIZE];
            for (int i = 0; i < ALPrdConstants.DICTIONARY_SIZE; i++) {
                leftPartsDict[i] = in.readInt(leftBW);
            }
            exceptionsCount = in.readInt(16);
            exceptions = new int[exceptionsCount];
            exceptionsPositions = new int[exceptionsCount];
            for (int i = 0; i < exceptionsCount; i++) {
                exceptions[i] = in.readInt(leftBW);
                exceptionsPositions[i] = in.readInt(16);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public float[] decompress() {
        int[] outputLong = new int[nValues];
        float[] output = new float[nValues];

        // Decoding 拼接
        for (int i = 0; i < nValues; i++) {
            int left = leftPartsDict[leftEncoded[i]];
            int right = rightEncoded[i];
            outputLong[i] = (left << rightBW) | right;
        }

        // Exceptions Patching (exceptions only occur in left parts)
        for (int i = 0; i < exceptionsCount; i++) {
            long right = rightEncoded[exceptionsPositions[i]];
            int left = exceptions[i];
            outputLong[exceptionsPositions[i]] = (int) ((left << rightBW) | right);
        }

        for (int i = 0; i < nValues; i++) {
            output[i] = Float.floatToRawIntBits(outputLong[i]);
        }
        return output;
    }
}