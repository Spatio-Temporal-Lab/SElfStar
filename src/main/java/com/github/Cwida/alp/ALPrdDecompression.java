package com.github.Cwida.alp;

import org.urbcomp.startdb.selfstar.utils.InputBitStream;

import java.io.IOException;

public class ALPrdDecompression {
    private int nValues;
    private byte rightBW;
    private final InputBitStream in;
    private long[] rightEncoded;
    private int[] leftEncoded;
    private int[] leftPartsDict;
    private int exceptionsCount;
    private int[] exceptions;
    private int[] exceptionsPositions;

    public ALPrdDecompression(InputBitStream in) {
        this.in = in;
    }

    public void deserialize() {
        try {
            nValues = in.readInt(32);
            rightBW = (byte) in.readInt(8);
            leftEncoded = new int[nValues];
            rightEncoded = new long[nValues];
            for (int i = 0; i < nValues; i++) {
                leftEncoded[i] = in.readInt(ALPrdConstants.DICTIONARY_BW);
                rightEncoded[i] = in.readLong(rightBW);
            }
            int leftBW = 64 - rightBW;
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

    public double[] decompress() {
        long[] outputLong = new long[nValues];
        double[] output = new double[nValues];

        // Decoding 拼接
        for (int i = 0; i < nValues; i++) {
            int left = leftPartsDict[leftEncoded[i]];
            long right = rightEncoded[i];
            outputLong[i] = ((long) left << rightBW) | right;
        }

        // Exceptions Patching (exceptions only occur in left parts)
        for (int i = 0; i < exceptionsCount; i++) {
            long right = rightEncoded[exceptionsPositions[i]];
            int left = exceptions[i];
            outputLong[exceptionsPositions[i]] = (((long) left << rightBW) | right);
        }

        for (int i = 0; i < nValues; i++) {
            output[i] = Double.longBitsToDouble(outputLong[i]);
        }
        return output;
    }
}