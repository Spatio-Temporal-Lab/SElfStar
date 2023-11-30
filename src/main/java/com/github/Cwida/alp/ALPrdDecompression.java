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
    /*
        TODO: read
            nValues             向量长度        int                                             -> valuesCount
            rightBw             右值位宽        byte                                            -> rightBitWidth
            leftParts           左值部分        bits<ALPrdConstants.DICTIONARY_BW>[nValues]     -> leftEncoded
            rightParts          右值部分        bits<rightBw>[nValues]                          -> rightEncoded
            leftPartsDict       左值字典        bits<leftBw>[ALPrdConstants.DICTIONARY_SIZE]    -> leftPartsDict
            exceptionsCount     异常值数量      short                                           -> exceptionsCount
            exceptions          异常值原值      bits<leftBw>[exceptionsCount]                   -> exceptions
            exceptionsPositions 异常值位置      short[exceptionsCount]                          -> exceptionsPositions
         */
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

        // Exceptions Patching (exceptions only occur in left parts) 处理异常值【字典外的值】

        // 我猜测这里可能有点小问题

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