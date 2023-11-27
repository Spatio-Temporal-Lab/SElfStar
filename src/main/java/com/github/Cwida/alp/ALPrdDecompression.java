package com.github.Cwida.alp;

import org.urbcomp.startdb.selfstar.utils.InputBitStream;

import java.io.IOException;

public class ALPrdDecompression {
    private int nValues;
    private byte rightBW;
    private short[] leftEncoded;
    private long[] rightEncoded;
    private short[] leftPartsDict;
    private short exceptionsCount;
    private short[] exceptions;
    private short[] exceptionsPositions;
    private InputBitStream in;

    public ALPrdDecompression(InputBitStream in) {
        this.in = in;
    }

    public ALPrdDecompression(short[] leftEncoded, long[] rightEncoded, short[] leftPartsDict, int valuesCount, short exceptionsCount, short[] exceptions, short[] exceptionsPositions, byte rightBitWidth) {
        // 仅供测试使用
        this.leftEncoded = leftEncoded;
        this.rightEncoded = rightEncoded;
        this.leftPartsDict = leftPartsDict;
        this.nValues = valuesCount;
        this.exceptionsCount = exceptionsCount;
        this.exceptions = exceptions;
        this.exceptionsPositions = exceptionsPositions;
        this.rightBW = rightBitWidth;
    }

    public void deserialize() {
        try {
            nValues = in.readInt(32);
            rightBW = (byte) in.readInt(8);
            for (int i = 0; i < nValues; i++) {
                leftEncoded[i] = (short) in.readLong(ALPrdConstants.DICTIONARY_BW);
                rightEncoded[i] = in.readLong(rightBW);
            }
            int leftBW = 64 - rightBW;
            for (int i = 0; i < ALPrdConstants.DICTIONARY_SIZE; i++) {
                leftPartsDict[i] = (short) in.readLong(leftBW);
            }
            exceptionsCount = (short) in.readInt(16);
            for (int i = 0; i < exceptionsCount; i++) {
                exceptions[i] = (short) in.readLong(leftBW);
                exceptionsPositions[i] = (short) in.readLong(16);
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
        deserialize();

        long[] outputLong = new long[nValues];
        double[] output = new double[nValues];

        // Decoding 拼接
        for (int i = 0; i < nValues; i++) {
            short left = leftPartsDict[leftEncoded[i]];
            long right = rightEncoded[i];
            outputLong[i] = ((long) left << rightBW) | right;
        }

        // Exceptions Patching (exceptions only occur in left parts) 处理异常值【字典外的值】
        for (int i = 0; i < exceptionsCount; i++) {
            long right = rightEncoded[exceptionsPositions[i]];
            short left = exceptions[i];
            outputLong[exceptionsPositions[i]] = (((long) left << rightBW) | right);
        }

        for (int i = 0; i < nValues; i++) {
            output[i] = Double.longBitsToDouble(outputLong[i]);
        }
        return output;
    }
}