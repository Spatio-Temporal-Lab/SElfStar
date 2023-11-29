package com.github.Cwida.alp;

import org.urbcomp.startdb.selfstar.utils.InputBitStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ALPDecompression {
    private static final double[] FRAC_ARR = {
            1.0,
            0.1,
            0.01,
            0.001,
            0.0001,
            0.00001,
            0.000001,
            0.0000001,
            0.00000001,
            0.000000001,
            0.0000000001,
            0.00000000001,
            0.000000000001,
            0.0000000000001,
            0.00000000000001,
            0.000000000000001,
            0.0000000000000001,
            0.00000000000000001,
            0.000000000000000001,
            0.0000000000000000001,
            0.00000000000000000001
    };
    private final ALPrdDecompression ALPrdDe;
    private long[] encodedValue;
    private double[] output;
    private int count;
    private byte vectorFactor;
    private byte vectorExponent;
    private short exceptionsCount;
    private double[] exceptions;
    private short[] exceptionsPositions;
    private long frameOfReference;
    private short bitWidth;
    private final InputBitStream in;

    public ALPDecompression(byte[] bs) {
        in = new InputBitStream(bs);
        this.ALPrdDe = new ALPrdDecompression(in);
    }

    public void deserialize() {
        /*
            read
            ALPCombination      最佳<e,f>组合       byte + byte                 -> vectorExponent, vectorFactor
            bitWidth            FOR单值所需位宽      short                      -> bitWidth
            frameOfReference    FOR基准值           long                       -> frameOfReference
            nValues             向量长度            int                         -> count
            ForValues           FOR偏移值           bits<bitWidth>[nValues]    -> encodedValue
            exceptionsCount     异常值数量           short                      -> exceptionsCount
            exceptions          异常值原值           double[exceptionsCount]    -> exceptions
            exceptionsPositions 异常值位置           short[exceptionsCount]     -> exceptionsPositions
         */
        try {
            vectorExponent = (byte) in.readInt(8);
            vectorFactor = (byte) in.readInt(8);
            bitWidth = (short) in.readInt(16);
            frameOfReference = in.readLong(64);
            count = in.readInt(32);
            encodedValue = new long[count];
            for (int i = 0; i < count; i++) {
                encodedValue[i] = in.readLong(bitWidth);
            }
            exceptionsCount = (short) in.readInt(16);
            exceptions = new double[exceptionsCount];
            exceptionsPositions = new short[exceptionsCount];
            for (int i = 0; i < exceptionsCount; i++) {
                exceptions[i] = Double.longBitsToDouble(in.readLong(64));
                exceptionsPositions[i] = (short) in.readLong(16);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public double[] decompress() {
        output = new double[count];

        long factor = ALPConstants.U_FACT_ARR[vectorFactor];
        double exponent = FRAC_ARR[vectorExponent];

        // unFOR
        for (int i = 0; i < count; i++) {
            encodedValue[i] = frameOfReference + encodedValue[i];
        }

        // Decoding
        for (int i = 0; i < count; i++) {
            double encodedInteger = encodedValue[i];
            output[i] = (double) ((long) (encodedInteger)) * factor * exponent;
        }

        // Exceptions Patching
        for (int i = 0; i < exceptionsCount; i++) {
            output[exceptionsPositions[i]] = exceptions[i];
        }

        return output;
    }

    public List<double[]> entry() throws IOException {
        List<double[]> result = new ArrayList<>();
        int rowGroupSize = in.readInt(8);
        for (int i = 0; i < rowGroupSize; i++) {
            int useALP = in.readBit();
            if (useALP == 1) {
                deserialize();
                result.add(decompress());
            } else {
                ALPrdDe.deserialize();
                result.add(ALPrdDe.decompress());
            }
        }
        return result;
    }
}
