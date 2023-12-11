package com.github.Cwida.alp;

import org.urbcomp.startdb.selfstar.utils.InputBitStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ALPDecompression32 {
    private static final float[] FRAC_ARR = {
            1.0F,
            0.1F,
            0.01F,
            0.001F,
            0.0001F,
            0.00001F,
            0.000001F,
            0.0000001F,
            0.00000001F,
            0.000000001F,
            0.0000000001F,
            0.00000000001F,
            0.000000000001F,
            0.0000000000001F,
            0.00000000000001F,
            0.000000000000001F,
            0.0000000000000001F,
            0.00000000000000001F,
            0.000000000000000001F,
            0.0000000000000000001F,
            0.00000000000000000001F
    };
    private final ALPrdDecompression32 ALPrdDe;
    private long[] encodedValue;
    private int count;
    private byte vectorFactor;
    private byte vectorExponent;
    private short exceptionsCount;
    private float[] exceptions;
    private short[] exceptionsPositions;
    private long frameOfReference;
    private final InputBitStream in;

    public ALPDecompression32(byte[] bs) {
        in = new InputBitStream(bs);
        this.ALPrdDe = new ALPrdDecompression32(in);
    }

    public void deserialize() {
        try {
            vectorExponent = (byte) in.readInt(8);
            vectorFactor = (byte) in.readInt(8);
            short bitWidth = (short) in.readInt(16);
            frameOfReference = in.readLong(64);
            count = in.readInt(32);
            encodedValue = new long[count];
            for (int i = 0; i < count; i++) {
                encodedValue[i] = in.readLong(bitWidth);
            }
            exceptionsCount = (short) in.readInt(16);
            exceptions = new float[exceptionsCount];
            exceptionsPositions = new short[exceptionsCount];
            for (int i = 0; i < exceptionsCount; i++) {
                exceptions[i] = Float.intBitsToFloat(in.readInt(32));
                exceptionsPositions[i] = (short) in.readLong(16);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public float[] decompress() {
        float[] output = new float[count];

        long factor = ALPConstants.FACT_ARR[vectorFactor];
        float exponent = FRAC_ARR[vectorExponent];

        // unFOR
        for (int i = 0; i < count; i++) {
            encodedValue[i] = frameOfReference + encodedValue[i];
        }

        // Decoding
        for (int i = 0; i < count; i++) {
            long encodedInteger = encodedValue[i];
            output[i] = encodedInteger * factor * exponent;
        }

        // Exceptions Patching
        for (int i = 0; i < exceptionsCount; i++) {
            output[exceptionsPositions[i]] = exceptions[i];
        }

        return output;
    }

    public List<float[]> entry() throws IOException {
        List<float[]> result = new ArrayList<>();
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
