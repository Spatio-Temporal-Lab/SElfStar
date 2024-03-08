package com.github.Cwida.alp;

import java.util.ArrayList;
import java.util.List;

public class ALPCompressionState32 {
    public byte vectorExponent;
    public byte vectorFactor;
    public short exceptionsCount;
    public short bitWidth;
    public long frameOfReference;
    public long[] encodedIntegers;
    public float[] exceptions;
    public short[] exceptionsPositions;
    public List<ALPCombination> bestKCombinations = new ArrayList<>();

    public boolean useALP = true;

    public ALPCompressionState32() {
        this.vectorExponent = 0;
        this.vectorFactor = 0;
        this.exceptionsCount = 0;
        this.bitWidth = 0;
        this.encodedIntegers = new long[ALPConstants.ALP_VECTOR_SIZE];
        this.exceptions = new float[ALPConstants.ALP_VECTOR_SIZE];
        this.exceptionsPositions = new short[ALPConstants.ALP_VECTOR_SIZE];
    }

    public void reset() {
        this.vectorExponent = 0;
        this.vectorFactor = 0;
        this.exceptionsCount = 0;
        this.bitWidth = 0;
    }
}
