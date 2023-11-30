package com.github.Cwida.alp;


import java.util.ArrayList;
import java.util.List;

public class ALPCompressionState {
    public byte vectorExponent;
    public byte vectorFactor;
    public short exceptionsCount;
    public short bitWidth;
    public long frameOfReference;
    public long[] encodedIntegers = new long[1000];
    public double[] exceptions = new double[1000];
    public short[] exceptionsPositions = new short[1000];
    public List<ALPCombination> bestKCombinations = new ArrayList<>();

    public boolean useALP = true;

    public ALPCompressionState() {
        this.vectorExponent = 0;
        this.vectorFactor = 0;
        this.exceptionsCount = 0;
        this.bitWidth = 0;
    }

    public ALPCompressionState(int vectorSize) {
        this.vectorExponent = 0;
        this.vectorFactor = 0;
        this.exceptionsCount = 0;
        this.bitWidth = 0;
        this.encodedIntegers = new long[vectorSize];
        this.exceptions = new double[vectorSize];
        this.exceptionsPositions = new short[vectorSize];
    }

    public void reset() {
        this.vectorExponent = 0;
        this.vectorFactor = 0;
        this.exceptionsCount = 0;
        this.bitWidth = 0;
    }

}