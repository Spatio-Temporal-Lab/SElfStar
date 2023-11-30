package com.github.Cwida.alp;

import java.util.HashMap;
import java.util.Map;

public class ALPrdCompressionState {
    public byte rightBw;
    public byte leftBw;
    public short exceptionsCount;
    public short[] leftPartsDict = new short[ALPrdConstants.DICTIONARY_SIZE];
    public short[] exceptions;
    public short[] exceptionsPositions;
    public int leftBpSize;
    public int rightBpSize;
    public Map<Short, Short> leftPartsDictMap = new HashMap<>();

    public ALPrdCompressionState() {
        this.rightBw = 0;
        this.leftBw = 0;
        this.exceptionsCount = 0;
    }

    public ALPrdCompressionState(int vecterSize) {
        this.rightBw = 0;
        this.leftBw = 0;
        this.exceptionsCount = 0;
        exceptions = new short[vecterSize];
        exceptionsPositions = new short[vecterSize];
    }

    public void reset() {
        this.leftBpSize = 0;
        this.rightBpSize = 0;
        this.exceptionsCount = 0;
    }
}
