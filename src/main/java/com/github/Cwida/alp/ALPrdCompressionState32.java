package com.github.Cwida.alp;

import java.util.HashMap;
import java.util.Map;

public class ALPrdCompressionState32 {
    public byte rightBw;
    public byte leftBw;
    public short exceptionsCount;
    public short[] leftPartsDict;
    public short[] exceptions;
    public short[] exceptionsPositions;
    public int leftBpSize;
    public int rightBpSize;
    public Map<Short, Short> leftPartsDictMap = new HashMap<>();

    public ALPrdCompressionState32() {
        this.rightBw = 0;
        this.leftBw = 0;
        this.exceptionsCount = 0;
        leftPartsDict = new short[ALPrdConstants.DICTIONARY_SIZE];
        exceptions = new short[ALPrdConstants.DICTIONARY_SIZE];
        exceptionsPositions = new short[ALPrdConstants.DICTIONARY_SIZE];
    }

    public void reset() {
        this.leftBpSize = 0;
        this.rightBpSize = 0;
        this.exceptionsCount = 0;
    }
}
