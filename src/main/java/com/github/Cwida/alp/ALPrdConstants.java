package com.github.Cwida.alp;

public class ALPrdConstants {
    public static int ALP_VECTOR_SIZE = 1000;
    public static final byte DICTIONARY_BW = 3;
    public static final byte DICTIONARY_SIZE = 1 << DICTIONARY_BW; // 8
    public static final byte CUTTING_LIMIT = 16;
    public static final byte EXCEPTION_SIZE = Short.BYTES;
    public static final byte EXCEPTION_POSITION_SIZE = Short.BYTES;

    public static void setVectorSize(int vectorSize){
        ALP_VECTOR_SIZE = vectorSize;
    }
}
