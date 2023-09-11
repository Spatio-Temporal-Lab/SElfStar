package org.urbcomp.startdb.selfStar.utils;

public class Elf32Utils {
    private final static int[] f =
            new int[]{0, 4, 7, 10, 14, 17, 20, 24, 27, 30, 34, 37, 40, 44, 47, 50, 54, 57,
                    60, 64, 67};

    private final static float[] map10iP =
            new float[]{1.0f, 1.0E1f, 1.0E2f, 1.0E3f, 1.0E4f, 1.0E5f, 1.0E6f, 1.0E7f,
                    1.0E8f, 1.0E9f, 1.0E10f, 1.0E11f, 1.0E12f, 1.0E13f, 1.0E14f,
                    1.0E15f, 1.0E16f, 1.0E17f, 1.0E18f, 1.0E19f, 1.0E20f};
    private final static double LOG_2_10 = Math.log(10) / Math.log(2);
    public final static int END_SIGN = Float.floatToIntBits(Float.NaN);

}
