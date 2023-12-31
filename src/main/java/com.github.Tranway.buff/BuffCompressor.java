package com.github.Tranway.buff;

import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

import java.io.IOException;

public class BuffCompressor {
    private static int batchSize = 1000;
    private static final int[] PRECISION_MAP = new int[]{
            0, 5, 8, 11, 15, 18, 21, 25, 28, 31, 35, 38, 50, 52, 52, 52, 64, 64, 64, 64, 64, 64, 64
    };
    private static final long[] LAST_MASK = new long[]{
            0b1L, 0b11L, 0b111L, 0b1111L, 0b11111L, 0b111111L, 0b1111111L, 0b11111111L
    };
    private final OutputBitStream out;
    private long size;
    private long lowerBound;
    private int maxPrec;
    private int decWidth;
    private int intWidth;
    private int wholeWidth;
    private int columnCount;

    public BuffCompressor(int batchSize) {
        BuffCompressor.batchSize = batchSize;
        out = new OutputBitStream(new byte[100000]);
        size = 0;
    }

    private static int getWidthNeeded(long number) {
        if (number == 0) {
            return 0;
        }
        int bitCount = 0;
        while (number > 0) {
            bitCount++;
            number = number >>> 1; // 右移一位
        }
        return bitCount;
    }

    public static int getDecimalPlace(double db) {
        if (db == 0.0) {
            return 0;
        }
        String strDb = Double.toString(db);
        int indexOfDecimalPoint = strDb.indexOf('.');
        int cnt = 0;

        if (indexOfDecimalPoint >= 0) {
            for (int i = indexOfDecimalPoint + 1; i < strDb.length(); ++i) {
                if (strDb.charAt(i) != 'E') {
                    cnt++;
                } else {
                    i++;
                    cnt -= Integer.parseInt(strDb.substring(i));
                    return Math.max(cnt, 0);
                }
            }
            return cnt;
        } else {
            return 0; // 没有小数点，小数位数为0
        }
    }

    public static SparseResult findMajority(byte[] nums) {
        SparseResult result = new SparseResult(batchSize);
        byte candidate = 0;
        int count = 0;

        for (byte num : nums) {
            if (count == 0) {
                candidate = num;
                count = 1;
            } else if (num == candidate) {
                count++;
            } else {
                count--;
            }
        }

        // 验证候选元素是否确实出现频率达到90%以上
        count = 0;
        for (int i = 0; i < nums.length; ++i) {
            int index = i / 8; // 当前行所处的byte下标
            result.bitmap[index] = (byte) (result.bitmap[index] << 1);
            if (nums[i] == candidate) {
                count++;
            } else {
                result.bitmap[index] = (byte) (result.bitmap[index] | 0b1);
                result.outliers[result.outliersCnt++] = nums[i];
            }
            if (i + 1 == nums.length && (i + 1) % 8 != 0) {
                result.bitmap[index] = (byte) (result.bitmap[index] << (i % 8) + 1);
            }
        }

        if (count >= nums.length * 0.9) {
            result.flag = true;
            result.frequentValue = candidate;
        } else {
            result.flag = false;
        }
        return result;
    }

    public byte[] getOut() {
        return this.out.getBuffer();
    }

    public void compress(double[] values) {
        headSample(values);
        byte[][] cols = encode(values);
        size += out.writeLong(lowerBound, 64);
        size += out.writeInt(batchSize, 32);
        size += out.writeInt(maxPrec, 32);
        size += out.writeInt(intWidth, 32);
        if (wholeWidth >= 64) {
            wholeWidthLongCompress(values);
        } else {
            sparseEncode(cols);
        }
        close();
    }

    public void wholeWidthLongCompress(double[] values) {
        for (double value : values) {
            size += out.writeLong(Double.doubleToLongBits(value), 64);
        }
    }

    public void close() {
        out.writeInt(0, 8);
    }

    public long getSize() {
        return size;
    }

    public void headSample(double[] dbs) {
        lowerBound = Long.MAX_VALUE;
        long upperBound = Long.MIN_VALUE;
        for (double db : dbs) {
            // double -> bits
            long bits = Double.doubleToLongBits(db);
            long sign = bits >>> 63;
            // get the exp
            long expBinary = bits >>> 52 & 0x7FF;
            long exp = expBinary - 1023;
            // get the mantissa
            long mantissa = bits & 0x000fffffffffffffL; // 0.11  1   -0.12  -1

            // get the mantissa with implicit bit
            long implicitMantissa = mantissa | (1L << 52);

            // get the precision
            int prec = getDecimalPlace(db);

            // update the max prec
            if (prec > maxPrec) {
                maxPrec = prec;
            }

            // get the integer
            long integer = exp < 0 ? 0 : (implicitMantissa >>> (52 - exp));
            long integerValue = (sign == 0) ? integer : -integer;

            if (integerValue > upperBound) {
                upperBound = integerValue;
            }
            if (integerValue < lowerBound) {
                lowerBound = integerValue;
            }
        }

        // get the int_width
        intWidth = getWidthNeeded(upperBound - lowerBound);

        // get the dec_width
        decWidth = PRECISION_MAP[maxPrec];

        // get the whole_width
        wholeWidth = intWidth + decWidth + 1;

        // get the col/bytes needed
        columnCount = wholeWidth / 8;
        if (wholeWidth % 8 != 0) {
            columnCount++;
        }
    }

    public byte[][] encode(double[] dbs) {
        byte[][] cols = new byte[columnCount][dbs.length]; // 第一维代表列号，第二维代表行号

        int dbCnt = 0;
        for (double db : dbs) {
            // double -> bits
            long bits = Double.doubleToLongBits(db);
            // bits -> string

            // get the sign
            long sign = bits >>> 63;

            // get the exp
            long expBinary = bits >>> 52 & 0x7FF; // mask for the last 11 bits
            long exp = expBinary - 1023;

            // get the mantissa
            long mantissa = bits & 0x000fffffffffffffL;

            // get the mantissa with implicit bit
            long implicitMantissa = mantissa | (1L << 52);

            long decimal;
            if (exp >= 0) {
                decimal = mantissa << (12 + exp) >>> (64 - decWidth);
            } else {
                if (53 - decWidth >= 0) {
                    decimal = implicitMantissa >>> 53 - decWidth >>> (-exp - 1);
                } else {
                    decimal = implicitMantissa << decWidth - 53 >>> (-exp - 1);
                }
            }

            // get the integer
            long integer = exp < 0 ? 0 : (implicitMantissa >>> (52 - exp));
            long integerValue = (sign == 0) ? integer : -integer;

            // get the offset of integer
            long offset = integerValue - lowerBound;

            // get the bitpack result
            long bitpack = sign << (wholeWidth - 1) | (offset << decWidth) | decimal;

            // encode into cols[][]
            int remain = wholeWidth % 8;
            int bytesCnt = 0;
            if (remain != 0) {
                bytesCnt++;
                cols[columnCount - bytesCnt][dbCnt] = (byte) (bitpack & LAST_MASK[remain - 1]);
                bitpack = bitpack >>> remain;
            }
            while (bytesCnt < columnCount) {
                bytesCnt++;
                cols[columnCount - bytesCnt][dbCnt] = (byte) (bitpack & LAST_MASK[7]);
                bitpack = bitpack >>> 8;
            }

            dbCnt++;
        }
        return cols;
    }

    public void sparseEncode(byte[][] cols) {
        SparseResult result;
        for (int j = 0; j < columnCount; ++j) {
            // 遍历每一列，查找频繁项
            result = findMajority(cols[j]);

            // col serilize
            if (result.flag) {
                size += out.writeBit(true);
                serialize(result);
            } else {
                size += out.writeBit(false);
                try {
                    size += out.write(cols[j], batchSize * 8L);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void serialize(SparseResult sr) {
        size += out.writeInt(sr.frequentValue, 8);
        try {
            size += out.write(sr.bitmap, batchSize);
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < sr.outliersCnt; i++) {
            size += out.writeInt(sr.outliers[i], 8);
        }
    }
}
