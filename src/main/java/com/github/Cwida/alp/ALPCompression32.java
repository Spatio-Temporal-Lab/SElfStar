package com.github.Cwida.alp;

import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

import java.io.IOException;
import java.util.*;

public class ALPCompression32 {
    static final float MAGIC_NUMBER = 12582912.0F; //(float) (Math.pow(2, 22) + Math.pow(2, 23));
    static final byte MAX_EXPONENT = 10;
    static final byte EXACT_TYPE_BIT_SIZE = Float.SIZE;
    private static final float[] EXP_ARR = {
            1.0F,
            10.0F,
            100.0F,
            1000.0F,
            10000.0F,
            100000.0F,
            1000000.0F,
            10000000.0F,
            100000000.0F,
            1000000000.0F,
            10000000000.0F,
    };
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
            0.0000000001F
    };
    static ALPCompressionState32 state;
    private final OutputBitStream out;
    ALPrdCompression32 aLPrd;
    private long size;

    public void reset(){
        state.reset();
        aLPrd.reset();
    }

    public long getSize(){
        return size;
    }

    public ALPCompression32() {
        this.out = new OutputBitStream(
                new byte[7000000]);
        this.aLPrd = new ALPrdCompression32(out, size);
        size = 0;
        state = new ALPCompressionState32();
    }

    /**
     * 用于将float转为long，来自文章中的fast rounding部分
     *
     * @param db a float value
     * @return n
     */
    public static long floatToLong(float db) {
        float n = db + MAGIC_NUMBER - MAGIC_NUMBER;
        return (long) n;
    }


    public void compress(List<Float> inputVector, int nValues, ALPCompressionState32 state) {
        if (state.bestKCombinations.size() > 1) {
            findBestFactorAndExponent(inputVector, nValues, state);
        } else {
            state.vectorExponent = state.bestKCombinations.get(0).e;
            state.vectorFactor = state.bestKCombinations.get(0).f;
        }

        // Encoding Floating-Point to Int64
        //! We encode all the values regardless of their correctness to recover the original floating-point
        //! We detect exceptions later using a predicated comparison
        List<Float> tmpDecodedValues = new ArrayList<>(nValues);  // Tmp array to check wether the encoded values are exceptions
        for (int i = 0; i < nValues; i++) {
            float db = inputVector.get(i);
            float tmpEncodedValue = db * EXP_ARR[state.vectorExponent] * FRAC_ARR[state.vectorFactor];
            long encodedValue = floatToLong(tmpEncodedValue);
            state.encodedIntegers[i] = encodedValue;

            float decodedValue = encodedValue * ALPConstants.FACT_ARR[state.vectorFactor] * FRAC_ARR[state.vectorExponent];
            tmpDecodedValues.add(decodedValue);
        }

        // Detecting exceptions with predicated comparison
        List<Short> exceptionsPositions = new ArrayList<>(nValues);
        for (int i = 0; i < nValues; i++) {
            float decodedValue = tmpDecodedValues.get(i);
            float actualValue = inputVector.get(i);
            boolean isException = (decodedValue != actualValue) || Float.floatToRawIntBits(actualValue)==-2147483648;  // 将-0.00归为异常值
            if (isException)
                exceptionsPositions.add((short) i);
        }

        // Finding first non exception value
        long aNonExceptionValue = 0;
        for (int i = 0; i < nValues; i++) {
            if (i == exceptionsPositions.size() || i != exceptionsPositions.get(i)) {
                aNonExceptionValue = state.encodedIntegers[i];
                break;
            }
        }

        // Replacing that first non exception value on the vector exceptions
        short exceptionsCount = 0;
        for (short exceptionPos : exceptionsPositions) {
            float actualValue = inputVector.get(exceptionPos);
            state.encodedIntegers[exceptionPos] = aNonExceptionValue;
            state.exceptions[exceptionsCount] = actualValue;
            state.exceptionsPositions[exceptionsCount] = exceptionPos;
            exceptionsCount++;
        }
        state.exceptionsCount = exceptionsCount;

        // Analyze FFOR
        long minValue = Long.MAX_VALUE;
        long maxValue = Long.MIN_VALUE;
        for (int i = 0; i < nValues; i++) {
            long encodedValue = state.encodedIntegers[i];
            maxValue = Math.max(maxValue, encodedValue);
            minValue = Math.min(minValue, encodedValue);
        }
        long minMaxDiff = maxValue - minValue;

        // Subtract FOR
        for (int i = 0; i < nValues; i++) {
            state.encodedIntegers[i] -= minValue;
        }

        int bitWidth = getWidthNeeded(minMaxDiff); // FFOR单值所需位宽

        state.bitWidth = (short) bitWidth;
        state.frameOfReference = minValue;

        size += out.writeBit(true);
        size += out.writeInt(state.vectorExponent, 8);
        size += out.writeInt(state.vectorFactor, 8);
        size += out.writeInt(bitWidth, 16);
        size += out.writeLong(state.frameOfReference, 64);
        size += out.writeInt(nValues, 32);
        for (int i = 0; i < nValues; i++) {
            size += out.writeLong(state.encodedIntegers[i], bitWidth);
        }
        size += out.writeInt(state.exceptionsCount, 16);
        for (int i = 0; i < state.exceptionsCount; i++) {
            size += out.writeInt(Float.floatToRawIntBits(state.exceptions[i]), 32);
            size += out.writeInt(state.exceptionsPositions[i], 16);
        }
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

    public byte[] getOut() {
        int byteCount = (int) Math.ceil(size / 8.0);
        return Arrays.copyOf(out.getBuffer(), byteCount);
    }

    public void close() {
        try {
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void flush() {
        out.flush();
    }

    /**
     * ALP & ALPrd 算法压缩入口
     *
     * @param rowGroup 输入行组
     */
    public void entry(List<List<Float>> rowGroup) {
        List<List<Float>> vectorsSampled = new ArrayList<>();
        int idxIncrements = Math.max(1, (int) Math.ceil((double) rowGroup.size() / ALPConstants.RG_SAMPLES)); // 用于行组采样的向量下标增量
        for (int i = 0; i < rowGroup.size(); i += idxIncrements) {
            vectorsSampled.add(rowGroup.get(i));
        }
        // 第一级采样
        findTopKCombinations(vectorsSampled);
        size += out.writeLong(rowGroup.size(), 8);
        if (!state.useALP) {
            // use ALPrd
            for (List<Float> row : rowGroup) {  // 逐行处理
                aLPrd.entry(row);
            }

            size += aLPrd.getSize();
        } else {
            // use ALP
            for (List<Float> row : rowGroup) {  // 逐行处理
                // 第二级采样，获取最佳组合
                findBestFactorAndExponent(row, row.size(), state);
                // 压缩处理
                compress(row, row.size(), state);
            }
        }
    }

    // 第一级采样
    private void findTopKCombinations(List<List<Float>> vectorsSampled) {
        state.bestKCombinations.clear();
        Map<Map.Entry<Byte, Byte>, Integer> bestKCombinationsHash = new HashMap<>();    // 记录每个组合出现的次数
        for (List<Float> sampledVector : vectorsSampled) {
            int nSamples = sampledVector.size();
            byte bestFactor = MAX_EXPONENT;
            byte bestExponent = MAX_EXPONENT;

            // Initialize bestTotalBits using the worst possible total bits obtained from compression【异常值大小+正常值大小】
            long bestTotalBits = (long) nSamples * (EXACT_TYPE_BIT_SIZE + ALPConstants.EXCEPTION_POSITION_SIZE) + (long) nSamples * EXACT_TYPE_BIT_SIZE;

            // Try all combinations in search for the one which minimize the compression size
            for (byte expIdx = MAX_EXPONENT; expIdx >= 0; expIdx--) {
                for (byte factorIdx = expIdx; factorIdx >= 0; factorIdx--) {
                    int exceptionsCnt = 0;  // 记录异常值出现次数
                    int nonExceptionsCnt = 0;   // 记录能够正常编码的次数
                    int estimatedBitsPerValue;  // 预计单值正常编码所需要的位宽
                    long estimatedCompressionSize = 0;  // 预计编码后所需的总位数
                    long maxEncodedValue = Long.MIN_VALUE;
                    long minEncodedValue = Long.MAX_VALUE;

                    int idxIncrements = Math.max(1, (int) Math.ceil((float) sampledVector.size() / ALPConstants.SAMPLES_PER_VECTOR)); // 用于行组采样的向量下标增量
                    for (int dbIndex = 0; dbIndex < sampledVector.size(); dbIndex += idxIncrements) {
                        float db = sampledVector.get(dbIndex);
                        float tmp_encoded_value = db * EXP_ARR[expIdx] * FRAC_ARR[factorIdx];
                        long encoded_value = floatToLong(tmp_encoded_value);    // 对应ALPenc

                        // The cast to float is needed to prevent a signed integer overflow
                        float decoded_value = (float) (encoded_value) * ALPConstants.FACT_ARR[factorIdx] * FRAC_ARR[expIdx];    // 对应Pdec
                        if (decoded_value == db) {
                            nonExceptionsCnt++;
                            maxEncodedValue = Math.max(encoded_value, maxEncodedValue);
                            minEncodedValue = Math.min(encoded_value, minEncodedValue);
                        } else {
                            exceptionsCnt++;
                        }
                    }
                    // Skip combinations which yields to almost all exceptions
                    // Also skip combinations which yields integers bigger than 2^48
                    if (nonExceptionsCnt < ALPConstants.SAMPLES_PER_VECTOR * 0.5 || maxEncodedValue >= 1L << 48) {
                        continue;
                    }
                    // Evaluate factor/exponent compression size (we optimize for FOR)
                    long delta = maxEncodedValue - minEncodedValue;
                    estimatedBitsPerValue = (int) Math.ceil(Math.log(delta + 1) / Math.log(2));   // FOR单值位宽
                    estimatedCompressionSize += (long) nSamples * estimatedBitsPerValue;    // 正常编码的部分
                    estimatedCompressionSize += (long) exceptionsCnt * (EXACT_TYPE_BIT_SIZE + ALPConstants.EXCEPTION_POSITION_SIZE);   // 异常值部分

                    // 更新单个向量中的最佳组合
                    if ((estimatedCompressionSize < bestTotalBits) ||
                            // We prefer bigger exponents
                            (estimatedCompressionSize == bestTotalBits && (bestExponent < expIdx)) ||
                            // We prefer bigger factors
                            ((estimatedCompressionSize == bestTotalBits && bestExponent == expIdx) && (bestFactor < factorIdx))) {
                        bestTotalBits = estimatedCompressionSize;
                        bestExponent = expIdx;
                        bestFactor = factorIdx;
                    }
                }
            }
            // 更新行组中的最佳组合
            if (bestTotalBits != (long) nSamples * (EXACT_TYPE_BIT_SIZE + ALPConstants.EXCEPTION_POSITION_SIZE) + (long) nSamples * EXACT_TYPE_BIT_SIZE) {
                Map.Entry<Byte, Byte> bestCombination = new AbstractMap.SimpleEntry<>(bestExponent, bestFactor);
                int cnt = bestKCombinationsHash.getOrDefault(bestCombination, 0);
                bestKCombinationsHash.put(bestCombination, cnt + 1);
            }
        }

        // Convert our hash pairs to a Combination vector to be able to sort
        List<ALPCombination> bestKCombinations = new ArrayList<>();
        bestKCombinationsHash.forEach((key, value) -> bestKCombinations.add(new ALPCombination(key.getKey(),  // Exponent
                key.getValue(), // Factor
                value           // N of times it appeared (hash value)
        )));

        // 使用 List.sort() 进行排序，传入自定义的比较器     等效于C++中的 sort(bestKCombinations.begin(), bestKCombinations.end(), compareALPCombinations);
        bestKCombinations.sort((c1, c2) -> {
            if (ALPCombination.compareALPCombinations(c1, c2)) {
                return -1; // 返回负值表示 c1 应排在 c2 前面
            } else {
                return 1;
            }
        });

        // Save k' best combinations
        for (int i = 0; i < Math.min(ALPConstants.MAX_COMBINATIONS, (byte) bestKCombinations.size()); i++) {
            state.bestKCombinations.add(bestKCombinations.get(i));
        }

        // 判断是否使用ALPrd
        state.useALP = !bestKCombinations.isEmpty();
    }

    // 第二级采样
    private void findBestFactorAndExponent(List<Float> inputVector, int nValues, ALPCompressionState32 state) {
        // We sample equidistant values within a vector; to do this we skip a fixed number of values
        List<Float> vectorSample = new ArrayList<>();
        int idxIncrements = Math.max(1, (int) Math.ceil((float) nValues / ALPConstants.SAMPLES_PER_VECTOR));
        for (int i = 0; i < nValues; i += idxIncrements) {
            vectorSample.add(inputVector.get(i));
        }

        byte bestExponent = 0;
        byte bestFactor = 0;
        long bestTotalBits = 0;
        int worseTotalBitsCounter = 0;
        int nSamples = vectorSample.size();

        // We try each K combination in search for the one which minimize the compression size in the vector
        for (int combinationIdx = 0; combinationIdx < state.bestKCombinations.size(); combinationIdx++) {
            byte exponentIdx = state.bestKCombinations.get(combinationIdx).e;
            byte factorIdx = state.bestKCombinations.get(combinationIdx).f;
            int exceptionsCount = 0;
            long estimatedCompressionSize = 0;
            long maxEncodedValue = Long.MIN_VALUE;
            long minEncodedValue = Long.MAX_VALUE;

            for (float db : vectorSample) {
                float tmpEncodedValue = db * EXP_ARR[exponentIdx] * FRAC_ARR[factorIdx];
                long encodedValue = (long) tmpEncodedValue;

                float decodedValue = encodedValue * ALPConstants.FACT_ARR[factorIdx] * FRAC_ARR[exponentIdx];
                if (decodedValue == db) {
                    maxEncodedValue = Math.max(encodedValue, maxEncodedValue);
                    minEncodedValue = Math.min(encodedValue, minEncodedValue);
                } else {
                    exceptionsCount++;
                }
            }

            long delta = Math.abs(maxEncodedValue - minEncodedValue);
            int estimatedBitsPerValue = (int) Math.ceil(Math.log(delta + 1) / Math.log(2));
            estimatedCompressionSize += (long) nSamples * estimatedBitsPerValue;
            estimatedCompressionSize += exceptionsCount * (ALPConstants.EXCEPTION_POSITION_SIZE * 8 + EXACT_TYPE_BIT_SIZE);

            if (combinationIdx == 0) {
                bestTotalBits = estimatedCompressionSize;
                bestFactor = factorIdx;
                bestExponent = exponentIdx;
                continue;
            }

            if (estimatedCompressionSize >= bestTotalBits) {
                worseTotalBitsCounter += 1;
                // 贪婪提前推出【连续两个组合未优于之前的最佳组合】
                if (worseTotalBitsCounter == ALPConstants.SAMPLING_EARLY_EXIT_THRESHOLD) {
                    break;
                }
                continue;
            }

            bestTotalBits = estimatedCompressionSize;
            bestFactor = factorIdx;
            bestExponent = exponentIdx;
            worseTotalBitsCounter = 0;
        }
        state.vectorExponent = bestExponent;
        state.vectorFactor = bestFactor;
    }
}
