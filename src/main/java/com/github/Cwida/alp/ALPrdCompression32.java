package com.github.Cwida.alp;

import org.urbcomp.startdb.selfstar.utils.OutputBitStream;

import java.util.*;

public class ALPrdCompression32 {
    static final byte EXACT_TYPE_BITSIZE = Float.SIZE;
    static ALPrdCompressionState32 state;
    private final OutputBitStream out;
    private long size;

    public void reset(){
        state.reset();
    }

    public ALPrdCompression32(OutputBitStream out, long size) {
        this.out = out;
        this.size = size;
    }

    public ALPrdCompression32(OutputBitStream out, long size, int vectorSize) {
        this.out = out;
        this.size = size;
        ALPrdConstants.setVectorSize(vectorSize);
        state = new ALPrdCompressionState32(vectorSize);
    }

    public static double estimateCompressionSize(byte rightBw, byte leftBw, short exceptionsCount, long sampleCount) {
        double exceptionsSize = exceptionsCount * ((ALPrdConstants.EXCEPTION_POSITION_SIZE + ALPrdConstants.EXCEPTION_SIZE) * 8);
        double estimatedSize = rightBw + leftBw + (exceptionsSize / sampleCount);
        return estimatedSize;
    }

    public static double buildLeftPartsDictionary(List<Integer> values, byte rightBw, byte leftBw,
                                                  boolean persistDict, ALPrdCompressionState32 state) {
        Map<Integer, Integer> leftPartsHash = new HashMap<>();
        List<Map.Entry<Integer, Integer>> leftPartsSortedRepetitions = new ArrayList<>();   // <出现次数，左值>

        // Building a hash for all the left parts and how many times they appear
        for (Integer value : values) {
            int leftTmp = value >>> rightBw;
            leftPartsHash.put(leftTmp, leftPartsHash.getOrDefault(leftTmp, 0) + 1);
        }

        // We build a list from the hash to be able to sort it by repetition count
        for (Map.Entry<Integer, Integer> entry : leftPartsHash.entrySet()) {
            leftPartsSortedRepetitions.add(new AbstractMap.SimpleEntry<>(entry.getValue(), entry.getKey()));
        }
        leftPartsSortedRepetitions.sort((a, b) -> Integer.compare(b.getKey(), a.getKey())); // 递减排序

        // Exceptions are left parts which do not fit in the fixed dictionary size
        int exceptionsCount = 0;
        for (int i = ALPrdConstants.DICTIONARY_SIZE; i < leftPartsSortedRepetitions.size(); i++) { // 超过字典容量的部分记为异常值
            exceptionsCount += leftPartsSortedRepetitions.get(i).getKey();
        }

        if (persistDict) {
            int dictIdx = 0;
            int dictSize = Math.min(ALPrdConstants.DICTIONARY_SIZE, leftPartsSortedRepetitions.size());
            for (; dictIdx < dictSize; dictIdx++) {
                //! The dict keys are mapped to the left part themselves
                state.leftPartsDict[dictIdx] = leftPartsSortedRepetitions.get(dictIdx).getValue().shortValue();
                state.leftPartsDictMap.put(state.leftPartsDict[dictIdx], (short) dictIdx);
            }
            //! Parallelly we store a map of the dictionary to quickly resolve exceptions during encoding
            for (int i = dictIdx; i < leftPartsSortedRepetitions.size(); i++) {
                state.leftPartsDictMap.put(leftPartsSortedRepetitions.get(i).getValue().shortValue(), (short) i);
            }
            state.leftBw = leftBw;
            state.rightBw = rightBw;
            state.exceptionsCount = (short) exceptionsCount;

//            assert state.leftBw > 0 && state.leftBw <= AlpRDConstants.CUTTING_LIMIT && state.rightBw > 0;
        }

        double estimatedSize = estimateCompressionSize(rightBw, ALPrdConstants.DICTIONARY_BW, (short) exceptionsCount, values.size());
        return estimatedSize;
    }

    public static double findBestDictionary(List<Integer> values, ALPrdCompressionState32 state) {
        int lBw = ALPrdConstants.DICTIONARY_BW;
        int rBw = EXACT_TYPE_BITSIZE;
        double bestDictSize = Integer.MAX_VALUE;

        //! Finding the best position to CUT the values
        for (int i = 1; i <= ALPrdConstants.CUTTING_LIMIT; i++) {
            byte candidateLBw = (byte) i;
            byte candidateRBw = (byte) (EXACT_TYPE_BITSIZE - i);
            double estimatedSize = buildLeftPartsDictionary(values, candidateRBw, candidateLBw, false, state);
            if (estimatedSize <= bestDictSize) {
                lBw = candidateLBw;
                rBw = candidateRBw;
                bestDictSize = estimatedSize;
            }
        }

        double bestEstimatedSize = buildLeftPartsDictionary(values, (byte) rBw, (byte) lBw, true, state);
        return bestEstimatedSize;
    }

    public long getSize() {
        return size;
    }

    public void compress(List<Integer> in, int nValues, ALPrdCompressionState32 state) {
        int[] rightParts = new int[ALPrdConstants.ALP_VECTOR_SIZE];
        short[] leftParts = new short[ALPrdConstants.ALP_VECTOR_SIZE];

        // Cutting the floating point values
        for (int i = 0; i < nValues; i++) {
            Integer tmp = in.get(i);
            rightParts[i] = tmp & ((1 << state.rightBw) - 1);
            leftParts[i] = (short) (tmp >>> state.rightBw);
        }

        // Dictionary encoding for left parts
        short exceptionsCount = 0;
        for (int i = 0; i < nValues; i++) {
            short dictionaryIndex;
            short dictionaryKey = leftParts[i];
            if (!state.leftPartsDictMap.containsKey(dictionaryKey)) {
                // If not found in the dictionary, store the smallest non-key index as an exception (the dict size)
                dictionaryIndex = ALPrdConstants.DICTIONARY_SIZE;
            } else {
                dictionaryIndex = state.leftPartsDictMap.get(dictionaryKey);
            }
            leftParts[i] = dictionaryIndex;

            // Left parts not found in the dictionary are stored as exceptions
            if (dictionaryIndex >= ALPrdConstants.DICTIONARY_SIZE) {
                leftParts[i] = 0;   // 用0替换
                state.exceptions[exceptionsCount] = dictionaryKey;
                state.exceptionsPositions[exceptionsCount] = (short) i;
                exceptionsCount++;
            }
        }

        size += out.writeBit(false);
        size += out.writeInt(nValues, 32);
        size += out.writeInt(state.rightBw, 8);
        for (int i = 0; i < nValues; i++) {
            size += out.writeInt(leftParts[i], ALPrdConstants.DICTIONARY_BW);
            size += out.writeInt(rightParts[i], state.rightBw);
        }
        for (int i = 0; i < ALPrdConstants.DICTIONARY_SIZE; i++) {
            size += out.writeInt(state.leftPartsDict[i], state.leftBw);
        }
        size += out.writeInt(exceptionsCount, 16);
        for (int i = 0; i < exceptionsCount; i++) {
            size += out.writeInt(state.exceptions[i], state.leftBw);
            size += out.writeInt(state.exceptionsPositions[i], 16);
        }
        /*
        TODO: bit pack
            useALP=0            不使用ALP压缩   bit
            nValues             向量长度        int
            rightBw             右值位宽        byte
            leftParts           左值部分        bits<ALPrdConstants.DICTIONARY_BW>[nValues]
            rightParts          右值部分        bits<rightBw>[nValues]
            leftPartsDict       左值字典        bits<leftBw>[ALPrdConstants.DICTIONARY_SIZE]
            exceptionsCount     异常值数量      short
            exceptions          异常值原值      bits<leftBw>[exceptionsCount]
            exceptionsPositions 异常值位置      short[exceptionsCount]
         */

//        // 以下为模拟调用ALPrdDecompression,仅供测试使用
//        ALPrdDecompression ALPrdDe = new ALPrdDecompression(leftParts, rightParts, state.leftPartsDict, nValues, state.exceptionsCount, state.exceptions, state.exceptionsPositions, state.rightBw);
//        double[] out = ALPrdDe.decompress();
//
//        String csvFile = "D:\\Code\\ALP\\src\\main\\java\\RDout.csv"; // 输出文件名
//
//        try (FileWriter writer = new FileWriter(csvFile, true)) {
//            for (double value : out) {
//                writer.append(String.valueOf(value)).append("\n"); // 写入每个值并在行尾添加换行符
//            }
//            System.out.println("CSV file was written successfully.");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    /**
     * ALPrd 算法入口
     *
     * @param row 单行数据
     */
    public void entry(List<Float> row) {
        List<Integer> rowLong = new ArrayList<>();
        for (float db : row) {
            rowLong.add(Float.floatToRawIntBits(db));
        }
        findBestDictionary(rowLong, state);
        compress(rowLong, rowLong.size(), state);
    }
}
