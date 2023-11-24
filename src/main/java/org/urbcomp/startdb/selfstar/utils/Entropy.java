package org.urbcomp.startdb.selfstar.utils;

import java.util.*;

public class Entropy {

    private final List<Long> xorList = new ArrayList<>();

    private final List<Long> centerList = new ArrayList<>();

    private final List<Integer> leadList = new ArrayList<>();

    private final List<Integer> trailList = new ArrayList<>();

    private final int dataSize;

    private final List<Long> vPrimeList = new ArrayList<>();

    private final List<Integer> betaStarList = new ArrayList<>();
    private final Map<LeadingTrailingPair, Map<Long, Integer>> conditionalCenterCount;
    private int lastBetaStar = Integer.MAX_VALUE;

    public Entropy(List<Double> values) {
        long lastValue = eraseValue(values.get(0));
        xorList.add(lastValue);
        leadList.add(0);
        trailList.add(0);
        centerList.add(lastValue);
        for (double value : values.subList(1, values.size())) {
            long curXor = eraseValue(value);
            long xor = lastValue ^ curXor;
//            System.out.println(Long.toBinaryString(xor));
            int trail = Long.numberOfTrailingZeros(xor);
            xorList.add(xor);
            leadList.add(Long.numberOfLeadingZeros(xor));
//            System.out.println("lead: " + Long.numberOfLeadingZeros(xor) + " trail: " + trail);
            trailList.add(trail);
            centerList.add(xor >>> trail);
            lastValue = curXor;
        }
        dataSize = values.size();
        conditionalCenterCount = getCCC();
    }

    private static double calculateSingleEntropy(Map<Long, Integer> countMap, int dataSize) {
        double entropy = 0.0;
        for (Map.Entry<Long, Integer> entry : countMap.entrySet()) {
            double probability = (double) entry.getValue() / dataSize;
            entropy -= probability * (Math.log(probability) / Math.log(2));
        }
        return entropy;
    }

    public static void main(String[] args) {
        List<Double> test = new ArrayList<>();
        test.add(0.1);
        test.add(0.1);
        test.add(0.2);
        test.add(0.2);
        test.add(0.3);
        test.add(0.3);
        test.add(0.3);
        test.add(0.2);


        Entropy entropy = new Entropy(test);

        System.out.println(entropy.calculateCodeBookBits());
    }

    public int getDataSize() {
        return dataSize;
    }

    public long eraseValue(double v) {
        long vLong = Double.doubleToRawLongBits(v);

        if (v == 0.0 || Double.isInfinite(v)) {
            vPrimeList.add(vLong);
            betaStarList.add(Integer.MAX_VALUE);
            return vLong;
        } else if (Double.isNaN(v)) {
            vPrimeList.add(0xfff8000000000000L & vLong);
            betaStarList.add(Integer.MAX_VALUE);
            return 0xfff8000000000000L & vLong;
        } else {
            // C1: v is a normal or subnormal
            int[] alphaAndBetaStar = Elf64Utils.getAlphaAndBetaStar(v, lastBetaStar);
            int e = ((int) (vLong >> 52)) & 0x7ff;
            int gAlpha = Elf64Utils.getFAlpha(alphaAndBetaStar[0]) + e - 1023;
            int eraseBits = 52 - gAlpha;
            long mask = 0xffffffffffffffffL << eraseBits;
            long delta = (~mask) & vLong;
            if (delta != 0 && eraseBits > 4) {  // C2
                if (alphaAndBetaStar[1] != lastBetaStar) {
                    lastBetaStar = alphaAndBetaStar[1];
                }
                betaStarList.add(lastBetaStar);
                vPrimeList.add(mask & vLong);
                return mask & vLong;
            } else {
                vPrimeList.add(vLong);
                betaStarList.add(Integer.MAX_VALUE);
                return vLong;
            }
        }
    }

    public double getLowerBound() {
        return calculateXorEntropy() + calculateBetaEntropy();
    }

    public double calculateXorEntropy() {
        // 统计每个元素的出现次数
        Map<Long, Integer> elementCount = new HashMap<>();
        for (Long element : xorList) {
            elementCount.put(element, elementCount.getOrDefault(element, 0) + 1);
        }
        // 计算熵
        return calculateSingleEntropy(elementCount, dataSize);
    }

    public double calculateBetaEntropy() {
        // 统计每个元素的出现次数
        Map<Long, Integer> elementCount = new HashMap<>();
        //BETA Entropy
        for (int element : betaStarList) {
            elementCount.put((long) element, elementCount.getOrDefault((long) element, 0) + 1);
        }
        return calculateSingleEntropy(elementCount, dataSize);
    }

    public double calculateBinEntropy() {
        double entropy = 0.0;
        for (Map<Long, Integer> centerCount : conditionalCenterCount.values()) {
            entropy += calculateSingleEntropy(centerCount, dataSize);
        }
        return entropy;
    }

    public double calculateCodeBookBits() {
        double centerBitNum = 0.0;
        double codeBookSize = 0;
        for (Map.Entry<LeadingTrailingPair, Map<Long, Integer>> entry : conditionalCenterCount.entrySet()) {
            codeBookSize += entry.getValue().size();
            for (Map.Entry<Long, Integer> entrySub : entry.getValue().entrySet()) {
//                System.out.println(entry.getKey().toString() + ", " + entrySub.getKey().toString() + ", " + entrySub.getValue()+", "+entry.getKey().getCenterNum());

                centerBitNum += entry.getKey().getCenterNum() * entrySub.getValue();
            }
        }
//        System.out.println("codeBookSize: " + codeBookSize);
//        System.out.println("centerBitNum: " + centerBitNum);
        return (codeBookSize * (Math.ceil(Math.log(codeBookSize) / Math.log(2)))) / dataSize;
    }

    public double getCodeBookSize() {
        double centerBitNum = 0.0;
        double codeBookSize = 0;
        for (Map.Entry<LeadingTrailingPair, Map<Long, Integer>> entry : conditionalCenterCount.entrySet()) {
            codeBookSize += entry.getValue().size();
            for (Map.Entry<Long, Integer> entrySub : entry.getValue().entrySet()) {
                System.out.println(entry.getKey().toString() + ", " + entrySub.getKey().toString() + ", " + entrySub.getValue() + ", " + entry.getKey().getCenterNum());
                centerBitNum += entry.getKey().getCenterNum() * entrySub.getValue();
            }
        }
        return codeBookSize;
    }

    private Map<LeadingTrailingPair, Map<Long, Integer>> getCCC() {
        Map<LeadingTrailingPair, Map<Long, Integer>> conditionalCenterCount = new HashMap<>();
//        Map<LeadingTrailingPair, Integer> pairCount = new HashMap<>();
        for (int i = 0; i < xorList.size(); i++) {
            LeadingTrailingPair pair = new LeadingTrailingPair(leadList.get(i), trailList.get(i));
//            pairCount.putIfAbsent(pair, 0);
//            pairCount.put(pair, pairCount.getOrDefault(pair, 0) + 1);
            conditionalCenterCount.putIfAbsent(pair, new HashMap<>());
            Map<Long, Integer> centerCount = conditionalCenterCount.get(pair);
            centerCount.put(centerList.get(i), centerCount.getOrDefault(centerList.get(i), 0) + 1);
        }
        return conditionalCenterCount;
    }

    static class LeadingTrailingPair {
        private final int leadingZeros;
        private final int trailingZeros;

        public LeadingTrailingPair(int leadingZeros, int trailingZeros) {
            this.leadingZeros = leadingZeros;
            this.trailingZeros = trailingZeros;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            LeadingTrailingPair that = (LeadingTrailingPair) obj;
            return leadingZeros == that.leadingZeros && trailingZeros == that.trailingZeros;
        }

        @Override
        public int hashCode() {
            return Objects.hash(leadingZeros, trailingZeros);
        }

        @Override
        public String toString() {
            return "leadingZeros=" + leadingZeros +
                    ", trailingZeros=" + trailingZeros;
        }

        public int getCenterNum() {
            if (trailingZeros == 64) {
                return 0;
            }
            if (leadingZeros == 64) {
                return 0;
            }
            return 64 - trailingZeros - leadingZeros;
        }
    }

}
