package org.urbcomp.startdb.selfstar.utils;

public class PostOfficeSolverNoFRZGPruning {
    //  (int) Math.ceil(Math.log(index) / Math.log(2))
    public static final int[] positionLength2Bits = {
        0, 0, 1, 2, 2, 3, 3, 3, 3,
        4, 4, 4, 4, 4, 4, 4, 4,
        5, 5, 5, 5, 5, 5, 5, 5,
        5, 5, 5, 5, 5, 5, 5, 5,
        6, 6, 6, 6, 6, 6, 6, 6,
        6, 6, 6, 6, 6, 6, 6, 6,
        6, 6, 6, 6, 6, 6, 6, 6,
        6, 6, 6, 6, 6, 6, 6, 6
    };

    /**
     * @param distribution   distribution of leading or trailing zeros
     * @param representation out param, representation of round
     * @param round          out param, round
     * @return positions
     */
    public static int[] initRoundAndRepresentation(int[] distribution, int[] representation, int[] round) {
        int totalCount = calTotalCount(distribution);
        int totalCost = Integer.MAX_VALUE;
        int[] positions = {};

        for (int z = 1; z < 64; z++) {
            int presentCost = totalCount * positionLength2Bits[z];
            PostOfficeResult por = PostOfficeSolverNoFRZGPruning.buildPostOffice(
                distribution, z);
            int tempTotalCost = por.getAppCost() + presentCost;
            if (tempTotalCost < totalCost) {
                totalCost = tempTotalCost;
                positions = por.getOfficePositions();
            }
        }

        representation[0] = 0;
        round[0] = 0;
        int i = 1;
        for (int j = 1; j < distribution.length; j++) {
            if (i < positions.length && j == positions[i]) {
                representation[j] = representation[j - 1] + 1;
                round[j] = j;
                i++;
            } else {
                representation[j] = representation[j - 1];
                round[j] = round[j - 1];
            }
        }

        return positions;
    }

    public static int writePositions(int[] positions, OutputBitStream out) {
        int thisSize = out.writeInt(positions.length, 5);
        for (int p : positions) {
            thisSize += out.writeInt(p, 6);
        }
        return thisSize;
    }

    private static int calTotalCount(int[] arr) {
        int totalCount = arr[0];
        for (int i = 1; i < arr.length; i++) {
            totalCount += arr[i];
        }
        return totalCount;
    }

    private static PostOfficeResult buildPostOffice(int[] arr, int num) {
        int[][] dp = new int[arr.length][num];      // 状态矩阵。d[i][j]表示，只考虑前i个居民点，且第i个位置是第j个邮局的总距离，i >= j，
        // 下标从0开始。注意，并非是所有居民点的总距离，因为没有考虑第j个邮局之后的居民点的距离
        int[][] pre = new int[arr.length][num];     // 对应于dp[i][j]，表示让dp[i][j]最小时，第j-1个邮局所在的位置信息

        dp[0][0] = 0;                       // 第0个位置是第0个邮局，此时状态为0
        pre[0][0] = -1;                     // 让dp[0][0]最小时，第-1个邮局所在的位置信息为-1

        for (int i = 1; i < arr.length; i++) {
            for (int j = Math.max(1, num + i - arr.length); j <= i && j < num; j++) {
                // arr.length - i < num - j，表示i后面的居民数（arr.length - i）不足以构建剩下的num - j个邮局
                if (i > 1 && j == 1) {
                    dp[i][j] = 0;
                    for (int k = 1; k < i; k++) {
                        dp[i][j] += arr[k] * k;
                    }
                    pre[i][j] = 0;
                } else {
                    int appCost = Integer.MAX_VALUE;
                    int preK = 0;
                    for (int k = j - 1; k <= i - 1; k++) {
                        int sum = dp[k][j - 1];
                        for (int p = k + 1; p <= i - 1; p++) {
                            sum += arr[p] * (p - k);
                        }
                        if (appCost > sum) {
                            appCost = sum;
                            preK = k;
                            if (sum == 0) { // 找到其中一个0，提前终止
                                break;
                            }
                        }
                    }
                    if (appCost != Integer.MAX_VALUE) {
                        dp[i][j] = appCost;
                        pre[i][j] = preK;
                    }
                }
            }
        }
        int tempTotalAppCost = Integer.MAX_VALUE;
        int tempBestLast = Integer.MAX_VALUE;
        for (int i = num - 1; i < arr.length; i++) {
            if (num - 1 == 0 && i > 0) {
                break;
            }
            int sum = dp[i][num - 1];
            for (int j = i + 1; j < arr.length; j++) {
                sum += arr[j] * (j - i);
            }
            if (tempTotalAppCost > sum) {
                tempTotalAppCost = sum;
                tempBestLast = i;
            }
        }

        int[] officePositions = new int[num];
        int i = 1;

        while (tempBestLast != -1) {
            officePositions[num - i] = tempBestLast;
            tempBestLast = pre[tempBestLast][num - i];
            i++;
        }

        return new PostOfficeResult(officePositions, tempTotalAppCost);
    }

    public static class PostOfficeResult {
        int[] officePositions;
        int totalAppCost;

        public PostOfficeResult(int[] officePositions, int totalCost) {
            this.officePositions = officePositions;
            this.totalAppCost = totalCost;
        }

        public int[] getOfficePositions() {
            return officePositions;
        }

        public int getAppCost() {
            return totalAppCost;
        }
    }
}
