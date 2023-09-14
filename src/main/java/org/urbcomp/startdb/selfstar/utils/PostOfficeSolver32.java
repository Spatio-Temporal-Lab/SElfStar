package org.urbcomp.startdb.selfstar.utils;

public class PostOfficeSolver32 {
    // 2^index
    public static final int[] pow2z = {1, 2, 4, 8, 16};

    //  (int) Math.ceil(Math.log(index) / Math.log(2))
    public static final int[] positionLength2Bits = {
            0, 0, 1, 2, 2, 3, 3, 3, 3,
            4, 4, 4, 4, 4, 4, 4, 4,
            5, 5, 5, 5, 5, 5, 5, 5,
            5, 5, 5, 5, 5, 5, 5, 5,
    };

    /**
     * @param distribution   distribution of leading or trailing zeros
     * @param representation out param, representation of round
     * @param round          out param, round
     * @return positions
     */
    public static int[] initRoundAndRepresentation(int[] distribution, int[] representation, int[] round) {
        int[] preNonZerosCount = new int[distribution.length];   // 当前及前面的非零个数（包括当前）
        int[] postNonZerosCount = new int[distribution.length];  // 当前后面的非零个数（不包括当前）
        int[] totalCountAndNonZerosCount = calTotalCountAndNonZerosCounts(distribution, preNonZerosCount, postNonZerosCount);

        int maxZ = Math.min(positionLength2Bits[totalCountAndNonZerosCount[1]], 4); // 最多用4个bit来表示

        int totalCost = Integer.MAX_VALUE;
        int[] positions = {};

        for (int z = 0; z <= maxZ; z++) {
            int presentCost = totalCountAndNonZerosCount[0] * z;
            if (presentCost >= totalCost) {
                break;
            }
            int num = PostOfficeSolver32.pow2z[z];     // 邮局的总数量
            PostOfficeResult por = PostOfficeSolver32.buildPostOffice(
                    distribution, num, totalCountAndNonZerosCount[1], preNonZerosCount, postNonZerosCount);
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
        int thisSize = out.writeInt(positions.length, 4);
        for (int p : positions) {
            thisSize += out.writeInt(p, 5);
        }
        return thisSize;
    }

    /**
     * 一条直线上有居民点，每个居民点有不同的居民数。邮局只能建在居民点上。给定一个整型数组arr，
     * 下标表示居民点的位置，每个值表示居民点的居民数。再给定一个正整数num表示邮局的数量。
     * 每个居民只能前往其左边或者当前居民点的邮局，0坐标位置必须要建一个邮局。
     * 选择num个居民点建立num个邮局，使得所有居民到邮局的总距离最短。
     *
     * @param arr The location of the settlement and the corresponding number of inhabitants
     * @param num Number of post offices to be built
     */
    public static PostOfficeResult buildPostOffice(int[] arr, int num) {
        int[] preNonZerosCount = new int[arr.length];   // 当前及前面的非零个数（包括当前）
        int[] postNonZerosCount = new int[arr.length];  // 当前后面的非零个数（不包括当前）
        int nonZerosCount = calTotalCountAndNonZerosCounts(arr, preNonZerosCount, postNonZerosCount)[1];

        return buildPostOffice(arr, num, nonZerosCount, preNonZerosCount, postNonZerosCount);
    }

    private static int[] calTotalCountAndNonZerosCounts(int[] arr, int[] outPreNonZerosCount, int[] outPostNonZerosCount) {
        int nonZerosCount = arr.length;
        int totalCount = arr[0];
        outPreNonZerosCount[0] = 1;            // 第一个视为非零
        for (int i = 1; i < arr.length; i++) {
            totalCount += arr[i];
            if (arr[i] == 0) {
                nonZerosCount--;
                outPreNonZerosCount[i] = outPreNonZerosCount[i - 1];
            } else {
                outPreNonZerosCount[i] = outPreNonZerosCount[i - 1] + 1;
            }
        }
        for (int i = 0; i < arr.length; i++) {
            outPostNonZerosCount[i] = nonZerosCount - outPreNonZerosCount[i];
        }
        return new int[]{totalCount, nonZerosCount};
    }

    private static PostOfficeResult buildPostOffice(int[] arr, int num, int nonZerosCount, int[] preNonZerosCount, int[] postNonZerosCount) {
        int originalNum = num;
        num = Math.min(num, nonZerosCount);

        int[][] dp = new int[arr.length][num];      // 状态矩阵。d[i][j]表示，只考虑前i个居民点，且第i个位置是第j个邮局的总距离，i >= j，
        // 下标从0开始。注意，并非是所有居民点的总距离，因为没有考虑第j个邮局之后的居民点的距离
        int[][] pre = new int[arr.length][num];     // 对应于dp[i][j]，表示让dp[i][j]最小时，第j-1个邮局所在的位置信息

        dp[0][0] = 0;                       // 第0个位置是第0个邮局，此时状态为0
        pre[0][0] = -1;                     // 让dp[0][0]最小时，第-1个邮局所在的位置信息为-1

        for (int i = 1; i < arr.length; i++) {
            if (arr[i] == 0) {
                continue;
            }
            for (int j = Math.max(1, num + i - arr.length); j <= i && j < num; j++) {
                // arr.length - i < num - j，表示i后面的居民数（arr.length - i）不足以构建剩下的num - j个邮局
                if (i > 1 && j == 1) {
                    dp[i][j] = 0;
                    for (int k = 1; k < i; k++) {
                        dp[i][j] += arr[k] * k;
                    }
                    pre[i][j] = 0;
                } else {
                    if (preNonZerosCount[i] < j + 1 || postNonZerosCount[i] < num - 1 - j) {
                        continue;
                    }
                    int appCost = Integer.MAX_VALUE;
                    int preK = 0;
                    for (int k = j - 1; k <= i - 1; k++) {
                        if (arr[k] == 0 && k > 0) {
                            continue;
                        }
                        if (preNonZerosCount[k] < j || postNonZerosCount[k] < num - j) {
                            continue;
                        }
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
            if (arr[i] == 0 && i > 0) {
                continue;
            }
            if (preNonZerosCount[i] < num) {
                continue;
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

        if (originalNum > nonZerosCount) {
            int[] modifyingOfficePositions = new int[originalNum];
            int j = 0, k = 0;
            while (j < originalNum && k < num) {
                if (j - k < originalNum - num && j < officePositions[k]) {
                    modifyingOfficePositions[j] = j;
                    j++;
                } else {
                    modifyingOfficePositions[j] = officePositions[k];
                    j++;
                    k++;
                }
            }
            officePositions = modifyingOfficePositions;
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
