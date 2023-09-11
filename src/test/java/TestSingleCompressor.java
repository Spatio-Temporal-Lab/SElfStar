import org.junit.jupiter.api.Test;
import org.urbcomp.startdb.selfStar.compressor.*;
import org.urbcomp.startdb.selfStar.compressor.xor.*;
import org.urbcomp.startdb.selfStar.compressor32.ElfStarCompressor32;
import org.urbcomp.startdb.selfStar.compressor32.ICompressor32;
import org.urbcomp.startdb.selfStar.compressor32.xor.ElfStarXORCompressor32;
import org.urbcomp.startdb.selfStar.decompressor.*;
import org.urbcomp.startdb.selfStar.decompressor.xor.*;
import org.urbcomp.startdb.selfStar.decompressor32.ElfStarDecompressor32;
import org.urbcomp.startdb.selfStar.decompressor32.IDecompressor32;
import org.urbcomp.startdb.selfStar.decompressor32.xor.ElfStarXORDecompressor32;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSingleCompressor {
    private static final String STORE_FILE = "src/test/resources/result/result.csv";
    private static final String STORE_WINDOW_FILE = "src/test/resources/result/resultWindow.csv";
    private static final String STORE_BLOCK_FILE = "src/test/resources/result/resultBlock.csv";
    private static final double TIME_PRECISION = 1000.0;
    private static final int BLOCK_SIZE = 1000;
    private static final int NO_PARAM = 0;
    private static final String INIT_FILE = "init.csv";     // warm up memory and cpu
    private final String[] fileNames = {
            INIT_FILE,
            "Air-pressure.csv",
            "Air-sensor.csv",
            "Basel-temp.csv",
            "Basel-wind.csv",
            "Bird-migration.csv",
            "Bitcoin-price.csv",
            "Blockchain-tr.csv",
            "City-lat.csv",
            "City-lon.csv",
            "City-temp.csv",
            "Dew-point-temp.csv",
            "electric_vehicle_charging.csv",
            "Food-price.csv",
            "IR-bio-temp.csv",
            "PM10-dust.csv",
            "POI-lat.csv",
            "POI-lon.csv",
            "SSD-bench.csv",
            "Stocks-DE.csv",
            "Stocks-UK.csv",
            "Stocks-USA.csv",
            "Wind-Speed.csv"
    };

    private final Map<String, Long> fileNameParamToTotalBits = new HashMap<>();
    private final Map<String, Long> fileNameParamToTotalBlock = new HashMap<>();
    private final Map<String, Long> fileNameParamMethodToCompressedBits = new HashMap<>();
    private final Map<String, Double> fileNameParamMethodToCompressTime = new HashMap<>();
    private final Map<String, Double> fileNameParamMethodToDecompressTime = new HashMap<>();
    private final Map<String, Double> fileNameParamMethodToCompressedRatio = new TreeMap<>();// use TreeMap to keep the order

    private static double[] toDoubleArray(byte[] byteArray) {
        int times = Double.SIZE / Byte.SIZE;
        double[] doubles = new double[byteArray.length / times];
        for (int i = 0; i < doubles.length; i++) {
            doubles[i] = ByteBuffer.wrap(byteArray, i * times, times).getDouble();
        }
        return doubles;
    }

    @Test
    public void testAllCompressor() {
        for (String fileName : fileNames) {
            System.out.println(fileName);
            testFloatingCompressor(fileName);
        }
        fileNameParamMethodToCompressedBits.forEach((fileNameParamMethod, compressedBits) -> {
            String fileNameParam = fileNameParamMethod.split(",")[0] + "," + fileNameParamMethod.split(",")[1];
            long fileTotalBits = fileNameParamToTotalBits.get(fileNameParam);
            fileNameParamMethodToCompressedRatio.put(fileNameParamMethod, (compressedBits * 1.0) / fileTotalBits);
        });
        System.out.println("Test All Compressor");
        writeResult(STORE_FILE, fileNameParamMethodToCompressedRatio, fileNameParamMethodToCompressTime, fileNameParamMethodToDecompressTime, fileNameParamToTotalBlock);
        System.gc();
    }


    private void testFloatingCompressor(String fileName) {

        String fileNameParam = fileName + "," + NO_PARAM;
        fileNameParamToTotalBits.put(fileNameParam, 0L);
        fileNameParamToTotalBlock.put(fileNameParam, 0L);
        ICompressor32[] compressors = new ICompressor32[]{
//                new BaseCompressor(new ChimpXORCompressor()),
//                new BaseCompressor(new ChimpNXORCompressor(128)),
//                new BaseCompressor(new GorillaXORCompressor()),
//                new ElfCompressor(new ElfXORCompressor()),
//                new ElfPlusCompressor(new ElfPlusXORCompressor()),
//                new ElfStarCompressor(new ElfStarXORCompressorAdaLead()),
//                new ElfStarCompressor(new ElfStarXORCompressorAdaLeadAdaTrail()),
                new ElfStarCompressor32(new ElfStarXORCompressor32()),
//                new SElfStarCompressor(new SElfXORCompressor()),
        };

        IDecompressor32[] decompressors = new IDecompressor32[]{
//                new BaseDecompressor(new ChimpXORDecompressor()),
//                new BaseDecompressor(new ChimpNXORDecompressor(128)),
//                new BaseDecompressor(new GorillaXORDecompressor()),
//                new ElfDecompressor(new ElfXORDecompressor()),
//                new ElfPlusDecompressor(new ElfPlusXORDecompressor()),
//                new ElfStarDecompressor(new ElfStarXORDecompressorAdaLead()),
//                new ElfStarDecompressor(new ElfStarXORDecompressorAdaLeadAdaTrail()),
                new ElfStarDecompressor32(new ElfStarXORDecompressor32()),
//                new ElfStarDecompressor(new ElfStarXORDecompressor())     // streaming version is the same
        };
        boolean firstMethod = true;
        for (int i = 0; i < compressors.length; i++) {
            ICompressor32 compressor = compressors[i];
            try (BlockReader br = new BlockReader(fileName, BLOCK_SIZE)) {
                List<Float> floatings;

                while ((floatings = br.nextSingleBlock()) != null) {

                    double compressTime = 0;
                    double decompressTime;
                    if (floatings.size() != BLOCK_SIZE) {
                        break;
                    }
                    if (firstMethod) {
                        fileNameParamToTotalBits.put(fileNameParam, fileNameParamToTotalBits.get(fileNameParam) + floatings.size() * 32);
                        fileNameParamToTotalBlock.put(fileNameParam, fileNameParamToTotalBlock.get(fileNameParam) + 1);
                    }
                    double start = System.nanoTime();
                    floatings.forEach(compressor::addValue);
                    compressor.close();
                    compressTime += (System.nanoTime() - start) / TIME_PRECISION;
                    IDecompressor32 decompressor = decompressors[i];
                    decompressor.setBytes(compressor.getBytes());

                    start = System.nanoTime();
                    List<Float> deValues = decompressor.decompress();
                    decompressTime = (System.nanoTime() - start) / TIME_PRECISION;

                    assertEquals(deValues.size(), floatings.size());
                    for (int j = 0; j < floatings.size(); j++) {
                        assertEquals(floatings.get(j), deValues.get(j));
                    }
                    String fileNameParamMethod = fileName + "," + NO_PARAM + "," + compressor.getKey();
                    if (!fileNameParamMethodToCompressedBits.containsKey(fileNameParamMethod)) {
                        fileNameParamMethodToCompressedBits.put(fileNameParamMethod, compressor.getCompressedSizeInBits());
                        fileNameParamMethodToCompressTime.put(fileNameParamMethod, compressTime);
                        fileNameParamMethodToDecompressTime.put(fileNameParamMethod, decompressTime);
                    } else {
                        long newSize = fileNameParamMethodToCompressedBits.get(fileNameParamMethod) + compressor.getCompressedSizeInBits();
                        double newCTime = fileNameParamMethodToCompressTime.get(fileNameParamMethod) + compressTime;
                        double newDTime = fileNameParamMethodToDecompressTime.get(fileNameParamMethod) + decompressTime;
                        fileNameParamMethodToCompressedBits.put(fileNameParamMethod, newSize);
                        fileNameParamMethodToCompressTime.put(fileNameParamMethod, newCTime);
                        fileNameParamMethodToDecompressTime.put(fileNameParamMethod, newDTime);
                    }
                    compressor.refresh();
                    decompressor.refresh();

                }
            } catch (Exception e) {
                throw new RuntimeException(fileName, e);
            }
            firstMethod = false;
        }
    }




    private void writeResult(String storeFile,
                             Map<String, Double> fileNameParamMethodToRatio,
                             Map<String, Double> fileNameParamMethodToCTime,
                             Map<String, Double> fileNameParamMethodToDTime,
                             Map<String, Long> fileNameParamToTotalBlock) {
        Map<String, List<Double>> methodToRatios = new TreeMap<>();
        Map<String, List<Double>> methodToCTimes = new HashMap<>();
        Map<String, List<Double>> methodToDTimes = new HashMap<>();

        for (String fileNameParamMethod : fileNameParamMethodToRatio.keySet()) {
            String fileName = fileNameParamMethod.split(",")[0];
            String param = fileNameParamMethod.split(",")[1];
            String method = fileNameParamMethod.split(",")[2];
            String fileNameParam = fileName + "," + param;
            if (fileName.equals(INIT_FILE)) {
                continue;
            }
            String paramMethod = param + "," + method;
            if (!methodToRatios.containsKey(paramMethod)) {
                methodToRatios.put(paramMethod, new ArrayList<>());
                methodToCTimes.put(paramMethod, new ArrayList<>());
                methodToDTimes.put(paramMethod, new ArrayList<>());
            }
            methodToRatios.get(paramMethod).add(fileNameParamMethodToRatio.get(fileNameParamMethod));
            methodToCTimes.get(paramMethod).add(fileNameParamMethodToCTime.get(fileNameParamMethod) / fileNameParamToTotalBlock.get(fileNameParam));
            methodToDTimes.get(paramMethod).add(fileNameParamMethodToDTime.get(fileNameParamMethod) / fileNameParamToTotalBlock.get(fileNameParam));
        }

        System.out.println("Average Performance");
        System.out.println("Param\tMethod\tRatio\tCTime\tDTime");
        for (String paramMethod : methodToRatios.keySet()) {
            System.out.print(paramMethod + "\t");
            System.out.print(methodToRatios.get(paramMethod).stream().mapToDouble(o -> o).average().orElse(0) + "\t");
            System.out.print(methodToCTimes.get(paramMethod).stream().mapToDouble(o -> o).average().orElse(0) + "\t");
            System.out.println(methodToDTimes.get(paramMethod).stream().mapToDouble(o -> o).average().orElse(0));
        }

        try {
            File file = new File(storeFile).getParentFile();
            if (!file.exists() && !file.mkdirs()) {
                throw new IOException("Create directory failed: " + file);
            }
            try (FileWriter writer = new FileWriter(storeFile, true)) {
                writer.write("Param, Method, Ratio, CTime, DTime");
                writer.write("\r\n");
                // 遍历键，并写入对应的值
                for (String fileNameParamMethod : fileNameParamMethodToRatio.keySet()) {
                    String fileNameParam = fileNameParamMethod.split(",")[0] + "," + fileNameParamMethod.split(",")[1];
                    writer.write(fileNameParamMethod);
                    writer.write(",");
                    writer.write(fileNameParamMethodToRatio.get(fileNameParamMethod).toString());
                    writer.write(",");
                    writer.write(fileNameParamMethodToCTime.get(fileNameParamMethod) / fileNameParamToTotalBlock.get(fileNameParam) + "");
                    writer.write(",");
                    writer.write(fileNameParamMethodToDTime.get(fileNameParamMethod) / fileNameParamToTotalBlock.get(fileNameParam) + "");
                    writer.write("\r\n");
                }
                System.out.println("Done!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
