import org.junit.jupiter.api.Test;
import org.urbcomp.startdb.selfstar.utils.Entropy;

import java.util.ArrayList;
import java.util.List;

public class TeatEntropy {
    private static final String INIT_FILE = "init.csv";     // warm up memory and cpu
    private final String[] fileNames = {
//            INIT_FILE,
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


    @Test
    public void calEntropy() {
        for (String fileName : fileNames) {
            List<Double> allFloatings = new ArrayList<>();
            List<Double> entropyAll = new ArrayList<>();
            int blockNum = 0;
            try (BlockReader br = new BlockReader(fileName, 1000)) {
                List<Double> floatings;
                while ((floatings = br.nextBlock()) != null) {
                    allFloatings.addAll(floatings);

                    Entropy entropy = new Entropy(floatings);
                    entropyAll.add(entropy.getLowerBound());
//                    System.out.println(entropy.getLowerBound());
//                    System.out.println(entropy.getCodeBookSize());
                    blockNum++;
                }
            } catch (Exception e) {
                throw new RuntimeException(fileName, e);
            }
            double ens = 0.0;
            for (double en : entropyAll) {
                ens += en;
            }
            ens = ens / blockNum;

            Entropy entropy = new Entropy(allFloatings);
            System.out.println(fileName + ", " + (entropy.getLowerBound() / 64));
            System.out.println("AVG: " + (ens / 64));
        }
    }
}
