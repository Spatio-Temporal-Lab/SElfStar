package transmit;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class BlockReader implements Closeable {
    private static final String dir = "src/main/resources/floating/";
    private final int blockSize;
    private final BufferedReader br;
    private boolean end = false;

    public BlockReader(String fileName, int blockSize) throws FileNotFoundException {
        this.br = new BufferedReader(new FileReader(dir + fileName));
        this.blockSize = blockSize;
    }

    public List<Double> nextBlock() throws IOException {
        if (end) {
            return null;
        }
        List<Double> floatings = new ArrayList<>(blockSize);
        int i = 0;
        String line;
        while (i < blockSize && (line = br.readLine()) != null) {
            if (line.startsWith("#") || line.equals("")) {
                continue;
            }
            i++;
            floatings.add(Double.parseDouble(line));

        }
        if (i < blockSize) {
            end = true;
        }
        if (floatings.isEmpty()) {
            return null;
        }

        return floatings;
    }


    public List<Float> nextSingleBlock() throws IOException {
        if (end) {
            return null;
        }
        List<Float> floatings = new ArrayList<>(blockSize);
        int i = 0;
        String line;
        while (i < blockSize && (line = br.readLine()) != null) {
            if (line.startsWith("#") || line.equals("")) {
                continue;
            }
            i++;
            floatings.add(Float.parseFloat(line));

        }
        if (i < blockSize) {
            end = true;
        }
        if (floatings.isEmpty()) {
            return null;
        }

        return floatings;
    }

    @Override
    public void close() throws IOException {
        br.close();
    }
}
