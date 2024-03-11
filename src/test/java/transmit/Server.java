package transmit;

import com.actiontec.net.bandwidth.BandwidthLimiter;
import com.actiontec.net.bandwidth.DownloadLimiter;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Optional;

public class Server {
    public static final int defaultPort = 8080;
    private ServerSocket serverSocket;
    private Socket listen;
    private OutputStream os;
    private InputStream is;

    public Server(int maxRate, Optional<Integer> listenPort) {
        try {
            serverSocket = new ServerSocket(listenPort.orElse(defaultPort));
            listen = serverSocket.accept();

            os = listen.getOutputStream();
            InputStream is_ = listen.getInputStream();
            DownloadLimiter dl = new DownloadLimiter(is_, new BandwidthLimiter(maxRate));
            is = new DataInputStream(dl);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public byte[] receiveBytes(int fixLen) throws IOException {
        try {
            byte[] data = new byte[fixLen];
            int totalBytesRead = 0;
            while (totalBytesRead < fixLen) {
                int curBytesRead = is.read(data, totalBytesRead, fixLen - totalBytesRead);
                totalBytesRead += curBytesRead;
            }
            return data;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    public void close() throws IOException {
        listen.close();
        serverSocket.close();
    }
}
