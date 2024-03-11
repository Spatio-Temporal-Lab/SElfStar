package transmit;

import com.actiontec.net.bandwidth.BandwidthLimiter;
import com.actiontec.net.bandwidth.UploadLimiter;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Optional;

public class Client {
    private static final String ip;
    private static final int defaultPort = 8080;

    static {
        try {
            ip = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private Socket clientSocket;
    private OutputStream os;

    public Client(int maxRate, Optional<Integer> port) {
        try {
            // 连接时间、延迟、带宽的权重(这个仅是优先权重，能改变带宽的能力有限)
            // clientSocket.setPerformancePreferences(1, 2, 1);
            clientSocket = new Socket(ip, port.orElse(defaultPort));
            UploadLimiter ul = new UploadLimiter(clientSocket.getOutputStream(), new BandwidthLimiter(maxRate));
            os = new DataOutputStream(ul);
        } catch (IOException c) {
            c.printStackTrace();
        }
    }

    public void send(byte[] data) throws IOException {
        os.write(data, 0, data.length);
        os.flush();
    }

    public void close() throws IOException {
        clientSocket.close();
    }
}
