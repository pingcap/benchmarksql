import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.FutureTask;

/** Drives Connector/J's real authentication callback over a loopback MySQL socket. */
public class ConnectionMeasurementTest {
    private static void packet(OutputStream stream, int sequence, byte[] payload) throws Exception {
        stream.write(payload.length);
        stream.write(payload.length >> 8);
        stream.write(payload.length >> 16);
        stream.write(sequence);
        stream.write(payload);
        stream.flush();
    }

    private static void readPacket(InputStream stream) throws Exception {
        int length = stream.read();
        if (length < 0) throw new EOFException();
        length |= stream.read() << 8;
        length |= stream.read() << 16;
        if (stream.read() < 0) throw new EOFException();
        for (int i = 0; i < length; i++) if (stream.read() < 0) throw new EOFException();
    }

    private static byte[] greeting() throws Exception {
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        data.write(10);
        data.write("8.0.33\0".getBytes(StandardCharsets.US_ASCII));
        data.write(new byte[] {1, 0, 0, 0});
        data.write("12345678\0".getBytes(StandardCharsets.US_ASCII));
        // PROTOCOL_41, SECURE_CONNECTION, PLUGIN_AUTH and LONG_PASSWORD.
        data.write(new byte[] {1, (byte) 0x82, 45, 2, 0, 8, 0, 21});
        data.write(new byte[10]);
        data.write("abcdefghijkl\0mysql_native_password\0".getBytes(StandardCharsets.US_ASCII));
        return data.toByteArray();
    }

    private static void connect(boolean authenticate) throws Exception {
        try (ServerSocket listener = new ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())) {
            FutureTask<Void> server = new FutureTask<>(() -> {
                try (Socket socket = listener.accept()) {
                    socket.setSoTimeout(5000);
                    InputStream input = socket.getInputStream();
                    OutputStream output = socket.getOutputStream();
                    packet(output, 0, greeting());
                    readPacket(input);
                    if (authenticate) {
                        packet(output, 2, new byte[] {0, 0, 0, 2, 0, 0, 0});
                        readPacket(input);
                        // Authentication has completed, but stop JDBC setup.
                        packet(output, 1, "\377\323\004#HY000fixture stops after authentication".getBytes(StandardCharsets.ISO_8859_1));
                    } else {
                        packet(output, 2, "\377\025\004#28000authentication denied".getBytes(StandardCharsets.ISO_8859_1));
                    }
                }
                return null;
            });
            Thread thread = new Thread(server, "mysql-handshake-fixture");
            thread.setDaemon(true);
            thread.start();
            try {
                DriverManager.getConnection("jdbc:mysql://127.0.0.1:" + listener.getLocalPort()
                        + "/?useSSL=false&connectTimeout=5000&socketTimeout=5000&socketFactory=jTPCCSocketFactory", "root", "");
                throw new AssertionError("fixture must stop before JDBC setup completes");
            } catch (SQLException expected) {
                if (expected.getErrorCode() != (authenticate ? 1235 : 1045)) throw expected;
            }
            server.get(10, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    public static void main(String[] args) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        Path output = Files.createTempFile("benchmarksql-connections-", ".csv");
        try {
            jTPCCSocketFactory.open(output.toFile());
            connect(false);
            if (jTPCCSocketFactory.count() != 0) throw new AssertionError("failed auth counted");
            connect(true);
            if (jTPCCSocketFactory.count() != 1) throw new AssertionError("initial auth missing");
            jTPCCSocketFactory.start(System.currentTimeMillis());
            String originalName = Thread.currentThread().getName();
            try {
                Thread.currentThread().setName("benchmarksql-Term-01");
                connect(true);
            } finally {
                Thread.currentThread().setName(originalName);
            }
            if (jTPCCSocketFactory.count() != 2) throw new AssertionError("new physical auth missing");
            jTPCCSocketFactory.finish(System.currentTimeMillis());
            List<String> rows = Files.readAllLines(output, StandardCharsets.UTF_8);
            if (rows.size() != 5 || !rows.get(2).startsWith("start,") || !rows.get(4).startsWith("end,"))
                throw new AssertionError(rows);
            if (!rows.get(3).endsWith(",benchmarksql-Term-01") || !rows.get(1).endsWith(","))
                throw new AssertionError("terminal and background sockets must be distinguishable");
            System.out.println("Connection measurement: real successful and failed authentication boundaries passed");
        } finally {
            Files.deleteIfExists(output);
        }
    }
}
