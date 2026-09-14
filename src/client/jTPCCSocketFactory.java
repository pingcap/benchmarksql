import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.protocol.StandardSocketFactory;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;

/** Records successful physical MySQL authentication, without issuing measurement SQL. */
public class jTPCCSocketFactory extends StandardSocketFactory {
    private static BufferedWriter events;
    private static long handshakes;
    private Socket recordedSocket;

    static synchronized void open(File file) throws IOException {
        if (events != null) throw new IllegalStateException("connection measurement already open");
        events = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8);
        handshakes = 0;
        events.write("event,epoch_ms,handshakes,local_port,remote_address,terminal\n");
        events.flush();
    }

    static synchronized long count() {
        return handshakes;
    }

    static void verify(Connection connection, long previousCount) throws SQLException {
        JdbcConnection mysql = connection.unwrap(JdbcConnection.class);
        if (!jTPCCSocketFactory.class.getName().equals(
                mysql.getPropertySet().getStringProperty("socketFactory").getValue())
                || count() <= previousCount) {
            throw new SQLException("Connection measurement did not observe this terminal's authentication");
        }
    }

    static synchronized void start(long timestamp) throws IOException {
        write("start", timestamp, 0, "");
    }

    static synchronized void finish(long timestamp) throws IOException {
        write("end", timestamp, 0, "");
        events.close();
        events = null;
    }

    private static void write(String event, long timestamp, int localPort, String remote) throws IOException {
        if (events == null) throw new IOException("Connection measurement output is unavailable");
        String thread = Thread.currentThread().getName();
        // TiDB's load-balancing driver also opens discovery sockets on its
        // background executor. Preserve those events without counting them as
        // terminal reconnects. Workload JDBC calls run on the named terminals.
        String terminal = thread.startsWith("benchmarksql-Term-") ? thread : "";
        events.write(event + "," + timestamp + "," + handshakes + "," + localPort + "," + remote + "," + terminal + "\n");
        events.flush();
    }

    @Override
    public void afterHandshake() throws IOException {
        super.afterHandshake();
        // Connector/J also invokes this callback for changeUser on an existing
        // socket. Count a socket once; a later successful physical reconnect
        // has a new socket even when the server reuses its connection ID.
        if (recordedSocket == rawSocket) return;
        synchronized (jTPCCSocketFactory.class) {
            handshakes++;
            write("authenticated", System.currentTimeMillis(), rawSocket.getLocalPort(),
                    rawSocket.getInetAddress().getHostAddress() + ":" + rawSocket.getPort());
        }
        recordedSocket = rawSocket;
    }
}
