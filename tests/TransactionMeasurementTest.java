import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import org.apache.log4j.Logger;

/** Exercises transaction result rows against JDBC success and failure boundaries. */
public class TransactionMeasurementTest {
    private static final Logger LOG = Logger.getLogger(TransactionMeasurementTest.class);

    private static <T> T jdbc(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler));
    }

    private static final class Database {
        final SQLException failure = new SQLException("injected failure", "40001", 1213);
        boolean failStatement;
        boolean failCommit;
        boolean failRollback;
        int commits;
        int rollbacks;

        Connection connection() {
            return jdbc(Connection.class, (proxy, method, args) -> {
                switch (method.getName()) {
                    case "prepareStatement": return statement();
                    case "commit":
                        if (failCommit) throw failure;
                        commits++;
                        return null;
                    case "rollback":
                        if (failRollback) throw new SQLException("lost connection", "08006");
                        rollbacks++;
                        return null;
                    default: throw new AssertionError(method);
                }
            });
        }

        PreparedStatement statement() {
            return jdbc(PreparedStatement.class, (proxy, method, args) -> {
                String name = method.getName();
                if (name.startsWith("set") || name.equals("addBatch") || name.equals("clearBatch")) return null;
                if (failStatement && name.startsWith("execute")) throw failure;
                if (name.equals("executeUpdate")) return 1;
                if (name.equals("executeBatch")) return new int[] {1};
                if (name.equals("executeQuery")) {
                    boolean[] returned = {false};
                    return jdbc(ResultSet.class, (result, getter, columns) -> {
                        switch (getter.getName()) {
                            case "next":
                                if (returned[0]) return false;
                                returned[0] = true;
                                return true;
                            case "getInt": return 1;
                            case "getDouble": return 1.0;
                            case "getString": return "GC";
                            case "getTimestamp": return new Timestamp(1);
                            case "close": return null;
                            default: throw new AssertionError(getter);
                        }
                    });
                }
                throw new AssertionError(method);
            });
        }
    }

    private static final class Input extends jTPCCRandom {
        boolean intentionalRollback;

        @Override
        public int nextInt(int low, int high) {
            if (low == 1 && high == 100) return intentionalRollback ? 1 : 50;
            return low;
        }

        @Override
        public int getItemID() { return 1; }
    }

    private static String[] executeNewOrder(Input input, Database database, boolean fatal) throws Exception {
        jTPCCTData transaction = new jTPCCTData();
        transaction.setNumWarehouses(1);
        transaction.setWarehouse(1);
        transaction.setDistrict(1);
        transaction.generateNewOrder(LOG, input, 0);
        long start = System.currentTimeMillis();
        try {
            transaction.execute(LOG, new jTPCCConnection(database.connection(), jTPCCConfig.DB_MYSQL));
            if (fatal) throw new AssertionError("rollback failure must propagate");
        } catch (Exception error) {
            if (!fatal) throw error;
        }
        String[] row = transaction.resultLine(start).trim().split(",");
        if (row.length != 7 || Long.parseLong(row[0]) < 0 || Long.parseLong(row[2]) < 0) {
            throw new AssertionError("every attempt needs completed timing and one full CSV row");
        }
        if (!row[3].equals("NEW_ORDER")) throw new AssertionError("transaction type lost");
        return row;
    }

    private static void outcome(String[] row, String rollback, String error) {
        if (!row[4].equals(rollback) || !row[6].equals(error)) {
            throw new AssertionError("wrong rollback/error classification: " + String.join(",", row));
        }
    }

    public static void main(String[] args) throws Exception {
        Input input = new Input();
        outcome(executeNewOrder(input, new Database(), false), "0", "0");
        input.intentionalRollback = true;
        outcome(executeNewOrder(input, new Database(), false), "1", "0");
        input.intentionalRollback = false;

        Database statementFailure = new Database();
        statementFailure.failStatement = true;
        outcome(executeNewOrder(input, statementFailure, false), "0", "1");

        Database commitFailure = new Database();
        commitFailure.failCommit = true;
        outcome(executeNewOrder(input, commitFailure, false), "0", "1");

        Database lostConnection = new Database();
        lostConnection.failStatement = true;
        lostConnection.failRollback = true;
        outcome(executeNewOrder(input, lostConnection, true), "0", "1");

        jTPCCConnection connection = new jTPCCConnection(commitFailure.connection(), jTPCCConfig.DB_MYSQL);
        try {
            connection.commit();
            throw new AssertionError("commit failure was suppressed");
        } catch (SQLException error) {
            if (error != commitFailure.failure) throw new AssertionError("JDBC cause was replaced");
        }
        System.out.println("Transaction measurement tests passed");
    }
}
