/*
 * LoadData - Load Sample Data directly into database tables or into
 * CSV files using multiple parallel workers.
 *
 * Copyright (C) 2016, Denis Lussier
 * Copyright (C) 2016, Jan Wieck
 *
 */

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LoadData {
    private static Properties ini = new Properties();
    private static String db;
    private static Properties dbProps;
    private static jTPCCRandom rnd;
    private static String fileLocation = null;
    private static String csvNullValue = null;

    private static int numWarehouses;
    private static int numWorkers;
    private static int nextJob = 0;
    private static Object nextJobLock = new Object();

    private static LoadDataWorker[] workers;
    private static Thread[] workerThreads;

    private static String[] argv;

    private static boolean writeCSV = false;
    private static BufferedWriter configCSV = null;
    private static BufferedWriter itemCSV = null;
    private static BufferedWriter warehouseCSV = null;
    private static BufferedWriter districtCSV = null;
    private static BufferedWriter stockCSV = null;
    private static BufferedWriter customerCSV = null;
    private static BufferedWriter historyCSV = null;
    private static BufferedWriter orderCSV = null;
    private static BufferedWriter orderLineCSV = null;
    private static BufferedWriter newOrderCSV = null;

    private static final Lock configCSVLock = new ReentrantLock();
    private static final Lock itemCSVLock = new ReentrantLock();
    private static final Lock warehouseCSVLock = new ReentrantLock();
    private static final Lock districtCSVLock = new ReentrantLock();
    private static final Lock stockCSVLock = new ReentrantLock();
    private static final Lock customerCSVLock = new ReentrantLock();
    private static final Lock historyCSVLock = new ReentrantLock();
    private static final Lock orderCSVLock = new ReentrantLock();
    private static final Lock orderLineCSVLock = new ReentrantLock();
    private static final Lock newOrderCSVLock = new ReentrantLock();
    private static final Lock jobLock = new ReentrantLock();

    public static void main(String[] args) {
        int i;

        System.out.println("Starting BenchmarkSQL LoadData");
        System.out.println("");

        /*
         * Load the Benchmark properties file.
         */
        try {
            ini.load(new FileInputStream(System.getProperty("prop")));
        } catch (IOException e) {
            System.err.println("ERROR: " + e.getMessage());
            System.exit(1);
        }
        argv = args;

        /*
         * Initialize the global Random generator that picks the
         * C values for the load.
         */
        rnd = new jTPCCRandom();

        /*
         * Load the JDBC driver and prepare the db and dbProps.
         */
        try {
            Class.forName(iniGetString("driver"));
        } catch (Exception e) {
            System.err.println("ERROR: cannot load JDBC driver - " + e.getMessage());
            System.exit(1);
        }
        db = iniGetString("conn");
        dbProps = new Properties();
        dbProps.setProperty("user", iniGetString("user"));
        dbProps.setProperty("password", iniGetString("password"));

        /*
         * Parse other vital information from the props file.
         */
        numWarehouses = iniGetInt("warehouses");
        numWorkers = iniGetInt("loadWorkers", 4);
        fileLocation = iniGetString("fileLocation");
        csvNullValue = iniGetString("csvNullValue", "NULL");

        /*
         * If CSV files are requested, open them all.
         */
        if (fileLocation != null) {
            writeCSV = true;

            try {
                configCSV = new BufferedWriter(new FileWriter(fileLocation + "bmsql_config.csv"));
                itemCSV = new BufferedWriter(new FileWriter(fileLocation + "bmsql_item.csv"));
                warehouseCSV = new BufferedWriter(new FileWriter(fileLocation + "bmsql_warehouse.csv"));
                districtCSV = new BufferedWriter(new FileWriter(fileLocation + "bmsql_district.csv"));
                stockCSV = new BufferedWriter(new FileWriter(fileLocation + "bmsql_stock.csv"));
                customerCSV = new BufferedWriter(new FileWriter(fileLocation + "bmsql_customer.csv"));
                historyCSV = new BufferedWriter(new FileWriter(fileLocation + "bmsql_history.csv"));
                orderCSV = new BufferedWriter(new FileWriter(fileLocation + "bmsql_oorder.csv"));
                orderLineCSV = new BufferedWriter(new FileWriter(fileLocation + "bmsql_order_line.csv"));
                newOrderCSV = new BufferedWriter(new FileWriter(fileLocation + "bmsql_new_order.csv"));
            } catch (IOException ie) {
                System.err.println(ie.getMessage());
                System.exit(3);
            }
        }

        System.out.println("");

        /*
         * Create the number of requested workers and start them.
         */
        workers = new LoadDataWorker[numWorkers];
        workerThreads = new Thread[numWorkers];
        for (i = 0; i < numWorkers; i++) {
            Connection dbConn;

            try {
                dbConn = DriverManager.getConnection(db, dbProps);
                dbConn.setAutoCommit(false);
                if (writeCSV)
                    workers[i] = new LoadDataWorker(i, csvNullValue, rnd.newRandom());
                else
                    workers[i] = new LoadDataWorker(i, dbConn, rnd.newRandom());
                workerThreads[i] = new Thread(workers[i]);
                workerThreads[i].start();
            } catch (SQLException se) {
                System.err.println("ERROR: " + se.getMessage());
                System.exit(3);
                return;
            }
        }

        for (i = 0; i < numWorkers; i++) {
            try {
                workerThreads[i].join();
            } catch (InterruptedException ie) {
                System.err.println("ERROR: worker " + i + " - " + ie.getMessage());
                System.exit(4);
            }
        }

        /*
         * Close the CSV files if we are writing them.
         */
        if (writeCSV) {
            try {
                configCSV.close();
                itemCSV.close();
                warehouseCSV.close();
                districtCSV.close();
                stockCSV.close();
                customerCSV.close();
                historyCSV.close();
                orderCSV.close();
                orderLineCSV.close();
                newOrderCSV.close();
            } catch (IOException ie) {
                System.err.println(ie.getMessage());
                System.exit(3);
            }
        }
    } // End of main()

    public static void configAppend(StringBuilder buf) throws IOException {
        try {
            configCSVLock.lock();
            configCSV.write(buf.toString());
        } finally {
            buf.setLength(0);
            configCSVLock.unlock();
        }
    }

    public static void itemAppend(StringBuilder buf) throws IOException {
        try {
            itemCSVLock.lock();
            itemCSV.write(buf.toString());
        } finally {
            buf.setLength(0);
            itemCSVLock.unlock();
        }
    }

    public static void warehouseAppend(StringBuilder buf) throws IOException {
        try {
            warehouseCSVLock.lock();
            warehouseCSV.write(buf.toString());
        } finally {
            buf.setLength(0);
            warehouseCSVLock.unlock();
        }
    }

    public static void districtAppend(StringBuilder buf) throws IOException {
        try {
            districtCSVLock.lock();
            districtCSV.write(buf.toString());
        } finally {
            buf.setLength(0);
            districtCSVLock.unlock();
        }
    }

    public static void stockAppend(StringBuilder buf) throws IOException {
        try {
            stockCSVLock.lock();
            stockCSV.write(buf.toString());
        } finally {
            buf.setLength(0);
            stockCSVLock.unlock();
        }
    }

    public static void customerAppend(StringBuilder buf) throws IOException {
        try {
            customerCSVLock.lock();
            customerCSV.write(buf.toString());
        } finally {
            buf.setLength(0);
            customerCSVLock.unlock();
        }
    }

    public static void historyAppend(StringBuilder buf) throws IOException {
        try {
            historyCSVLock.lock();
            historyCSV.write(buf.toString());
        } finally {
            buf.setLength(0);
            historyCSVLock.unlock();
        }
    }

    public static void orderAppend(StringBuilder buf) throws IOException {
        try {
            orderCSVLock.lock();
            orderCSV.write(buf.toString());
        } finally {
            buf.setLength(0);
            orderCSVLock.unlock();
        }
    }

    public static void orderLineAppend(StringBuilder buf) throws IOException {
        try {
            orderLineCSVLock.lock();
            orderLineCSV.write(buf.toString());
        } finally {
            buf.setLength(0);
            orderLineCSVLock.unlock();
        }
    }

    public static void newOrderAppend(StringBuilder buf) throws IOException {
        try {
            newOrderCSVLock.lock();
            newOrderCSV.write(buf.toString());
        } finally {
            buf.setLength(0);
            newOrderCSVLock.unlock();
        }
    }

    public static int getNextJob() {
        int job;
        try {
            jobLock.lock();
            if (nextJob > numWarehouses)
                job = -1;
            else
                job = nextJob++;
        } finally {
            jobLock.unlock();
        }
        return job;
    }

    public static int getNumWarehouses() {
        return numWarehouses;
    }

    private static String iniGetString(String name) {
        String strVal = null;

        for (int i = 0; i < argv.length - 1; i += 2) {
            if (name.equalsIgnoreCase(argv[i])) {
                strVal = argv[i + 1];
                break;
            }
        }

        if (strVal == null)
            strVal = ini.getProperty(name);

        if (strVal == null)
            System.out.println(name + " (not defined)");
        else if (name.equals("password"))
            System.out.println(name + "=***********");
        else
            System.out.println(name + "=" + strVal);
        return strVal;
    }

    private static String iniGetString(String name, String defVal) {
        String strVal = null;

        for (int i = 0; i < argv.length - 1; i += 2) {
            if (name.equalsIgnoreCase(argv[i])) {
                strVal = argv[i + 1];
                break;
            }
        }

        if (strVal == null)
            strVal = ini.getProperty(name);

        if (strVal == null) {
            System.out.println(name + " (not defined - using default '" +
                    defVal + "')");
            return defVal;
        } else if (name.equals("password"))
            System.out.println(name + "=***********");
        else
            System.out.println(name + "=" + strVal);
        return strVal;
    }

    private static int iniGetInt(String name) {
        String strVal = iniGetString(name);

        if (strVal == null)
            return 0;
        return Integer.parseInt(strVal);
    }

    private static int iniGetInt(String name, int defVal) {
        String strVal = iniGetString(name);

        if (strVal == null)
            return defVal;
        return Integer.parseInt(strVal);
    }
}
