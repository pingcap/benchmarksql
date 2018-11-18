/*
 * jTPCC - Open Source Java implementation of a TPC-C like benchmark
 *
 * Copyright (C) 2003, Raul Barbosa
 * Copyright (C) 2004-2016, Denis Lussier
 * Copyright (C) 2016, Jan Wieck
 *
 */

import org.apache.log4j.*;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.text.*;


public class jTPCC implements jTPCCConfig
{
    private static org.apache.log4j.Logger log = Logger.getLogger(jTPCC.class);

    private int currentlyDisplayedTerminal;

    private jTPCCTerminal[] terminals;
    private String[] terminalNames;
    private boolean terminalsBlockingExit = false;
    private Random random;
    private long terminalsStarted = 0, sessionCount = 0, transactionCount;

    private long newOrderCounter, sessionStartTimestamp, sessionEndTimestamp, sessionNextTimestamp=0, sessionNextKounter=0;
    private long sessionEndTargetTime = -1, fastNewOrderCounter, recentTpmC=0, recentTpmTotal=0;
    private boolean signalTerminalsRequestEndSent = false, databaseDriverLoaded = false;

    private FileOutputStream fileOutputStream;
    private PrintStream printStreamReport;
    private String sessionStart, sessionEnd;
    private int limPerMin_Terminal;

    private double tpmC;

    public static void main(String args[])
    {
        PropertyConfigurator.configure("log4j.xml");
        new jTPCC();
    }

    private String getProp (Properties p, String pName)
    {
        String prop =  p.getProperty(pName);
        log.info("Term-00, " + pName + "=" + prop);
        return(prop);
    }

    public jTPCC()
    {

        // load the ini file
        Properties ini = new Properties();
        try {
          ini.load( new FileInputStream(System.getProperty("prop")));
        } catch (IOException e) {
          errorMessage("Term-00, could not load properties file");
        }


        log.info("Term-00, ");
        log.info("Term-00, +-------------------------------------------------------------+");
        log.info("Term-00,      BenchmarkSQL v" + JTPCCVERSION);
        log.info("Term-00, +-------------------------------------------------------------+");
        log.info("Term-00,  (c) 2003, Raul Barbosa");
        log.info("Term-00,  (c) 2004-2016, Denis Lussier");
        log.info("Term-00,  (c) 2016, Jan Wieck");
        log.info("Term-00, +-------------------------------------------------------------+");
        log.info("Term-00, ");
        String  iDriver             = getProp(ini,"driver");
        String  iConn               = getProp(ini,"conn");
        String  iUser               = getProp(ini,"user");
        String  iPassword           = ini.getProperty("password");


        log.info("Term-00, ");
        String  iWarehouses         = getProp(ini,"warehouses");
        String  iTerminals          = getProp(ini,"terminals");

        String  iRunTxnsPerTerminal =  ini.getProperty("runTxnsPerTerminal");
        String iRunMins  =  ini.getProperty("runMins");
        if (Integer.parseInt(iRunTxnsPerTerminal) ==0 && Integer.parseInt(iRunMins)!=0){
            log.info("Term-00, runMins" + "=" + iRunMins);
        }else if(Integer.parseInt(iRunTxnsPerTerminal) !=0 && Integer.parseInt(iRunMins)==0){
            log.info("Term-00, runTxnsPerTerminal" + "=" + iRunTxnsPerTerminal);
        }else{
            errorMessage("Term-00, Must indicate either transactions per terminal or number of run minutes!");
        };
        String  limPerMin           = getProp(ini,"limitTxnsPerMin");
        log.info("Term-00, ");
        String  iNewOrderWeight     = getProp(ini,"newOrderWeight");
        String  iPaymentWeight      = getProp(ini,"paymentWeight");
        String  iOrderStatusWeight  = getProp(ini,"orderStatusWeight");
        String  iDeliveryWeight     = getProp(ini,"deliveryWeight");
        String  iStockLevelWeight   = getProp(ini,"stockLevelWeight");

        log.info("Term-00, ");

        if(Integer.parseInt(limPerMin) !=0){
            limPerMin_Terminal = Integer.parseInt(limPerMin)/Integer.parseInt(iTerminals);
        }
        else{
            limPerMin_Terminal = -1;
        }


        boolean iRunMinsBool=false;


        this.random = new Random(System.currentTimeMillis());

        fastNewOrderCounter = 0;
        updateStatusLine();

        try
        {
            String driver = iDriver;
            printMessage("Loading database driver: \'" + driver + "\'...");
            Class.forName(iDriver);
            databaseDriverLoaded = true;
        }
        catch(Exception ex)
        {
            errorMessage("Unable to load the database driver!");
            databaseDriverLoaded = false;
        }


        if(databaseDriverLoaded)
        {
            try
            {
                boolean limitIsTime = iRunMinsBool;
                int numTerminals = -1, transactionsPerTerminal = -1, numWarehouses = -1;
                int newOrderWeightValue = -1, paymentWeightValue = -1, orderStatusWeightValue = -1, deliveryWeightValue = -1, stockLevelWeightValue = -1;
                long executionTimeMillis = -1;

                try
                {
                    if (Integer.parseInt(iRunMins) != 0 && Integer.parseInt(iRunTxnsPerTerminal) ==0)
                    {
                        iRunMinsBool = true;
                    }
                    else if (Integer.parseInt(iRunMins) == 0 && Integer.parseInt(iRunTxnsPerTerminal) !=0)
                    {
                        iRunMinsBool = false;
                    }
                    else
                    {
                        throw new NumberFormatException();
                    }
                }
                catch(NumberFormatException e1)
                {
                    errorMessage("Must indicate either transactions per terminal or number of run minutes!");
                    throw new Exception();
                }

                try
                {
                    numWarehouses = Integer.parseInt(iWarehouses);
                    if(numWarehouses <= 0)
                        throw new NumberFormatException();
                }
                catch(NumberFormatException e1)
                {
                    errorMessage("Invalid number of warehouses!");
                    throw new Exception();
                }

                try
                {
                    numTerminals = Integer.parseInt(iTerminals);
                    if(numTerminals <= 0 || numTerminals > 10*numWarehouses)
                        throw new NumberFormatException();
                }
                catch(NumberFormatException e1)
                {
                    errorMessage("Invalid number of terminals!");
                    throw new Exception();
                }



                if(Long.parseLong(iRunMins) != 0 && Integer.parseInt(iRunTxnsPerTerminal) == 0)
                {
                    try
                    {
                        executionTimeMillis = Long.parseLong(iRunMins) * 60000;
                        if(executionTimeMillis <= 0)
                            throw new NumberFormatException();
                    }
                    catch(NumberFormatException e1)
                    {
                        errorMessage("Invalid number of minutes!");
                        throw new Exception();
                    }
                }
                else
                {
                    try
                    {
                        transactionsPerTerminal = Integer.parseInt(iRunTxnsPerTerminal);
                        if(transactionsPerTerminal <= 0)
                            throw new NumberFormatException();
                    }
                    catch(NumberFormatException e1)
                    {
                        errorMessage("Invalid number of transactions per terminal!");
                        throw new Exception();
                    }
                }

                try
                {
                    newOrderWeightValue = Integer.parseInt(iNewOrderWeight);
                    paymentWeightValue = Integer.parseInt(iPaymentWeight);
                    orderStatusWeightValue = Integer.parseInt(iOrderStatusWeight);
                    deliveryWeightValue = Integer.parseInt(iDeliveryWeight);
                    stockLevelWeightValue = Integer.parseInt(iStockLevelWeight);

                    if(newOrderWeightValue < 0 ||paymentWeightValue < 0 || orderStatusWeightValue < 0 || deliveryWeightValue < 0 || stockLevelWeightValue < 0)
                        throw new NumberFormatException();
                    else if(newOrderWeightValue == 0 && paymentWeightValue == 0 && orderStatusWeightValue == 0 && deliveryWeightValue == 0 && stockLevelWeightValue == 0)
                        throw new NumberFormatException();
                }
                catch(NumberFormatException e1)
                {
                    errorMessage("Invalid number in mix percentage!");
                    throw new Exception();
                }

                if(newOrderWeightValue + paymentWeightValue + orderStatusWeightValue + deliveryWeightValue + stockLevelWeightValue > 100)
                {
                    errorMessage("Sum of mix percentage parameters exceeds 100%!");
                    throw new Exception();
                }

                newOrderCounter = 0;
                printMessage("Session started!");
                if(!limitIsTime)
                    printMessage("Creating " + numTerminals + " terminal(s) with " + transactionsPerTerminal + " transaction(s) per terminal...");
                else
                    printMessage("Creating " + numTerminals + " terminal(s) with " + (executionTimeMillis/60000) + " minute(s) of execution...");
                printMessage("Transaction Weights: " + newOrderWeightValue + "% New-Order, " + paymentWeightValue + "% Payment, " + orderStatusWeightValue + "% Order-Status, " + deliveryWeightValue + "% Delivery, " + stockLevelWeightValue + "% Stock-Level");

                printMessage("Number of Terminals\t" + numTerminals);

                terminals = new jTPCCTerminal[numTerminals];
                terminalNames = new String[numTerminals];
                terminalsStarted = numTerminals;
                try
                {
                    String database = iConn;
                    String username = iUser;
                    String password = iPassword;

                    int[][] usedTerminals = new int[numWarehouses][10];
                    for(int i = 0; i < numWarehouses; i++)
                        for(int j = 0; j < 10; j++)
                            usedTerminals[i][j] = 0;

                    for(int i = 0; i < numTerminals; i++)
                    {
                        int terminalWarehouseID;
                        int terminalDistrictID;
                        do
                        {
                            terminalWarehouseID = (int)randomNumber(1, numWarehouses);
                            terminalDistrictID = (int)randomNumber(1, 10);
                        }
                        while(usedTerminals[terminalWarehouseID-1][terminalDistrictID-1] == 1);
                        usedTerminals[terminalWarehouseID-1][terminalDistrictID-1] = 1;

                        String terminalName = "Term-" + (i>=9 ? ""+(i+1) : "0"+(i+1));
                        Connection conn = null;
                        printMessage("Creating database connection for " + terminalName + "...");
                        conn = DriverManager.getConnection(database, username, password);
                        conn.setAutoCommit(false);

                        jTPCCTerminal terminal = new jTPCCTerminal
                        (terminalName, terminalWarehouseID, terminalDistrictID, conn,
                         transactionsPerTerminal, paymentWeightValue, orderStatusWeightValue,
                         deliveryWeightValue, stockLevelWeightValue, numWarehouses, limPerMin_Terminal, this);

                        terminals[i] = terminal;
                        terminalNames[i] = terminalName;
                        printMessage(terminalName + "\t" + terminalWarehouseID);
                    }

                    sessionEndTargetTime = executionTimeMillis;
                    signalTerminalsRequestEndSent = false;


                    printMessage("Transaction\tWeight");
                    printMessage("% New-Order\t" + newOrderWeightValue);
                    printMessage("% Payment\t" + paymentWeightValue);
                    printMessage("% Order-Status\t" + orderStatusWeightValue);
                    printMessage("% Delivery\t" + deliveryWeightValue);
                    printMessage("% Stock-Level\t" + stockLevelWeightValue);

                    printMessage("Transaction Number\tTerminal\tType\tExecution Time (ms)\t\tComment");

                    printMessage("Created " + numTerminals + " terminal(s) successfully!");
                    boolean dummvar = true;



                    //^Create Terminals, Start Transactions v //

                    if(dummvar==true){
                    sessionStart = getCurrentTime();
                    sessionStartTimestamp = System.currentTimeMillis();
                    sessionNextTimestamp = sessionStartTimestamp;
                    if(sessionEndTargetTime != -1)
                        sessionEndTargetTime += sessionStartTimestamp;

                    synchronized(terminals)
                    {
                        printMessage("Starting all terminals...");
                        transactionCount = 1;
                        for(int i = 0; i < terminals.length; i++)
                            (new Thread(terminals[i])).start();

                    }

                    printMessage("All terminals started executing " + sessionStart);
                    }
                }

                catch(Exception e1)
                {
                    errorMessage("This session ended with errors!");
                    printStreamReport.close();
                    fileOutputStream.close();

                    throw new Exception();
                }

            }
            catch(Exception ex)
            {
            }
        }
                           updateStatusLine();
    }

    private void signalTerminalsRequestEnd(boolean timeTriggered)
    {
        synchronized(terminals)
        {
            if(!signalTerminalsRequestEndSent)
            {
                if(timeTriggered)
                    printMessage("The time limit has been reached.");
                printMessage("Signalling all terminals to stop...");
                signalTerminalsRequestEndSent = true;

                for(int i = 0; i < terminals.length; i++)
                    if(terminals[i] != null)
                        terminals[i].stopRunningWhenPossible();

                printMessage("Waiting for all active transactions to end...");
            }
        }
    }

    public void signalTerminalEnded(jTPCCTerminal terminal, long countNewOrdersExecuted)
    {
        synchronized(terminals)
        {
            boolean found = false;
            terminalsStarted--;
            for(int i = 0; i < terminals.length && !found; i++)
            {
                if(terminals[i] == terminal)
                {
                    terminals[i] = null;
                    terminalNames[i] = "(" + terminalNames[i] + ")";
                    newOrderCounter += countNewOrdersExecuted;
                    found = true;
                }
            }
        }

        if(terminalsStarted == 0)
        {
            sessionEnd = getCurrentTime();
            sessionEndTimestamp = System.currentTimeMillis();
            sessionEndTargetTime = -1;
            printMessage("All terminals finished executing " + sessionEnd);
            endReport();
            terminalsBlockingExit = false;
            printMessage("Session finished!");
        }
    }

    public void signalTerminalEndedTransaction(String terminalName, String transactionType, long executionTime, String comment, int newOrder)
    {
        transactionCount++;
        fastNewOrderCounter += newOrder;

        if(sessionEndTargetTime != -1 && System.currentTimeMillis() > sessionEndTargetTime)
        {
            signalTerminalsRequestEnd(true);
        }

       updateStatusLine();

    }

    private void endReport()
    {
        long currTimeMillis = System.currentTimeMillis();
        long freeMem = Runtime.getRuntime().freeMemory() / (1024*1024);
        long totalMem = Runtime.getRuntime().totalMemory() / (1024*1024);
        double tpmC = (6000000*fastNewOrderCounter/(currTimeMillis - sessionStartTimestamp))/100.0;
        double tpmTotal = (6000000*transactionCount/(currTimeMillis - sessionStartTimestamp))/100.0;


	System.out.println("");
        log.info("Term-00, ");
        log.info("Term-00, ");
        log.info("Term-00, Measured tpmC (NewOrders) = " + tpmC);
        log.info("Term-00, Measured tpmTOTAL = " + tpmTotal);
        log.info("Term-00, Session Start     = " + sessionStart );
        log.info("Term-00, Session End       = " + sessionEnd);
        log.info("Term-00, Transaction Count = " + (transactionCount-1));

    }

    private void printMessage(String message)
    {
        log.trace("Term-00, " + message);
    }

    private void errorMessage(String message)
    {
        log.error("Term-00, "+ message);
    }

    private void exit()
    {
        System.exit(0);
    }

    private long randomNumber(long min, long max)
    {
        return (long)(random.nextDouble() * (max-min+1) + min);
    }

    private String getCurrentTime()
    {
        return dateFormat.format(new java.util.Date());
    }

    private String getFileNameSuffix()
    {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        return dateFormat.format(new java.util.Date());
    }

    synchronized private void updateStatusLine()
    {
        long currTimeMillis = System.currentTimeMillis();

        if(currTimeMillis > sessionNextTimestamp)
        {
	    StringBuilder informativeText = new StringBuilder("");
	    Formatter fmt = new Formatter(informativeText);
            double tpmC = (6000000*fastNewOrderCounter/(currTimeMillis - sessionStartTimestamp))/100.0;
            double tpmTotal = (6000000*transactionCount/(currTimeMillis - sessionStartTimestamp))/100.0;

            sessionNextTimestamp += 1000;  /* update this every seconds */

	    fmt.format("Term-00, Running Average tpmTOTAL: %.2f", tpmTotal);

	    /* XXX What is the meaning of these numbers? */
            recentTpmC = (fastNewOrderCounter - sessionNextKounter) * 12;
            recentTpmTotal= (transactionCount-sessionNextKounter)*12;
            sessionNextKounter = fastNewOrderCounter;
	    fmt.format("    Current tpmTOTAL: %d", recentTpmTotal);

	    long freeMem = Runtime.getRuntime().freeMemory() / (1024*1024);
	    long totalMem = Runtime.getRuntime().totalMemory() / (1024*1024);
	    fmt.format("    Memory Usage: %dMB / %dMB          ", (totalMem - freeMem), totalMem);

            System.out.print(informativeText);
            for (int count = 0; count < 1+informativeText.length(); count++)
                System.out.print("\b");
        }
    }
}
