import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class jTPCCRunner {
    private static Logger log = Logger.getLogger(jTPCC.class);

    private static CountDownLatch latch;
    private static final Lock lock = new ReentrantLock();

    private static final AtomicReference<Double> tpmCSum = new AtomicReference<>(0D);
    private static final AtomicReference<Double> tpmTotalSum = new AtomicReference<>(0D);
    private static final AtomicReference<Date> sessionStart = new AtomicReference<>(new Date());
    private static final AtomicReference<Date> sessionEnd = new AtomicReference<>(new Date());
    private static final AtomicLong tranCountSum = new AtomicLong(0L);
    private static final Map<String, LongAdder> executeTimeMap = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        String prop = System.getProperty("prop");
        String runIDStr = System.getProperty("runID");
        String processorStr = System.getProperty("processor");
        String debugStr = System.getProperty("debug");

        int processor = Integer.parseInt(processorStr);
        if (processor <= 0) {
            throw new Exception("processor can only be positive");
        }
        latch = new CountDownLatch(processor);

        boolean debug = false;
        if (null != debugStr && debugStr.equals("1")) {
            debug = true;
        }

        int runID = Integer.parseInt(runIDStr);
        List<Process> processList = new ArrayList<>();

        jTPCCUtil.printTitle();
        for (int i = 0; i < processor; i++) {
            runID++;
//            System.out.println("&&&&&&& runID: " + runID);
            ProcessBuilder processBuilder = new ProcessBuilder();
            processBuilder.redirectErrorStream(true);
            processBuilder.command("java", "-cp", ".:../lib/mysql/*:../lib/*:../dist/*",
                    "-Dprop=" + prop, "-DrunID=" + runID, "-Dprocessor=" + processor, "-Dbatch=1", "jTPCC");
            processBuilder.redirectErrorStream(true);

            Process process = processBuilder.start();
            processList.add(process);
        }

        for (int i = 0; i < processor; i++) {
            int finalI = i;
            boolean finalDebug = debug;
            new Thread(() -> {
                Scanner scanner = new Scanner(processList.get(finalI).getInputStream());
                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    if (line.trim().isEmpty()) {
                        continue;
                    }
                    if (finalDebug) {
                        System.out.println("[Process " + finalI + "] " + line);
                    }
                    if (summary(line) && !finalDebug) continue;
                    // Only the first process data is output
                    if (finalDebug) {
                        System.out.println("[Process " + finalI + "] " + line);
                    } else {
                        if (finalI == 0) {
                            System.out.println(line);
                        }
                    }
                }
                latch.countDown();
            }).start();
        }
        latch.await();

        printSummary();
    }

    private static void printSummary() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        log.info("Term-00, ");
        log.info("Term-00, ");
        log.info("Term-00, Measured tpmC (NewOrders) = " + tpmCSum.get());
        log.info("Term-00, Measured tpmTOTAL = " + tpmTotalSum.get());
        log.info("Term-00, Session Start     = " + sdf.format(sessionStart.get()));
        log.info("Term-00, Session End       = " + sdf.format(sessionEnd.get()));
        log.info("Term-00, Transaction Count = " + (tranCountSum.longValue() - 1));
        for (String key : executeTimeMap.keySet()) {
            long value = executeTimeMap.get(key).longValue();
            log.info(key + "=" + value);
        }
    }

    private static boolean summary(String line) {
        if (line.contains("Measured tpmC (NewOrders)")) {
            // Term-00, Measured tpmC (NewOrders) = 12185.26
            try {
                lock.lock();
                String[] lines = line.split("=");
                if (lines.length == 2) {
                    tpmCSum.set(tpmCSum.get() + Double.parseDouble(lines[1].trim()));
                }
            } finally {
                lock.unlock();
            }
            return true;
        }
        if (line.contains("Measured tpmTOTAL")) {
            // Term-00, Measured tpmTOTAL = 27073.62
            try {
                lock.lock();
                String[] lines = line.split("=");
                if (lines.length == 2) {
                    tpmTotalSum.set(tpmTotalSum.get() + Double.parseDouble(lines[1].trim()));
                }
            } finally {
                lock.unlock();
            }
            return true;
        }

        SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        if (line.contains("Session Start")) {
            // Term-00, Session Start     = 2021-08-24 21:54:42
            try {
                lock.lock();
                String[] lines = line.split("=");
                if (lines.length == 2) {
                    Date currentDate = SDF.parse(lines[1].trim());
                    if (sessionStart.get().after(currentDate)) {
                        sessionStart.set(currentDate);
                    }
                }
            } catch (ParseException e) {
                System.out.println(e.getMessage());
                return false;
            } finally {
                lock.unlock();
            }
            return true;
        }
        if (line.contains("Session End")) {
            // Term-00, Session End       = 2021-08-24 22:24:43
            try {
                lock.lock();
                String[] lines = line.split("=");
                if (lines.length == 2) {
                    Date currentDate = SDF.parse(lines[1].trim());

                    if (sessionEnd.get().before(currentDate)) {
                        sessionEnd.set(currentDate);
                    }
                }
            } catch (ParseException e) {
                System.out.println(e.getMessage());
                return false;
            } finally {
                lock.unlock();
            }
            return true;
        }
        if (line.contains("Transaction Count")) {
            // Term-00, Transaction Count = 812757
            String[] lines = line.split("=");
            if (lines.length == 2) {
                tranCountSum.addAndGet(Long.parseLong(lines[1].trim()));
            }
            return true;
        }
        if (line.contains("executeTime")) {
            // executeTime[Payment]=84310282
            // executeTime[Order-Status]=266607
            // executeTime[Delivery]=22537610
            // executeTime[Stock-Level]=929640
            // executeTime[New-Order]=7197477
            try {
                lock.lock();
                String[] lines = line.split(" : ");
                if (lines.length == 2) {
                    String execTimeStr = lines[1].trim();
                    String[] execTimeEntryStr = execTimeStr.split("=");
                    if (execTimeEntryStr.length == 2) {
                        LongAdder execTimeVal = executeTimeMap.computeIfAbsent(execTimeEntryStr[0].trim(),
                                (x) -> new LongAdder());
                        execTimeVal.add(Long.parseLong(execTimeEntryStr[1].trim()));
                    }
                }
            } finally {
                lock.unlock();
            }
            return true;
        }
        return false;
    }
}
