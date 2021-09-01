import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class jTPCCRunner {
    private static CountDownLatch latch;
    private static final Lock lock = new ReentrantLock();

    private static final AtomicReference<Double> tpmCSum = new AtomicReference<>(0D);
    private static final AtomicReference<Double> tpmTotalSum = new AtomicReference<>(0D);
    private static final AtomicReference<Date> sessionStart = new AtomicReference<>(new Date());
    private static final AtomicReference<Date> sessionEnd = new AtomicReference<>(new Date());
    private static final AtomicLong tranCountSum = new AtomicLong(0L);
    private static final Map<String, LongAdder> executeTimeMap = new ConcurrentHashMap<>();

    private static final AtomicReference<String> threadName = new AtomicReference<>("");
    private static final SimpleDateFormat logDateSDF = new SimpleDateFormat("HH:mm:ss,SSS");

    private static final String FINISH_FLAG = "==finished==";

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

        List<BlockingQueue<String>> queues = new ArrayList<>();

        System.out.printf("%s [main] INFO   jTPCC : Term-00,%n", logDateSDF.format(new Date()));
        System.out.printf("%s [main] INFO   jTPCC : Term-00, +-------------------------------------------------------------+%n",
                logDateSDF.format(new Date()));
        System.out.printf("%s [main] INFO   jTPCC : Term-00,      BenchmarkSQL v%s%n",
                logDateSDF.format(new Date()), jTPCCConfig.JTPCCVERSION);
        System.out.printf("%s [main] INFO   jTPCC : Term-00, +-------------------------------------------------------------+%n",
                logDateSDF.format(new Date()));
        System.out.printf("%s [main] INFO   jTPCC : Term-00,  (c) 2003, Raul Barbosa%n", logDateSDF.format(new Date()));
        System.out.printf("%s [main] INFO   jTPCC : Term-00,  (c) 2004-2016, Denis Lussier%n", logDateSDF.format(new Date()));
        System.out.printf("%s [main] INFO   jTPCC : Term-00,  (c) 2016, Jan Wieck%n", logDateSDF.format(new Date()));
        System.out.printf("%s [main] INFO   jTPCC : Term-00, +-------------------------------------------------------------+%n",
                logDateSDF.format(new Date()));
        System.out.printf("%s [main] INFO   jTPCC : Term-00,%n", logDateSDF.format(new Date()));

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
            queues.add(new LinkedBlockingQueue<>());
        }

        boolean finalDebug = debug;
        for (int i = 0; i < processor; i++) {
            int finalI = i;

            new Thread(() -> {
                Process process = processList.get(finalI);
                BlockingQueue<String> queue = queues.get(finalI);
                Scanner scanner = new Scanner(process.getInputStream());
                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    queue.offer(line);
                }
                queue.offer(FINISH_FLAG);
                if (finalDebug) {
                    if (!process.isAlive()) {
                        System.out.println("[Process " + finalI + "] exit value: " + process.exitValue());
                    }
                }
            }).start();
        }

        for (int i = 0; i < processor; i++) {
            int finalI = i;
            new Thread(() -> {
                try {
                    for (; ; ) {
                        String line = queues.get(finalI).take();
                        if (line.trim().equals(FINISH_FLAG)) {
//                            System.out.println(FINISH_FLAG);
                            break;
                        }
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
                                if (line.contains("Term-00,\t")) {
                                    // 17:06:48,261 [Thread-297] INFO   jTPCC : Term-00,
                                    String lineRight = line.substring(line.indexOf("[") + 1);
                                    threadName.set(lineRight.substring(0, lineRight.indexOf("]")));
                                }
                                System.out.println(line);
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    System.out.printf("%s [main] INFO   jTPCC : %s%n", logDateSDF.format(new Date()), e.getMessage());
                } finally {
                    latch.countDown();
                }
            }).start();
        }
        latch.await();

        printSummary();
    }

    private static void printSummary() {
        if (tpmCSum.get() <= 0 || tpmTotalSum.get() <= 0 || "".equals(threadName.get())) {
            System.out.println("Interrupt exit...");
            return;
        }
        System.out.printf("%s [%s] INFO   jTPCC : Term-00, Measured tpmC (NewOrders) = %s%n",
                logDateSDF.format(new Date()), threadName.get(), tpmCSum.get());
        System.out.printf("%s [%s] INFO   jTPCC : Term-00, Measured tpmTOTAL = %s%n",
                logDateSDF.format(new Date()), threadName.get(), tpmTotalSum.get());

        SimpleDateFormat sessionDateSDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.printf("%s [%s] INFO   jTPCC : Term-00, Session Start     = %s%n",
                logDateSDF.format(new Date()), threadName.get(), sessionDateSDF.format(sessionStart.get()));
        System.out.printf("%s [%s] INFO   jTPCC : Term-00, Session End       = %s%n",
                logDateSDF.format(new Date()), threadName.get(), sessionDateSDF.format(sessionEnd.get()));
        System.out.printf("%s [%s] INFO   jTPCC : Term-00, Transaction Count = %s%n",
                logDateSDF.format(new Date()), threadName.get(), (tranCountSum.longValue() - 1));

        for (String key : executeTimeMap.keySet()) {
            long value = executeTimeMap.get(key).longValue();
            System.out.printf("%s [%s] INFO   jTPCC : %s=%s%n",
                    logDateSDF.format(new Date()), threadName.get(), key, value);
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
