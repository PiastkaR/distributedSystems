package com.example.distributedsystems.healer;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.File;
import java.io.IOException;

public class Autohealer implements Watcher {

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;

    // Parent Znode where each worker stores an ephemeral child to indicate it is alive
    private static final String AUTOHEALER_ZNODES_PATH = "/workers";

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        if (args.length != 2) {
            System.out.println("Expecting parameters <number of workers> <path to worker jar file>");
            System.exit(1);
        }

        int numberOfWorkers = Integer.parseInt(args[0]);
        String pathToWorkerProgram = args[1];
        Autohealer autohealer = new Autohealer(numberOfWorkers, pathToWorkerProgram);
        autohealer.connectToZookeeper();
        autohealer.startWatchingWorkers();
        autohealer.run();
        autohealer.close();
    }

    // Path to the worker jar
    private final String pathToProgram;

    // The number of worker instances we need to maintain at all times
    private final int numberOfWorkers;
    private ZooKeeper zooKeeper;

    public Autohealer(int numberOfWorkers, String pathToProgram) {
        this.numberOfWorkers = numberOfWorkers;
        this.pathToProgram = pathToProgram;
    }

    public void startWatchingWorkers() throws KeeperException, InterruptedException, IOException {
        if (zooKeeper.exists(AUTOHEALER_ZNODES_PATH, false) == null) {
            zooKeeper.create(AUTOHEALER_ZNODES_PATH, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        launchWorkersIfNecessary();
    }

    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully connected to Zookeeper");
                } else {
                    synchronized (zooKeeper) {
                        System.out.println("Disconnected from Zookeeper event");
                        zooKeeper.notifyAll();
                    }
                }
                break;
            case NodeChildrenChanged:
                try {
                    launchWorkersIfNecessary();
                } catch (InterruptedException e) {


                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                /**
             * Add states code here to respond to the relevant events
             */
        }
    }

    private void launchWorkersIfNecessary() throws InterruptedException, KeeperException, IOException {
        /**
         * Implement this method to watch and launch new workers if necessary
         */
        int currentWorkersCount = zooKeeper.getChildren(AUTOHEALER_ZNODES_PATH, this).size();
        System.out.println("Current workers count: " + currentWorkersCount);
        if (currentWorkersCount < numberOfWorkers) {
            int workersToStart = numberOfWorkers - currentWorkersCount;
            System.out.println("Starting " + workersToStart + " new worker(s)");
            for (int i = 0; i < workersToStart; i++) {
                startNewWorker();
            }
        } else {
            System.out.println("No need to start new workers");
        }
    }

    /**
     * Helper method to start a single worker
     *
     * @throws IOException
     */
    private void startNewWorker() throws IOException {
        File flakyWorkerJarFile = new File(pathToProgram);
        ProcessBuilder processBuilder =
                new ProcessBuilder("java", "-jar", flakyWorkerJarFile.getCanonicalPath());

        File log = new File("log.txt");
        processBuilder.redirectErrorStream(true);
        processBuilder.redirectOutput(ProcessBuilder.Redirect.appendTo(log));
        System.out.println("Launching new worker instance");
        processBuilder.start();
    }
}