import org.apache.hadoop.util.hash.Hash;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class G088HW3 {

    public static final int THRESHOLD = 10000000;
    static final int p = 8191; // constant used to calculate hash function


    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            throw new IllegalArgumentException("USAGE: D W left right K portExp");
        }
        int D, W, left, right, K, portExp;
        D = Integer.parseInt(args[0]);
        W = Integer.parseInt(args[1]);
        left = Integer.parseInt(args[2]);
        right = Integer.parseInt(args[3]);
        K = Integer.parseInt(args[4]);
        portExp = Integer.parseInt(args[5]);

        int[] a1 = new int[D];
        int[] b1 = new int[D];
        int[] a2 = new int[D];
        int[] b2 = new int[D];

        Random rand = new Random();

        for(int i = 0; i < D; i++){
            a1[i] = rand.nextInt(p - 1) + 1;
            b1[i] = rand.nextInt(p);
            a2[i] = rand.nextInt(p - 1) + 1;
            b2[i] = rand.nextInt(p);
        }

        System.out.println("D: " + D);
        System.out.println("W: " + W);
        System.out.println("left: " + left);
        System.out.println("right: " + right);
        System.out.println("K: " + K);
        System.out.println("portExp: " + portExp);

        SparkConf conf = new SparkConf(true)
                .setMaster("local[*]") // remove this line if running on the cluster
                .setAppName("DistinctExample");

        // Here, with the duration you can control how large to make your batches.
        // Beware that the data generator we are using is very fast, so the suggestion
        // is to use batches of less than a second, otherwise you might exhaust the
        // JVM memory.

        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.milliseconds(100));
        sc.sparkContext().setLogLevel("ERROR");

        // TECHNICAL DETAIL:
        // The streaming spark context and our code and the tasks that are spawned all
        // work concurrently. To ensure a clean shut down we use this semaphore.
        // The main thread will first acquire the only permit available and then try
        // to acquire another one right after spinning up the streaming computation.
        // The second tentative at acquiring the semaphore will make the main thread
        // wait on the call. Then, in the `foreachRDD` call, when the stopping condition
        // is met we release the semaphore, basically giving "green light" to the main
        // thread to shut down the computation.
        // We cannot call `sc.stop()` directly in `foreachRDD` because it might lead
        // to deadlocks.

        Semaphore stoppingSemaphore = new Semaphore(1);
        stoppingSemaphore.acquire();

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        System.out.println("Receiving data from port = " + portExp);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // DEFINING THE REQUIRED DATA STRUCTURES TO MAINTAIN THE STATE OF THE STREAM
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        long[] streamLength = new long[1]; // Stream length (an array to be passed by reference)
        streamLength[0]=0L;
        HashMap<Long, Long> histogram = new HashMap<>(); // Hash Table for the distinct elements

        long[][] count_sk = new long[D][W];

        // CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
        sc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevels.MEMORY_AND_DISK)
                // For each batch, to the following.
                // BEWARE: the `foreachRDD` method has "at least once semantics", meaning
                // that the same data might be processed multiple times in case of failure.
                .foreachRDD((batch, time) -> {
                    // this is working on the batch at time `time`.
                    long batchSize = batch.count();
                    streamLength[0] += batchSize;
                    // Extract the distinct items from the batch
                    Map<Long, Long> batchItems = batch
                            .mapToPair(s -> new Tuple2<>(Long.parseLong(s), 1L))
                            .reduceByKey((i1, i2) -> 1L)
                            .collectAsMap();
                    // Update the streaming state
                    for (Map.Entry<Long, Long> pair : batchItems.entrySet()) {
                        if (!histogram.containsKey(pair.getKey())) {
                            histogram.put(pair.getKey(), 1L);
                        }
                    }
                    // If we wanted, here we could run some additional code on the global histogram
                    if (batchSize>0) {
                        System.out.println("Batch size at time [" + time + "] is: " + batchSize);
                    }
                    if (streamLength[0] >= THRESHOLD) {
                        stoppingSemaphore.release();
                    }
                });

        // MANAGING STREAMING SPARK CONTEXT
        System.out.println("Starting streaming engine");
        sc.start();
        System.out.println("Waiting for shutdown condition");
        stoppingSemaphore.acquire();
        System.out.println("Stopping the streaming engine");
        // NOTE: You will see some data being processed even after the
        // shutdown command has been issued: This is because we are asking
        // to stop "gracefully", meaning that any outstanding work
        // will be done.
        sc.stop(false, true);
        System.out.println("Streaming engine stopped");

        // COMPUTE AND PRINT FINAL STATISTICS
        System.out.println("Number of items processed = " + streamLength[0]);
        System.out.println("Number of distinct items = " + histogram.size());
        long max = 0L;
        for (Long key : histogram.keySet()) {
            if (key > max) {max = key;}
        }
        System.out.println("Largest item = " + max);
    }

    /**
     * Calculate the value of the hash function of a given integer u
     * @param d number of possible output values
     * @param u value of the considered integer
     * @param a random integer in [1, p-1] fixed for every run
     * @param b random integer in [0, p-1] fixed for every run
     * @return hash function's value of integer u
     */
    private static int hashFunc(int d, int u, int a, int b){
        return (int) (((((long)a*(long)u)+b)%(long)p)%(long)d);
    }

    private static int hashFunc2(int u, int a, int b){
        int out = (int)(((((long)a*(long)u)+b)%(long)p)%2L);
        if (out == 0)
            return -1;
        else
            return 1;
    }

}