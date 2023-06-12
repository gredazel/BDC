
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;

import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.Semaphore;


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

        System.out.println("D = " + D + " W = " + W + " [left,right] = [" + left + "," + right + "] K = " + K + " Port = " + portExp );

        SparkConf conf = new SparkConf(true)
                .setMaster("local[*]").setAppName("G088HW3");

        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.milliseconds(100));
        sc.sparkContext().setLogLevel("ERROR");

        Semaphore stoppingSemaphore = new Semaphore(1);
        stoppingSemaphore.acquire();

        long[] streamLength = new long[1]; // Stream length (an array to be passed by reference)
        long[] LRlenght = new long[1]; // Stream length (an array to be passed by reference)
        streamLength[0]=0L;
        LRlenght[0] = 0L;
        //maps holding exact frequencies
        HashMap<Long, Long> histogram = new HashMap<>(); // Hash Table for the distinct elements
        HashMap<Long, Long> LRHistogram = new HashMap<>();

        long[][] count_sk = new long[D][W];
        for(int x = 0; x < D; x++)
            for(int y = 0; y < W; y++)
                count_sk[x][y] = 0L;

        sc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevels.MEMORY_AND_DISK)
                .foreachRDD((batch, time) -> {
                    if (streamLength[0] < THRESHOLD) {
                        long batchSize = batch.count();
                        streamLength[0] += batchSize;
                        Map<Long, Long> batchItems = batch
                                .mapToPair(s -> new Tuple2<>(Long.parseLong(s), 1L))
                                .reduceByKey((i1, i2) -> i1 + i2)
                                .collectAsMap();

                        long lrvals = 0L;

                        for (Map.Entry<Long, Long> pair : batchItems.entrySet()) {
                            if(pair.getKey() <= right && pair.getKey() >= left){
                                lrvals += pair.getValue();
                                if (!LRHistogram.containsKey(pair.getKey())) {
                                    LRHistogram.put(pair.getKey(), pair.getValue());
                                }else {
                                    LRHistogram.replace(pair.getKey(), pair.getValue() + LRHistogram.get(pair.getKey()).longValue());
                                }

                                //WD count sketch
                                for (int i = 0; i < D; i++) {
                                    count_sk[i][hashFunc(W, pair.getKey(), a1[i], b1[i])] += (hashFunc2(pair.getKey(), a2[i], b2[i]) * pair.getValue());
                                }

                            }
                            if (!histogram.containsKey(pair.getKey())) {
                                histogram.put(pair.getKey(), pair.getValue());
                            }else {
                                histogram.replace(pair.getKey(), pair.getValue() + histogram.get(pair.getKey()).longValue());
                            }
                        }

                        LRlenght[0] += lrvals;

                        if (streamLength[0] >= THRESHOLD) {
                            stoppingSemaphore.release();
                            //System.out.println("released");
                        }
                    }
                });

        // MANAGING STREAMING SPARK CONTEXT
        //System.out.println("Starting streaming engine");
        sc.start();
        //System.out.println("Waiting for shutdown condition");
        stoppingSemaphore.acquire();
        //System.out.println("Stopping the streaming engine");
        sc.stop(false, false);
        //System.out.println("Streaming engine stopped");

        double trueF2 = 0.0;

        System.out.println("Total number of items = " + streamLength[0]);
        System.out.println("Total number of items in [" + left + "," + right + "] = " + LRlenght[0]);
        System.out.println("Number of distinct items in [" + left + "," + right + "] = " + LRHistogram.size());

        // CALCULATE TRUE SECOND MOMENT
        for(Map.Entry<Long, Long> pair : LRHistogram.entrySet()){
            trueF2 += (double)(pair.getValue() * pair.getValue());
        }
        trueF2 /= (double) (LRlenght[0] * LRlenght[0]);

        System.out.println("True F2 = " + trueF2);

        //CALCULATE APRX SECOND MOMENT
        ArrayList<Double> approximations = new ArrayList<>();
        for(int j = 0; j < D; j++){
            double val = 0.0;
            for (int x = 0; x < W; x++){
                val += count_sk[j][x]*count_sk[j][x];
            }
            approximations.add(val);
        }
        Collections.sort(approximations);
        double aprxF2 = approximations.get(approximations.size()/2);
        aprxF2 /= (double)(LRlenght[0] * LRlenght[0]);

        System.out.println("Approx F2 = " + aprxF2);

        // AVERAGE RELATIVE ERROR
        List<Map.Entry<Long, Long>> entries = new ArrayList<>(LRHistogram.entrySet());
        entries.sort(Map.Entry.comparingByValue(Comparator.reverseOrder()));
        double sumRelativeErrors = 0.0;
        for(int i = 0; i < K; i++){
            ArrayList<Long> frequency_approximations = new ArrayList<>();
            for (int d = 0; d < D; d++){
                long val = hashFunc2(entries.get(i).getKey(), a2[d], b2[d]) * count_sk[d][hashFunc(W, entries.get(i).getKey(), a1[d], b1[d])];
                frequency_approximations.add(val);
            }
            long aprxFrequency = frequency_approximations.get(frequency_approximations.size()/2);
            sumRelativeErrors += ((double)(Math.abs(entries.get(i).getValue() - aprxFrequency)))/(double)(entries.get(i).getValue());
            if(K <= 20)
                System.out.println("Item " + entries.get(i).getKey() + " Freq = " + entries.get(i).getValue() + " Est. Freq = "+ aprxFrequency);
        }
        double averageRelativeError = sumRelativeErrors / K;

        System.out.println("Avg err for top " + K + " = " + averageRelativeError);
        System.out.println("F2 " + trueF2 + " F2 Estimate " + aprxF2);
    }

    /**
     * Calculate the value of the hash function of a given integer u
     * @param d number of possible output values
     * @param u value of the considered integer
     * @param a random integer in [1, p-1] fixed for every run
     * @param b random integer in [0, p-1] fixed for every run
     * @return hash function's value of integer u
     */
    private static int hashFunc(long d, long u, int a, int b){
        return (int) (((((long)a*u)+(long)b)%(long)p)%d);
    }

    private static int hashFunc2(long u, int a, int b){
        int out = (int)(((((long)a*u)+(long)b)%(long)p)%2);
        if (out == 0)
            return -1;
        else
            return 1;
    }

}