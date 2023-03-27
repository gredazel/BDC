import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;
public class MapReduceTriangles {
        static final int p = 8191; // constant used to calculate hash function
        public static Long CountTriangles(ArrayList<Tuple2<Integer, Integer>> edgeSet) {
                if (edgeSet.size()<3) return 0L;
                HashMap<Integer, HashMap<Integer,Boolean>> adjacencyLists = new HashMap<>();
                for (int i = 0; i < edgeSet.size(); i++) {
                        Tuple2<Integer,Integer> edge = edgeSet.get(i);
                        int u = edge._1();
                        int v = edge._2();
                        HashMap<Integer,Boolean> uAdj = adjacencyLists.get(u);
                        HashMap<Integer,Boolean> vAdj = adjacencyLists.get(v);
                        if (uAdj == null) {uAdj = new HashMap<>();}
                        uAdj.put(v,true);
                        adjacencyLists.put(u,uAdj);
                        if (vAdj == null) {vAdj = new HashMap<>();}
                        vAdj.put(u,true);
                        adjacencyLists.put(v,vAdj);
                }
                Long numTriangles = 0L;
                for (int u : adjacencyLists.keySet()) {
                        HashMap<Integer,Boolean> uAdj = adjacencyLists.get(u);
                        for (int v : uAdj.keySet()) {
                                if (v>u) {
                                        HashMap<Integer,Boolean> vAdj = adjacencyLists.get(v);
                                        for (int w : vAdj.keySet()) {
                                                if (w>v && (uAdj.get(w)!=null)) numTriangles++;
                                        }
                                }
                        }
                }
                return numTriangles;
        }
        public static int MR_ApproxTCwithNodeColors(int c, JavaRDD<String> edges) {
                JavaPairRDD<Integer, String> subsets;
                subsets = edges.flatMapToPair((line) -> {
                        String splitted[] = line.split(",");
                        int hash_a = hashFunct(c, Integer.parseInt(splitted[0]));
                        int hash_b = hashFunct(c, Integer.parseInt(splitted[1]));
                        ArrayList<Tuple2<Integer, String>> edgesSets = new ArrayList<>(); //string ArrayList that represent the c sets of edges; each element is a set of edges
                        if(hash_a == hash_b){
                                edgesSets.add(new Tuple2<>(hash_a, line));
                        }
                        return edgesSets.iterator();
                }); // end of first point round 1; don't know how to solve this error
                for(Tuple2<Integer, String> line : subsets.collect()){
                        System.out.println("Key: " + line._1 + " Value: " + line._2);
                }


                return 0;
        }


        /**
         * Calculate the value of the hash function of a given vertex u
         * @param c integer parameter used to partition data
         * @param u value of the considered vertex
         * @return hash function's value of vertex u
         */
        private static int hashFunct(int c, Integer u){
                Random rand = new Random();
                int a = rand.nextInt(p);
                int b = rand.nextInt(p);
                return (((a*u)+b)%p)%c;
        }

        public static void main(String[] args){
                if (args.length != 1) {
                        throw new IllegalArgumentException("USAGE: num_partitions file_path");
                }

                SparkConf conf = new SparkConf(true).setAppName("WordCount");
                JavaSparkContext sc = new JavaSparkContext(conf);
                sc.setLogLevel("WARN");

                JavaRDD<String> docs = sc.textFile(args[0]);

                long numdocs = docs.count();
                System.out.println("Number of documents = " + numdocs);
                JavaPairRDD<String, Long> numberCounts;

                MR_ApproxTCwithNodeColors(4, docs);
        }
}
