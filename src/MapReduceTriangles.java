import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Int;
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
                Random rand = new Random();
                int a = rand.nextInt(p-1) + 1;
                int b = rand.nextInt(p);

                JavaPairRDD<Integer, Tuple2<Integer, Integer>> subsets;
                subsets = edges.flatMapToPair((line) -> {
                        String splitted[] = line.split(",");
                        int hash_a = hashFunct(a, b, c, Integer.parseInt(splitted[0]));
                        int hash_b = hashFunct(a, b, c, Integer.parseInt(splitted[1]));
                        ArrayList<Tuple2<Integer, Tuple2<Integer, Integer>>> edgesSets = new ArrayList<>(); //string ArrayList that represent the c sets of edges; each element is a set of edges
                        if(hash_a == hash_b){
                                edgesSets.add(new Tuple2<>(hash_a, new Tuple2<>(Integer.parseInt(splitted[0]), Integer.parseInt(splitted[1]))));
                        }
                        return edgesSets.iterator();
                }); // end of first point round 1;
                System.out.println("Number of elements of subsets: " + subsets.count());


                System.out.println("Map Phase:");
                int count = 0;/*
                for(Tuple2<Integer, Tuple2<Integer, Integer>> line : subsets.collect()){
                        System.out.println("Key: " + line._1 + " Value: " + line._2);
                        if(count > 5) break;
                        count++;
                }*/

                JavaPairRDD<Integer, Iterable<Tuple2<Integer, Integer>>> shuffle = subsets.groupByKey();

                System.out.println("Shuffle Phase:");
                count = 0;/*
                for(Tuple2<Integer, Iterable<Tuple2<Integer, Integer>>> line : shuffle.collect()){
                        System.out.println("Key: " + line._1 + " Value: " + line._2);
                        if(count > 10) break;
                        count++;
                }*/

                JavaPairRDD<Integer, Long> reduced = shuffle.mapValues((list) ->{
                        ArrayList<Tuple2<Integer, Integer>> array = new ArrayList<>();
                        for(Tuple2<Integer, Integer> l : list){
                                array.add(l);
                        }
                        return CountTriangles(array);
                });

                for(Tuple2<Integer, Long> line : reduced.collect()){
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
        private static int hashFunct(int a, int b, int c, Integer u){
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

                MR_ApproxTCwithNodeColors(2, docs);
        }
}
