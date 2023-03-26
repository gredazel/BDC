import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;
public class MapReduceTriangles {
        //Commento
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
        public int MR_ApproxTCwithNodeColors(int c, JavaRDD<String> edges) {
                JavaPairRDD<Integer, String> subsets;
                subsets = edges.flatMapToPair((document) -> {
                        String[] tokens = document.split("\\r?\\n");
                        ArrayList<String>[] edgesSets = new ArrayList[c]; //string ArrayList that represent the c sets of edges; each element is a set of edges
                        for(String token : tokens){
                                String verteces[] = token.split(",");
                                int a = hashFunct(c, Integer.parseInt(verteces[0]));
                                int b = hashFunct(c, Integer.parseInt(verteces[1]));
                                if (a == b)
                                        edgesSets[a].add(token);
                        } // THIS is FOR CREATING THE C PARTITIONS AND PUT IN THEM ASSOCIATED EDGES
                        
                        ArrayList<Tuple2<String, Tuple2<Integer, Integer>>> pairs = new ArrayList<>();
                        for(int i = 0; i < c; i++)
                                for (String token : edgesSets[i]){
                                        String[] verteces = token.split(",");
                                        Tuple2<Integer, Integer> tuple = new Tuple2(Integer.parseInt(verteces[0]), Integer.parseInt(verteces[1]));
                                        pairs.add(new Tuple2<>(Integer.toString(i), tuple)); //ok qui non sono PER NIENTE sicura che si faccia così... anzi...
                                }

                        return pairs.iterator();
                }).reduceByKey((x, y) -> y._1() + y._2()); // end of first point round 1; don't know how to solve this error

                return 0;
        }

        /**
         * Calculate the value of the hash function of a given vertex u
         * @param c integer parameter used to partition data
         * @param u value of the considered vertex
         * @return hash function's value of vertex u
         */
        private int hashFunct(int c, Integer u){
                Random rand = new Random();
                int a = rand.nextInt(p);
                int b = rand.nextInt(p);
                return (((a*u)+b)%p)%c;
        }
}
