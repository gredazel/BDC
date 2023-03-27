import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class G088HW1 {
    public static void main(String[] args) {
        if(args.length < 3){
            System.out.println("TOO FEW ARGS");
        }
        int C = Integer.parseInt(args[0]);
        int R = Integer.parseInt(args[1]);
        String filepath = args[2];

        SparkConf conf = new SparkConf(true).setAppName("Triangles");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        JavaRDD<String> docs = sc.textFile(filepath).repartition(8).cache();

        System.out.println("Number of triangles =" + MR_ApproxTCwithNodeColors(C, docs));
    }

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
    public static long MR_ApproxTCwithNodeColors(int c, JavaRDD<String> edges) {

        JavaPairRDD<Integer, Tuple2<Integer, Integer>> coloredEdges;
        coloredEdges = edges.flatMapToPair((document) -> {
            String[] tokens = document.split("\\r?\\n");

            ArrayList<Tuple2<Integer, Tuple2<Integer, Integer>>> edgesSets = new ArrayList<>(); //string ArrayList that represent the c sets of edges; each element is a set of edges
            for(String token : tokens){
                String verteces[] = token.split(",");
                int vert1 = Integer.parseInt(verteces[0]);
                int vert2 = Integer.parseInt(verteces[1]);
                Integer a = hashFunct(c, vert1);
                Integer b = hashFunct(c, vert2);
                Tuple2<Integer, Integer> val = new Tuple2<>(vert1, vert2);
                if (a == b){
                    edgesSets.add(new Tuple2<>(a, val));
                }
            } // THIS is FOR CREATING THE C PARTITIONS AND PUT IN THEM ASSOCIATED EDGES
            return edgesSets.iterator();
        });//Map into <color, <v1, v2>>

        JavaPairRDD<Integer, Long> counted = coloredEdges.groupByKey().mapToPair((e) ->{
            ArrayList<Tuple2<Integer, Integer>> E = new ArrayList<>();
            for(Tuple2<Integer, Integer> elem : e._2()){
                E.add(elem);
            }
            return new Tuple2<>(0, CountTriangles(E));
        });

        long totTriangles = c*c * counted.reduce((x, y) -> new Tuple2<>(0, x._2() + y._2()))._2();
        return totTriangles;
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
}
