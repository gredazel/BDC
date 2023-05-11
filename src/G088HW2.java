import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;

public class G088HW2 {
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

        JavaRDD<String> rawData = sc.textFile(filepath);
        JavaPairRDD<Integer, Integer> edges = MakeEdgeRDD(rawData).repartition(32).cache();;


        System.out.println("Dataset = " + filepath);
        System.out.println("Number of edges = " + edges.count());
        System.out.println("Number of colors = " + C);
        System.out.println("Number of repetitions = " + R);

        //------------ALGORITHM 1----------------
        ArrayList<Long> ColorApprox = new ArrayList<Long>();
        long avgTime = System.currentTimeMillis();
        for (int i = 0; i < R; i++){
            ColorApprox.add(MR_ApproxTCwithNodeColors(C, edges));
        }
        Collections.sort(ColorApprox);
        avgTime = System.currentTimeMillis() - avgTime;
        avgTime /= R;
        System.out.println("Approximation through node coloring");
        System.out.println("- Number of triangles (median over " + R + " runs) = " + ColorApprox.get(R/2));
        System.out.println("- Running time (average over " + R + " runs) = " + avgTime + "ms");

        //------------ALGORITHM 2----------------
        ArrayList<Long> ColorExact = new ArrayList<Long>();
        long Time2 = System.currentTimeMillis();
        for (int i = 0; i < R; i++){
            ColorExact.add(MR_ExactTC(C, edges));
        }
        Time2 = System.currentTimeMillis() - Time2;
        System.out.println("Exact number of triangles");
        System.out.println("- Number of triangles = " + ColorExact.get(ColorExact.size()-1));
        System.out.println("- Running time = " + Time2 + "ms");

        System.out.println("All Triangles in array");
        for(int i = 0; i < ColorExact.size(); i++){
            System.out.println("- Number of triangles = " + ColorExact.get(i));
        }
    }

    static final int p = 8191; // constant used to calculate hash function

    public static Long CountTriangles(Iterator<Tuple2<Integer, Integer>> edgeSet) {
        HashMap<Integer, HashMap<Integer,Boolean>> adjacencyLists = new HashMap<>();
        int i = 0;
        while (edgeSet.hasNext()) {
            Tuple2<Integer,Integer> edge = edgeSet.next();
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
            i++;
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





    public static Long CountTriangles(ArrayList<Tuple2<Integer, Integer>> edgeSet) {
        if (edgeSet.size()<3) return 0L;
        HashMap<Integer, HashMap<Integer,Boolean>> adjacencyLists = new HashMap<>();
        int i = 0;
        for (i = 0; i < edgeSet.size(); i++) {
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
    public static long MR_ApproxTCwithNodeColors(int c, JavaPairRDD<Integer, Integer> edges) {
        Random rand = new Random();
        int a = rand.nextInt(p - 1) + 1;
        int b = rand.nextInt(p);

        long totTriangles = edges.flatMapToPair((token) -> {

            ArrayList<Tuple2<Integer, Tuple2<Integer, Integer>>> edgesSets = new ArrayList<>();
            int color1 = hashFunct(c, token._1(), a, b);
            int color2 = hashFunct(c, token._2(), a, b);;
            if (color1 == color2){
                edgesSets.add(new Tuple2<>(color1, token));
            }
            return edgesSets.iterator();
        }).groupByKey().mapToPair((e) ->{
            ArrayList<Tuple2<Integer, Integer>> E = new ArrayList<>();
            for(Tuple2<Integer, Integer> elem : e._2()){
                E.add(elem);
            }
            return new Tuple2<>(0, CountTriangles(E));
        }).reduceByKey((x,y) -> x + y).first()._2();

        return totTriangles *c *c;
    }

    public static long MR_ApproxTCwithSparkPartitions(int c, JavaPairRDD<Integer, Integer> edges){
        return edges.mapPartitions((edge) ->{

            ArrayList<Long> pair = new ArrayList<>();
            pair.add(CountTriangles(edge));

            return pair.iterator();
        }).reduce((x, y) -> x + y) * c * c;
    }

    public static long MR_ExactTC(int c, JavaPairRDD<Integer, Integer> edges){
        Random rand = new Random();
        int a = rand.nextInt(p - 1) + 1;
        int b = rand.nextInt(p);

        long totTriangles = edges.flatMapToPair((token) -> {
            ArrayList<Tuple2<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, Integer>>> edgesSets = new ArrayList<>();
            int color1 = hashFunct(c, token._1(), a, b);
            int color2 = hashFunct(c, token._2(), a, b);
            for(int i = 0; i < c; i++){
                Tuple3<Integer, Integer, Integer> not_sorted = new Tuple3<Integer, Integer, Integer>(color1, color2, i);
                Tuple3<Integer, Integer, Integer> key = sortTuple3(not_sorted);
                Tuple2<Integer, Integer> value = new Tuple2<Integer, Integer>(token._1(), token._2());
                Tuple2<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, Integer>> pair = new Tuple2<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, Integer>>(key, value);
                edgesSets.add(pair);
            }
            return edgesSets.iterator();
        }).groupByKey().mapToPair((e) ->{
            ArrayList<Tuple2<Integer, Integer>> E = new ArrayList<>();
            for(Tuple2<Integer, Integer> elem : e._2()){
                E.add(elem);
            }
            return new Tuple2<>(0, CountTriangles(E));
        }).reduceByKey((x,y) -> x + y).first()._2();


        return totTriangles;
    }

    public static Tuple3<Integer, Integer, Integer> sortTuple3(Tuple3<Integer, Integer, Integer> tuple) {
        Integer[] sorted = new Integer[] {tuple._1(), tuple._2(), tuple._3()};
        Arrays.sort(sorted);
        return new Tuple3<>(sorted[0], sorted[1], sorted[2]);
    }
    public static JavaPairRDD<Integer, Integer> MakeEdgeRDD(JavaRDD<String> stringEdges){
        return stringEdges.mapToPair((token) -> {
                    String verteces[] = token.split(",");
                    int vert1 = Integer.parseInt(verteces[0]);
                    int vert2 = Integer.parseInt(verteces[1]);
                    return new Tuple2<>(vert1, vert2);});
    }

    /**
     * Calculate the value of the hash function of a given vertex u
     * @param c integer parameter used to partition data
     * @param u value of the considered vertex
     * @param a random integer in [1, p-1] fixed for every run
     * @param b random integer in [0, p-1] fixed for every run
     * @return hash function's value of vertex u
     */
    private static int hashFunct(int c, Integer u, int a, int b){
        return (((a*u)+b)%p)%c;
    }}
