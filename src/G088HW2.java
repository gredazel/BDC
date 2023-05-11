import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Int;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;

public class G088HW2 {
    static final int p = 8191; // constant used to calculate hash function

    public static void main(String[] args){
        if(args.length < 3){
            System.out.println("TOO FEW ARGS");
        }
        int C = Integer.parseInt(args[0]);
        int R = Integer.parseInt(args[1]);
        int F = Integer.parseInt(args[2]);
        String filepath = args[3];

        SparkConf conf = new SparkConf(true).setAppName("Triangles");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        //convert string RDD into JavaPairRDD<Integer, Integer>
        JavaPairRDD<Integer, Integer> edges = MakeEdgeRDD(sc.textFile(filepath)).repartition(32).cache();

        System.out.println("Dataset = " + filepath);
        System.out.println("Number of edges = " + edges.count());
        System.out.println("Number of colors = " + C);
        System.out.println("Number of repetitions = " + R);
        
        if(F == 0){
            //fill array with results of R runs and calculate average time of node colouring
            ArrayList<Long> ColorApprox = new ArrayList<>();
            long avgTime = System.currentTimeMillis();
            for (int i = 0; i < R; i++){
                ColorApprox.add(MR_ApproxTCwithNodeColors(C, edges));
            }
            avgTime = System.currentTimeMillis() - avgTime;
            Collections.sort(ColorApprox);
            avgTime /= R;
            System.out.println("Approximation algorithm with node coloring");
            System.out.println("- Number of triangles (median over " + R + " runs) = " + ColorApprox.get(R/2));
            System.out.println("- Running time (average over " + R + " runs) = " + avgTime + "ms");
        }else if(F ==1) {
            long avgTime = System.currentTimeMillis();
            long out = 0L;
            for (int i = 0; i < R; i++){
                out = MR_ExactTC(edges, C);
            }
            avgTime = System.currentTimeMillis() - avgTime;
            avgTime /= R;
            System.out.println("Exact algorithm with node coloring");
            System.out.println("- Number of triangles  = " + out);
            System.out.println("- Running time (average over " + R + " runs) = " + avgTime + "ms");
        }else {
            System.out.println("Error, F value must be either 1 or 0, current: " + F);
            return;
        }
    }

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

    public static Long CountTriangles2(ArrayList<Tuple2<Integer, Integer>> edgeSet, Tuple3<Integer, Integer, Integer> key, long a, long b, long p, int C) {
        if (edgeSet.size()<3) return 0L;
        HashMap<Integer, HashMap<Integer,Boolean>> adjacencyLists = new HashMap<>();
        HashMap<Integer, Integer> vertexColors = new HashMap<>();
        for (int i = 0; i < edgeSet.size(); i++) {
            Tuple2<Integer,Integer> edge = edgeSet.get(i);
            int u = edge._1();
            int v = edge._2();
            if (vertexColors.get(u) == null) {vertexColors.put(u, (int) ((a*u+b)%p)%C);}
            if (vertexColors.get(v) == null) {vertexColors.put(v, (int) ((a*v+b)%p)%C);}
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
                        if (w>v && (uAdj.get(w)!=null)) {
                            ArrayList<Integer> tcol = new ArrayList<>();
                            tcol.add(vertexColors.get(u));
                            tcol.add(vertexColors.get(v));
                            tcol.add(vertexColors.get(w));
                            Collections.sort(tcol);
                            boolean condition = (tcol.get(0).equals(key._1())) && (tcol.get(1).equals(key._2())) && (tcol.get(2).equals(key._3()));
                            if (condition) {numTriangles++;}
                        }
                    }
                }
            }
        }
        return numTriangles;
    }

    /**
     * ALGORITHM 1
     * @param c number of partitions
     * @param edges JavaPairRDD containing all edges
     * @return approximation of triangles number using color approximation
     */
    public static long MR_ApproxTCwithNodeColors(int c, JavaPairRDD<Integer, Integer> edges) {
        Random rand = new Random();
        int a = rand.nextInt(p - 1) + 1;
        int b = rand.nextInt(p);

        return edges.flatMapToPair((token) -> {

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
        }).reduceByKey((x,y) -> x + y).first()._2() *c *c;

    }

    public static long MR_ExactTC(JavaPairRDD<Integer, Integer> edges, int c){
        Random rand = new Random();
        int a = rand.nextInt(p - 1) + 1;
        int b = rand.nextInt(p);
        JavaPairRDD<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, Integer>> map1 = edges.flatMapToPair((token) -> {
            ArrayList<Tuple2<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, Integer>>> edgesSets = new ArrayList<>();
            int color1 = hashFunct(c, token._1(), a, b);
            int color2 = hashFunct(c, token._2(), a, b);;

            for(int i = 0; i < c; i++){
                ArrayList<Integer> colors = new ArrayList<>();
                colors.add(color1);
                colors.add(color2);
                colors.add(i);
                Collections.sort(colors);
                edgesSets.add(new Tuple2<>(new Tuple3<>(colors.get(0), colors.get(1), colors.get(2)), new Tuple2<>(token._1(), token._2())));
            }
            return edgesSets.iterator();
        });
        JavaPairRDD<Integer, Long> reduce1 = map1.groupByKey().mapToPair((e)->{
            ArrayList<Tuple2<Integer, Integer>> same_key_edges = new ArrayList<>();
            for(Tuple2<Integer, Integer> elem : e._2()){
                same_key_edges.add(elem);
            }

            return new Tuple2<>(0, CountTriangles2(same_key_edges, e._1(), a, b, p, c));
        });
        long out = reduce1.reduceByKey((x,y)->(x + y)).first()._2();
        return out;
    }

    /**
     * Method to convert into JavaPairRDD a JavaRDD (in this case the edges RDD)
     * @param stringEdges input RDD of strings
     * @param stringEdges input RDD
     * @return conversion to JavaPairRDD<Integer, Integer>
     */
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
    private static int hashFunct(int c, int u, int a, int b){
        return (((a*u)+b)%p)%c;
    }
}
