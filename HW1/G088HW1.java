
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.*;

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

        //convert string RDD into JavaPairRDD<Integer, Integer>
        JavaPairRDD<Integer, Integer> docs = MakeEdgeRDD(sc.textFile(filepath)).repartition(C).cache();

        System.out.println("Dataset = " + filepath);
        System.out.println("Number of edges = " + docs.count());
        System.out.println("Number of colors = " + C);
        System.out.println("Number of repetitions = " + R);

        //fill array with results of R runs and calculate average time of node colouring
        ArrayList<Long> ColorApprox = new ArrayList<>();
        long avgTime = System.currentTimeMillis();
        for (int i = 0; i < R; i++){
            ColorApprox.add(MR_ApproxTCwithNodeColors(C, docs));
        }
        avgTime = System.currentTimeMillis() - avgTime;
        Collections.sort(ColorApprox);
        avgTime /= R;


        System.out.println("Approximation through node coloring");
        System.out.println("- Number of triangles (median over " + R + " runs) = " + ColorApprox.get(R/2));
        System.out.println("- Running time (average over " + R + " runs) = " + avgTime + "ms");

        //run algorithm 2 and measure time
        long Time2 = System.currentTimeMillis();
        long repartitioned = MR_ApproxTCwithSparkPartitions(C, docs);
        Time2 = System.currentTimeMillis() - Time2;

        System.out.println("Approximation through Spark partitions");
        System.out.println("- Number of triangles = " + repartitioned);
        System.out.println("- Running time = " + Time2 + "ms");
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

    /**
     * ALGORITHM 2
     * @param c number of partitions
     * @param edges JavaPairRDD conteining all the edges
     * @return approximations of triangles number calculated with spark partitions
     */
    public static long MR_ApproxTCwithSparkPartitions(int c, JavaPairRDD<Integer, Integer> edges){
        return edges.mapPartitions((edge) ->{
            ArrayList<Tuple2<Integer, Integer>> temp = new ArrayList<>();
            while (edge.hasNext()){
                temp.add(edge.next());
            }
            ArrayList<Long> pair = new ArrayList<>();
            pair.add(CountTriangles(temp));

            return pair.iterator();
        }).reduce((x, y) -> x + y) * c * c;
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
    private static int hashFunct(int c, Integer u, int a, int b){
        return (((a*u)+b)%p)%c;
    }}
