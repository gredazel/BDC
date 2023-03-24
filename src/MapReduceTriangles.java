import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.ArrayList;
import java.util.Random;
public class MapReduceTriangles {
        static final int p = 8191; // constant used to calculate hash function
        public int MR_ApproxTCwithNodeColors(int c, JavaRDD<String> edges) {
                ArrayList<String>[] edgesSets = new ArrayList[c]; //string ArrayList that represent the c sets of edges; each element is a set of edges
                for(int i = 0; i < edges.count(); i++){

                        int index = hashFunct(c, )
                }
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
