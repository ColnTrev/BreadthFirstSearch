import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam$class;
import org.apache.spark.InternalAccumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by ColnTrev1 on 4/2/18.
 */
public class BFS {
    public static void main(String[] args){
        if(args.length < 3){
            System.out.println("Usage: <input path> <source> <target> <max iterations>");
            System.exit(-1);
        }
        String inputFile = args[0];
        String source = args[1];
        String target = args[2];
        Integer limit = Integer.parseInt(args[3]);
        SparkConf conf = new SparkConf().setAppName("Breadth First Search");
        JavaSparkContext context = new JavaSparkContext(conf);
        LongAccumulator encountered = context.sc().longAccumulator();
        final Broadcast<String> sourceId = context.broadcast(source);
        final Broadcast<String> targetId = context.broadcast(target);

        JavaRDD<String> lines = context.textFile(inputFile);

        JavaRDD<Tuple2<String, Tuple3<String, Integer, String>>> operations = lines.map((String line)->{
            String[] tokens = line.split(";");
            String node = tokens[0];
            String connections = tokens[1];
            Integer distance = Integer.MAX_VALUE;
            String status = "WHITE";
            if(node.equals(sourceId)){
                distance = 0;
                status = "GREY";
            }
            return new Tuple2<>(node, new Tuple3<>(connections, distance, status));
        });
        for(int i = 0; i < limit; i++){
            JavaRDD<Tuple2<String, Tuple3<String,Integer,String>>> processed =
                    operations.flatMap((Tuple2<String, Tuple3<String,Integer,String>> entry)->{
                        List<Tuple2<String, Tuple3<String,Integer,String>>> results = new ArrayList<>();
                        String node = entry._1();
                        String[] connections = entry._2()._1().split(",");
                        Integer distance = entry._2()._2();
                        String status = entry._2()._3();

                        if(status.equals("GREY")){
                            for(String connection : connections) {
                                String nextNode = connection;
                                String cons = "";
                                Integer nextDistance = distance + 1;
                                String nextStatus = "GREY";
                                if (nextNode.equals(targetId)) {
                                    encountered.add(1);
                                }
                                Tuple2<String, Tuple3<String, Integer, String>> newEntry =
                                        new Tuple2<>(nextNode, new Tuple3<>(cons, nextDistance, nextStatus));
                                results.add(newEntry);
                            }
                        }
                        results.add(new Tuple2<>(node, new Tuple3<>(connections, distance, status))); //TODO: fix
                        return results.iterator();
                    });

            processed.collect(); //kicks off map function...spark trick

            processed.reduceByKey(); //TODO processed needs to be a PairRDD
        }
    }
}
