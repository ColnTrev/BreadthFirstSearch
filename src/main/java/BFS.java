import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;


import java.util.*;

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
        String source = "1";
        SparkConf conf = new SparkConf().setAppName("Breadth First Search");
        JavaSparkContext context = new JavaSparkContext(conf);
        LongAccumulator encountered = context.sc().longAccumulator();
        final Broadcast<String> sourceId = context.broadcast(source);

        JavaRDD<String> lines = context.textFile(inputFile);

        JavaPairRDD<String,Data> operations = lines.mapToPair(line->{
            String[] tokens = line.split(";");
            String node = tokens[0];
            String[] cons = tokens[1].split(",");
            List<String> connections = new ArrayList<>(Arrays.asList(cons));
            Integer distance = Integer.MAX_VALUE;
            String status = "WHITE";
            if(node.equals(sourceId.value())){
                distance = 0;
                status = "GREY";
                encountered.add(1);
            }
            return new Tuple2<>(node, new Data(connections,distance,status));
        });
        operations.collect();

        while(encountered.value() > 0){
            encountered.setValue(0);
            JavaPairRDD<String, Data> processed = operations.flatMapToPair(entry->{
                List<Tuple2<String, Data>> results = new ArrayList<>();
                String node = entry._1();
                List<String> cons = entry._2().connections;

                Integer distance = entry._2().distance;
                String status = entry._2().status;

                if(status.equals("GREY")){
                    for(String connection : cons) {
                        String nextNode = connection;
                        Integer nextDistance = distance + 1;
                        String nextStatus = "GREY";
                        encountered.add(1);
                        Tuple2<String, Data> newEntry = new Tuple2<>(nextNode, new Data(new ArrayList<>(), nextDistance, nextStatus));
                        results.add(newEntry);
                    }
                    status = "BLACK";
                }

                results.add(new Tuple2<>(node, new Data(cons, distance, status)));
                return results.iterator();
            });

            //processed.collect(); //kicks off map function...spark trick
            System.out.println("+++++PROCESSED RESULTS+++++++");
            processed.foreach(entry -> System.out.println(entry._1() + " " + entry._2().status));
            operations = processed.reduceByKey((k1, k2) ->{
                List<String> cons = null;
                Integer dist = Integer.MAX_VALUE;
                String stat = "WHITE";
                if(!k1.connections.isEmpty()){
                    cons = new ArrayList<>(k1.connections);
                }
                if(!k2.connections.isEmpty()){
                    cons = new ArrayList<>(k2.connections);
                }

                if(k1.distance < dist){
                    dist = k1.distance;
                }
                if(k2.distance < dist){
                    dist = k2.distance;
                }

                if(!k1.status.equals("WHITE") && k2.status.equals("WHITE")){
                    stat = k1.status;
                }
                if(k1.status.equals("WHITE") && !k2.status.equals("WHITE")){
                    stat = k2.status;
                }
                if(k1.status.equals("GREY") && k2.status.equals("BLACK")){
                    stat = k2.status;
                }
                if(k1.status.equals("BLACK") && k2.status.equals("GREY")){
                    stat = k1.status;
                }
                if(stat.equals("WHITE")){
                    stat = k1.status;
                }
                return new Data(cons, dist, stat);
            });
            System.out.println("+++++OPERATIONS RESULTS+++++++");
            operations.foreach(entry -> System.out.println(entry._1() + ' ' + entry._2().status));
        }

    }
}
