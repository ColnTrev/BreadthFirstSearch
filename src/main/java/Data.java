import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by colntrev on 4/3/18.
 */
public class Data implements Serializable {
    List<String> connections;
    String status;
    Integer distance;

    public Data(){
        connections = new ArrayList<>();
        status = "WHITE";
        distance = 0;
    }

    public Data(List<String> cons, Integer dist, String health){
        connections = new ArrayList<>(cons);
        distance = dist;
        status = health;
    }
}
