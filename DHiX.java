/*
This is the implementation of the paper "DHiX: Dynamic and Hybrid Real-time Window Aggregations with Constant Time Complexity".
By Savong Bou, Toshiyuki Amagasa, Hiroyuki Kitagawa
*/
import java.util.Arrays;
import java.util.List;

public class DHiX {
    public static void main(String[] args) {
        DHiXEngine engine = new DHiXEngine();

        List<DHiXEngine.Record> stream = Arrays.asList(
            new DHiXEngine.Record(1, 5),
            new DHiXEngine.Record(2, 10),
            new DHiXEngine.Record(3, 20),
            new DHiXEngine.Record(4, 15),
            new DHiXEngine.Record(5, 25)
        );

        engine.execute(stream, "count", 3, 2); // count window W=3, slide S=2
        // OR: engine.execute(stream, "time", 3, 2); // for time-based window
    }
}
