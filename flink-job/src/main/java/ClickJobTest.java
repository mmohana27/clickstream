import static org.junit.Assert.assertEquals;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

public class ClickJobTest {

    @Test
    public void parseExtractsPage() {
        String json = "{\"user_id\":\"user-A\",\"page\":\"/cart\",\"ts\":1730000000000}";
        assertEquals("/cart", ClickJob.parse(json));
    }

    @Test
    public void mergeAddsCounts() {
        Tuple2<String, Integer> a = Tuple2.of("/home", 3);
        Tuple2<String, Integer> b = Tuple2.of("/home", 4);
        Tuple2<String, Integer> result = ClickJob.merge(a, b);
        assertEquals("/home", result.f0);
        assertEquals(Integer.valueOf(7), result.f1);
    }
}