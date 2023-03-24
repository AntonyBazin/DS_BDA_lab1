import bdtc.lab1.HW1Combiner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class CombinerTest {

    private ReduceDriver<Text, Text, Text, Text> combineDriver;
    @Before
    public void setUp() {
        HW1Combiner combiner = new HW1Combiner();
        combineDriver = ReduceDriver.newReduceDriver(combiner);
    }

    @Test
    public void testCombiner() throws IOException {
        List<Text> values = new ArrayList<Text>();
        values.add(new Text("6,100,1")); // (6 + 2 + 1) == 9 - sum
        values.add(new Text("2,100,1"));
        values.add(new Text("1,100,1"));
        combineDriver
                .withInput(new Text("test_key"), values)
                .withOutput(new Text("test_key"), new Text("9,100,3"))
                .runTest();
    }
}
