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
        values.add(new Text("6,1")); // (6 + 2 + 1 + 1) == 10 - сумма
        values.add(new Text("2,1")); // всего участвовало 4 слагаемых
        values.add(new Text("1,1"));
        values.add(new Text("1,1"));
        combineDriver
                .withInput(new Text("test_key,100"), values)
                .withOutput(new Text("test_key,100"), new Text("10,4"))
                .runTest();
    }
}
