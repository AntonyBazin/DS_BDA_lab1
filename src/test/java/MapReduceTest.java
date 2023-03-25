import bdtc.lab1.HW1Mapper;
import bdtc.lab1.HW1Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class MapReduceTest {

    private MapDriver<LongWritable, Text, Text, Text> mapDriver;
    private ReduceDriver<Text, Text, Text, Text> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;
    private final String testLineOne = "2, 1679128192, 77\n";
    private final String testLineTwo = "2, 1679128172, 64\n";

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapperSingle() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testLineOne))
                .withOutput(new Text("2,279850000"), new Text("77,1"))
                .runTest();
    }

    @Test
    public void testMapperMultiple() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testLineOne))
                .withInput(new LongWritable(), new Text(testLineOne))
                .withOutput(new Text("2,279850000"), new Text("77,1"))
                .withOutput(new Text("2,279850000"), new Text("77,1"))
                .runTest();
    }

    @Test
    public void testReducerSingleWeight() throws IOException {
        List<Text> values = new ArrayList<Text>();
        values.add(new Text("6,1")); // (6 + 2)/2 == 4 - среднее значение
        values.add(new Text("2,1"));
        reduceDriver
                .withInput(new Text("test_key,100"), values)
                .withOutput(new Text("test_key,100"), new Text("60s, 4"))
                .runTest();
    }

    @Test
    public void testReducerAfterCombiner() throws IOException {
        List<Text> values = new ArrayList<Text>();
        values.add(new Text("6,2"));
        reduceDriver
                .withInput(new Text("test_key,100"), values)
                .withOutput(new Text("test_key,100"), new Text("60s, 3"))
                .runTest();
    }

    @Test
    public void testMapReduceManyInstances() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testLineOne)) // аггрегация усреднением,
                .withInput(new LongWritable(), new Text(testLineOne)) // повторы одинаковых строк
                .withInput(new LongWritable(), new Text(testLineOne)) // не изменят среднего
                .withInput(new LongWritable(), new Text(testLineOne))
                .withOutput(new Text("2,279850000"), new Text("60s, 77"))
                .runTest();
    }

    @Test
    public void testMapReduceUniqueInstances() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testLineOne))
                .withInput(new LongWritable(), new Text(testLineTwo))
                .withOutput(new Text("2,279850000"), new Text("60s, 70"))
                .runTest();
    }

    @Test
    public void testMapReduceUniqueInstances2() throws IOException {
        final String testLine1 = "0, 1679745825, 81\n";
        final String testLine2 = "0, 1679745898, 82\n";
        final String testLine3 = "0, 1679745938, 81\n";
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testLine1))
                .withInput(new LongWritable(), new Text(testLine2))
                .withInput(new LongWritable(), new Text(testLine3))
                .withOutput(new Text("0,279950000"), new Text("60s, 81"))
                .runTest();
    }
}
