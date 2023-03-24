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
    private final String testLine = "2, 1679128192, 77\n";

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testLine))
                .withOutput(new Text("2"), new Text("77,281920000,1"))
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<Text> values = new ArrayList<Text>();
        values.add(new Text("6,100,1")); // (6 + 2)/2 == 4 - среднее значение
        values.add(new Text("2,100,1"));
        reduceDriver
                .withInput(new Text("test_key"), values)
                .withOutput(new Text("test_key"), new Text("100, 60s, 4"))
                .runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testLine)) // аггрегация усреднением,
                .withInput(new LongWritable(), new Text(testLine)) // повторы одинаковых строк
                .withInput(new LongWritable(), new Text(testLine)) // не изменят среднего
                .withInput(new LongWritable(), new Text(testLine))
                .withOutput(new Text("2"), new Text("281920000, 60s, 77"))
                .runTest();
    }
}
