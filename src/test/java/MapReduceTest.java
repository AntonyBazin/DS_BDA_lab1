import bdtc.lab1.CounterType;
import bdtc.lab1.HW1Mapper;
import bdtc.lab1.HW1Reducer;
import org.apache.hadoop.io.IntWritable;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class MapReduceTest {

    private MapDriver<LongWritable, Text, Text, Text> mapDriver;
    private ReduceDriver<Text, Text, Text, Text> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;
    private Matcher matcher;
    private final String testLine = "2, 1679128192, 77\n";
    private final static String regexStr = "([0-9]{1,2}), [0-9]{10}, [0-9]{2}";
    private final static Pattern lineRegex = Pattern.compile(regexStr);

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        matcher = lineRegex.matcher(testLine);
    }

    @Test
    public void testMapper() throws IOException {
        if(!matcher.find()){
            System.out.println("No match!\n");
        }
        mapDriver
                .withInput(new LongWritable(), new Text(testLine))
                .withOutput(new Text(matcher.group(1)), new Text("77,281920000"))
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<Text> values = new ArrayList<Text>();
        values.add(new Text("6,100")); // (6 + 2)/2 == 4 - mean value
        values.add(new Text("2,100"));
        reduceDriver
                .withInput(new Text("test_key"), values)
                .withOutput(new Text("test_key"), new Text("4, 100, 60s"))
                .runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        if(!matcher.find()){
            System.out.println("No match!\n");
        }
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testLine))
                .withInput(new LongWritable(), new Text(testLine))
                .withOutput(new Text(matcher.group(1)), new Text("77, 281920000, 60s"))
                .runTest();
    }
}
