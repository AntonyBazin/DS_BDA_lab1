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

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
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
                .withOutput(new Text(matcher.group(1)), new IntWritable(77))
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(6)); // (6 + 2)/2 == 4 - mean value
        values.add(new IntWritable(2));
        reduceDriver
                .withInput(new Text(testLine), values)
                .withOutput(new Text(testLine), new IntWritable(4))
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
                .withOutput(new Text(matcher.group(1)), new IntWritable(77))
                .runTest();
    }
}
