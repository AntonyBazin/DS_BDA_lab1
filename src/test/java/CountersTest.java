import bdtc.lab1.CounterType;
import bdtc.lab1.HW1Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;


public class CountersTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private final String testMalformedLine = "mama mila ramu";
    private final String testLine = "1, 1679128192, 77";
    private final static String regexStr = "([0-9]{1,2}), [0-9]{10}, [0-9]{2}";
    private final static Pattern lineRegex = Pattern.compile(regexStr);
    
    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapperCounterOne() throws IOException  {
        mapDriver
                .withInput(new LongWritable(), new Text(testMalformedLine))
                .runTest();
        assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }

    @Test
    public void testMapperCounterZero() throws IOException {
        Matcher matcher = lineRegex.matcher(testLine);
        if(!matcher.find()){
            System.out.println("No match!\n");
        }
        mapDriver
                .withInput(new LongWritable(), new Text(testLine))
                .withOutput(new Text(matcher.group(1)), new IntWritable(77))
                .runTest();
        assertEquals("Expected 1 counter increment", 0, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }

    @Test
    public void testMapperCounters() throws IOException {
        Matcher matcher = lineRegex.matcher(testLine);
        if(!matcher.find()){
            System.out.println("No match!\n");
        }
        mapDriver
                .withInput(new LongWritable(), new Text(testLine))
                .withInput(new LongWritable(), new Text(testMalformedLine))
                .withInput(new LongWritable(), new Text(testMalformedLine))
                .withOutput(new Text(matcher.group(1)), new IntWritable(77))
                .runTest();

        assertEquals("Expected 2 counter increment", 2, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }
}

