package bdtc.lab1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.*;


public class HW1Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text amounts = new Text();
    private final Text word = new Text();
    private static final int scale = 60;

    // The regex pattern to match the strings
    private final static String regexStr = "([0-9]{1,2}), ([0-9]{10}), ([0-9]{2})";
    Pattern lineRegex = Pattern.compile(regexStr);

    @Override
    protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException{
        super.setup(context);
        context.getConfiguration().setIfUnset("scale","60");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        int timeScaled;
        Matcher matcher = lineRegex.matcher(line);

        if (!matcher.find()) {
            context.getCounter(CounterType.MALFORMED).increment(1);
        } else {
            timeScaled = Integer.parseInt(matcher.group(2)) % (scale * 1000) * 10000;

            word.set(matcher.group(1));
            amounts.set(matcher.group(3) + "," + timeScaled);
            context.write(word, amounts);
        }
    }
}
