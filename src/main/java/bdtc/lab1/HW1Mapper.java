package bdtc.lab1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.htrace.fasterxml.jackson.databind.deser.DataFormatReaders;

import java.io.IOException;
import java.util.regex.*;


public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private final Text word = new Text();
    // The regex pattern to match the strings
    private final static String regexStr = "(\\d{1,2}), \\d{10}, \\d{2}";
    Pattern lineRegex = Pattern.compile(regexStr);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Matcher matcher = lineRegex.matcher(line);

        if (!matcher.find()) {
            context.getCounter(CounterType.MALFORMED).increment(1);
        } else {
            word.set(matcher.group(1));
            context.write(word, one);
        }
    }
}
