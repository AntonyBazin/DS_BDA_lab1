package bdtc.lab1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Функция reduce Combiner'a аггрегирует отдельные записи с общим ключом
 * в одну.
 */

public class HW1Combiner extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum = 0, count = 0, time = 0, metric_value, record_weight;
        String[] fragments;


        for (Text value: values) {
            fragments = value.toString().split(",");
            metric_value = Integer.parseInt(fragments[0]);
            time = Integer.parseInt(fragments[1]);
            record_weight = Integer.parseInt(fragments[2]);

            sum += metric_value * record_weight;
            count += record_weight;
        }
        context.write(key, new Text(sum + "," + time + "," + count));
    }
}