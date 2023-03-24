package bdtc.lab1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Редьюсер: суммирует все единицы полученные от {@link HW1Mapper}, выдаёт суммарное количество пользователей по браузерам
 */
public class HW1Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException{
        super.setup(context);
        context.getConfiguration().setIfUnset("scale","60");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum = 0, count = 0, time = 0, metric_value, scale, record_weight;
        String[] fragments;

        Configuration conf = context.getConfiguration();
        scale = Integer.parseInt(conf.get("scale"));

        for (Text value: values) {
            fragments = value.toString().split(",");
            metric_value = Integer.parseInt(fragments[0]);
            time = Integer.parseInt(fragments[1]);
            record_weight = Integer.parseInt(fragments[2]);

            sum += metric_value * record_weight;
            count += record_weight;
        }
        sum /= count;
        context.write(key, new Text(time + ", " + scale + "s, " + sum));
    }
}
