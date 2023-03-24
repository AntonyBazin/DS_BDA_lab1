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
        int sum = 0, count = 0, metric_value = 0, time = 0, scale = 0;
        String[] fragments;

        Configuration conf = context.getConfiguration();
        scale = Integer.parseInt(conf.get("scale"));

        for (Text value: values) {
            fragments = value.toString().split(",");
            metric_value = Integer.parseInt(fragments[0]);
            time = Integer.parseInt(fragments[1]);

            sum += metric_value;
            ++count;
        }
        sum /= count;
        context.write(key, new Text(time + ", " + scale + "s, " + sum));
    }
}
