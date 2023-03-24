package bdtc.lab1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Функция reduce Combiner-a аггрегирует отдельные записи с общим ключом
 * в одну. Поскольку операция взятия среднего не ассоциативна,
 * Combiner просто суммирует значения метрик и ведет подсчет числа вхождений
 * одинаковых записей.
 */

public class HW1Combiner extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum = 0, count = 0, time = 0, metric_value, record_weight;
        String[] fragments;


        for (Text value: values) {
            // Разделение строки, получаемой от маппера, по разделителю (запятая)
            fragments = value.toString().split(",");
            metric_value = Integer.parseInt(fragments[0]);
            time = Integer.parseInt(fragments[1]);
            record_weight = Integer.parseInt(fragments[2]);

            // Суммирование метрик. После маппера число повторений каждой строки равно 1,
            // и, следовательно, при добавлении очередного значения metric_value к sum
            // домножение на record_weight не требуется.
            sum += metric_value;
            count += record_weight;
        }
        context.write(key, new Text(sum + "," + time + "," + count));
    }
}