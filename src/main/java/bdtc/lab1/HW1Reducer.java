package bdtc.lab1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Редьюсер: суммирует все значения, полученные от {@link HW1Mapper} (либо {@link HW1Combiner}),
 * выдаёт значения метрик, аггрегированных усреднением за определенные промежутки времени.
 */
public class HW1Reducer extends Reducer<Text, Text, Text, Text> {

    // Величина временных интервалов определяется пользователем либо, если она не задана,
    // устанавливается значение по умолчанию - 60 секунд.
    @Override
    protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException{
        super.setup(context);
        context.getConfiguration().setIfUnset("scale","60");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum = 0, count = 0, metric_value, scale, record_weight;
        String[] value_fragments;

        // Получение значения временного интервала для аггрегации
        Configuration conf = context.getConfiguration();
        scale = Integer.parseInt(conf.get("scale"));

        // Проход по всем значениям, полученным от маппера либо комбайнера
        for (Text value: values) {
            // Разделение строки, получаемой от маппера, по разделителю (запятая)
            value_fragments = value.toString().split(",");
            metric_value = Integer.parseInt(value_fragments[0]);
            record_weight = Integer.parseInt(value_fragments[1]);

            // Суммирование метрик. После маппера число повторений каждой строки равно 1,
            // после комбайнера - сумме числа вхождений строк с аналогичным ключом.
            // Поэтому при добавлении очередного значения metric_value к sum
            // требуется домножение на record_weight. В конце происходит усреднение.
            sum += metric_value * record_weight;
            count += record_weight;
        }
        sum /= count;
        context.write(key, new Text(scale + "s, " + sum));
    }
}
