package bdtc.lab1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.*;

/**
 * Класс маппера. Получает из конфигурации задачи значение временного интервала, по которому
 * следует аггрегировать значения. При отсутствии соответствующего параметра задаст его со
 * значением 60 секунд. Преобразует временную метку в соответствии с заданным интервалом, и
 * передает далее ключ (id записи,временная_метка) и значение (значение_метрики,число_вхождений)
 */

public class HW1Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text amounts = new Text();
    private final Text word = new Text();

    // Регулярное выражение для отбора подходящих по формату строк
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
        int timeScaled, scale;
        Matcher matcher = lineRegex.matcher(line);

        // Получение значения временного интервала для аггрегации
        Configuration conf = context.getConfiguration();
        scale = Integer.parseInt(conf.get("scale"));

        // Проверка соответствия строки шаблону
        if (!matcher.find()) {
            context.getCounter(CounterType.MALFORMED).increment(1);
        } else {
            // Отбрасывание незначащих разрядов временной метки, представленной в миллисекундах
            timeScaled = Integer.parseInt(matcher.group(2)) % (scale * 1000) * 10000;

            // Задание ключа и значения
            word.set(matcher.group(1) + "," + timeScaled);
            amounts.set(matcher.group(3) + "," + 1);
            context.write(word, amounts);
        }
    }
}
