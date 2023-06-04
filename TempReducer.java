import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        int cont = 0;
        for (IntWritable val : values) {
            sum += val.get();
            cont += 1;
        }
        int prom = sum / cont;
        context.write(key, new IntWritable(prom));
    }
}
