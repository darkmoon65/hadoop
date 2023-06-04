import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        String[] line = value.toString().split("\\n");
        for (String str : line) {
            String[] datos = str.toString().split("\\s");
            IntWritable temp = new IntWritable(Integer.parseInt(datos[1]));
            Text fecha = new Text(datos[0]);
            context.write(fecha, temp);
        }
    }
}
