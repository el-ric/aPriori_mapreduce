package wordcount;
import java.io.IOException;
import java.util.StringTokenizer;


import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMappermap extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                StringTokenizer itr = new StringTokenizer(value.toString(),","); //Break string into words
                while (itr.hasMoreTokens()) {
                        word.set(itr.nextToken());
                        context.write(word, one); //maps words
                }               
        }
};