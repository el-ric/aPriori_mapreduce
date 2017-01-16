package apriori_map;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Reducer;

public class aprioriReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	 // System.err.println("aprioriReducer Start");    
        	  int sum = 0;
                for (IntWritable val : values) {
                        sum += val.get(); //counts the number of instances of each word
                }
                result.set(sum);
                context.write(key, result); //Key is the word, result is the sum    
              //  System.err.println("aprioriReducer End"); 
        }
        
}