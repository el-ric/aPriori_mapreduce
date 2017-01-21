package apriori_map;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class aprioriReducer3 extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
    	
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        	int sum = 0;
        	context.getCounter("", "");
                for (IntWritable val : values) {
                        sum += val.get(); //counts the number of instances of each word
                }

          	if (apriori.checkSupport(sum,apriori.total_read[1])) {
            	 
            	   result.set(sum);
                   context.write(key, result);
                   
               }

        }
        
}