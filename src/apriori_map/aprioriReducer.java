package apriori_map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;




public class aprioriReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        
    	//Check if a number meets support threshold
    	private static boolean checkSupport(int sum) {
    		boolean isFrequent;
    		double s = 1.0 * sum/apriori.total_read;
    		
    		
    		System.err.println("total_records " +  apriori.total_read + " sum:" + sum + " difference " + s); 

    		if(s >=  apriori.supportThreshold)
    			isFrequent = true;
    		else
    			isFrequent = false;
    		
    		return isFrequent;
    		
    		
    	}
    	
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        	int sum = 0;

                for (IntWritable val : values) {
                        sum += val.get(); //counts the number of instances of each word
                }

            	
            	if (checkSupport(sum)) {
            	   result.set(sum);
                   context.write(key, result);
               }

        }
        
}