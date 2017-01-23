package apriori_map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class aprioriReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        private static int total_read = 0;
        private static int supportFrequency = 0;
        
        /*
         * Accepts: Count of each item set, and support frequency determined by the user
         * Returns: True, if count >= support frequency, False, if not
         * Purpose: Determines if an item set should be considered as "frequent"
         */

    	public static boolean checkFrequency(int sum, int supportFrequency) {    		
    		if (sum>=supportFrequency)
    			return true;
    		else
    			return false;
    	}
    	
    	 /*
    	   * Accepts: Generic parameters for a reducer job
    	   * Returns: None, but writes reducer output data to file
    	   * Purpose: Reads data written by the mapper into the context, and performs the reducer function
    	   */
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	int sum = 0;
            
        	for (IntWritable val : values) {
        		sum += val.get(); //counts the number of instances of each word
            }
        	//total_read = context.getConfiguration().getInt("total_read",0);
        	supportFrequency = context.getConfiguration().getInt("supportFrequency",0);
        	if (checkFrequency(sum,supportFrequency)) {
               result.set(sum);
               context.write(key, result);
            }
        }
}