package apriori_map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class aprioriReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        private static int total_read = 0;
        private static int supportFrequency = 0;
        
    	public static boolean checkFrequency(int sum, int supportFrequency) {
    		/*		
    		double s = 1.0 * sum/total;
    		if(s >=  apriori.supportThreshold)
    			return true;
    		else
    			return false;
    		*/
    		
    		if (sum>=supportFrequency)
    			return true;
    		else
    			return false;
    	}
    	
    	
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