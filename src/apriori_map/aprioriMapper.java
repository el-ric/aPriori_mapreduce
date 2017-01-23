package apriori_map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;

public class aprioriMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private static int passNum = 0;
        private static int total_read = 0;
        private static ArrayList<String> candidateItemsetMap  = new ArrayList<String>();        

        /*
         * Accepts: Map reduce job context (information needed for the mapper)
         * Returns: None, but assigns the general parameter values of the mapper
         * Purpose: Initializes the mapper
         */
        @Override
		public void setup(Context context) throws IOException {
        	Configuration conf = context.getConfiguration();
        	passNum = conf.getInt("passNumber",0);
        	 
        	if(passNum > 1){
	        	String items_tmp = conf.get("candidateItems");
	        	String[] items_array = items_tmp.split(";:");
	        	this.candidateItemsetMap.clear();
	        	for (int j=0;j< items_array.length; j++){
	        		this.candidateItemsetMap.add(items_array[j]); 
	        	}
        	}
        }
        
        /*
         * Accepts: Generic map reduce parameters
         * Returns: None
         * Purpose: Writes data to the context, which can be picked up be the reducer
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	if(passNum == 1) { // Perform word count for first pass
	        	String[] itr = value.toString().split(","); //Break string into words
	                for(String item: itr) { 
	                	 word.set(item);
	                	 context.write(word, one);
	                }
        	}
        	else {
           		String line = value.toString();
        		for(int i=0; i<candidateItemsetMap.size();i++) {
        			String candidateItem = candidateItemsetMap.get(i);
        			String[] check = candidateItem.split(",");
        			
        			boolean flag = true;
        			for(int j=0; j<check.length; j++)
        				if(!isItemInBasket(check[j], line))
        					flag = false;
        			
        			if(flag == true) {
        				Text toAdd = new Text(candidateItem);
        				context.write(toAdd, one);
        			}
        		}  
        	}
                
        	context.getConfiguration().setInt("total_read", context.getConfiguration().getInt("total_read",0) + 1);       
        }
        
        /*
         * Accepts: Basket and items
         * Returns: True if items are in the basket, False if not
         * Purpose: Checks if the candidate item set is contained in the line of text from the input file
         */  
        private boolean isItemInBasket(String items, String basket) {
    		String[] itemSet = items.split(",");
    		String[] basketSet = basket.split(",");
    		boolean isPresent = true;
    		
    		for(int i=0; i<itemSet.length; i++) {
    			boolean inBasket = false;
    			for(int j=0; j<basketSet.length; j++) {
    				if(itemSet[i].equals(basketSet[j]))
    					inBasket = true;
    			}
    			if(!inBasket)
    				isPresent = false;
    		}
    		
    		return isPresent;
    	}
};