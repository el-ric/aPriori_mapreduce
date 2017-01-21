package apriori_map;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Mapper;




public class aprioriMapper3 extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
     
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {    
            //System.err.println("apriorimapper 3 start"); 
            String[] itr = value.toString().split(","); //Break string into words

               
	            int size =  itr.length;
	            int target = 1;
	            int i = 0;
	            String[] join_items = new String[target + 1];
	            //System.err.println("aprioriMapper Start");
	            for(i=0;i<size;i++){ 
	            	
	            	//System.err.println(itr[i] +"|" + i);
	            	for(int j=i+1;j<size;j++){
	            		int k = 0;    		
	                   // System.err.println("value" + itr[i]); 
	                    if (apriori.frequent_items.contains(itr[i]) && apriori.frequent_items.contains(itr[j])) {
		            		join_items[k] = itr[i]; 
		            		join_items[k +1] = itr[j];
		            		String basket = String.join(",", join_items);
		            	//	System.err.println("New basket" + basket ); 
		            		word.set(basket);
		                	context.write(word, one);
		                	
	                    } else {
	                    	
	                    	//System.err.println("Element not found"); 
	                    }
	            	}

	            }
            
            
	            apriori.total_read[1] = apriori.total_read[1] + 1;   
            //System.err.println("apriorimapper 3 End"); 
          
        }
};