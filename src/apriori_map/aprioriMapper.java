package apriori_map;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Mapper;

public class aprioriMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private static int passNum = apriori.passNumber;
        public static ArrayList<String> candidateItemset = apriori.candidateItems;        
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	passNum = apriori.passNumber;
        	candidateItemset = apriori.candidateItems;   
        	
        	if(passNum == 1) { // Perform word count for first pass
	        	String[] itr = value.toString().split(","); //Break string into words
	                for(String item: itr) { 
	                	 word.set(item);
	                	 context.write(word, one);
	                }
        	}
        	else {
           		String line = value.toString();
        		for(int i=0; i<candidateItemset.size();i++) {
        			String candidateItem = candidateItemset.get(i);
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
                
            apriori.total_read[passNum] = apriori.total_read[passNum] + 1;       
        }
        
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