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
        	if(passNum == 1) { // Perform word count for first pass
	        	String[] itr = value.toString().split(","); //Break string into words
	                for(String item: itr) { 
	                	 word.set(item);
	                	 context.write(word, one);
	                }
        	}
                
            apriori.total_read[passNum] = apriori.total_read[passNum] + 1;       
        }
};