package apriori_map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Mapper;

public class aprioriMapperk extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private static int passNum = apriori.passNumber;
        public static ArrayList<String> candidateItemset = apriori.candidateItems;        
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        		passNum = apriori.passNumber;
            	candidateItemset = apriori.candidateItems;    
        		//System.err.println("\n MAP REDUCE INPUT \n" + value);
        		String line = value.toString();
        		for(int i=0; i<candidateItemset.size();i++) {
        			String candidateItem = candidateItemset.get(i);
        			String[] check = candidateItem.split(",");
        			
        			String[] lines = line.split(",");
        			Arrays.sort(lines);
        			line = String.join(",", lines);
        			
        			boolean flag = true;
        			for(int j=0; j<check.length; j++)
        				if(!line.contains(check[j]))
        					flag = false;
        			
        			if(flag == true) {
        				Arrays.sort(check);
        				candidateItem = String.join(",", check);
        				Text toAdd = new Text(candidateItem);
        				context.write(toAdd, one);
        			}
        			/*
        			if(line.contains(candidateItem)) {
        				Text toAdd = new Text(candidateItem);
        				context.write(toAdd, one);
        			}*/
        		}    
        	
                
            apriori.total_read[passNum] = apriori.total_read[passNum] + 1;       
        }
       
};