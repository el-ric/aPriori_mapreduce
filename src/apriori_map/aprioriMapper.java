package apriori_map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Mapper;

public class aprioriMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	   System.err.println("value" + value.toString());     
        	String[] itr = value.toString().split(","); //Break string into words
                //System.err.println("aprioriMapper Start");
                for(String item: itr){ 
                	//System.out.println(item);
                	 word.set(item);
                	 context.write(word, one);
                	// allitems = allitems + item;

                }
                
              //  System.err.println("aprioriReducer End"); 
        	    //  while (itr.hasMoreTokens()) {
        	//              word.set(itr.nextToken());
        	//              context.write(word, one); //maps words
                     //      }               
        }
};