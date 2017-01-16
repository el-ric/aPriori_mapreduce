package apriori_map;

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Mapper;

public class aprioriMapper2 extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private String allitems = "";
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                
            System.err.println("apriorimapper 2 start"); 
            System.err.println(value.toString());
            
                if ( Integer.parseInt(value.toString()) > 1000) {
                	 word.set(key.toString());
                	 IntWritable size = new IntWritable(Integer.parseInt(value.toString()));
                	 context.write(word, size);
                }
                
               // System.err.println("aprioriMapper 2 Start");
              //  for(String item: itr){ 
                	//System.out.println(item);
             //   	 word.set(item);
             //   	 context.write(word, one);
                	// allitems = allitems + item;

            //    }
                
                System.err.println("apriorimapper 2 End"); 
        	    //  while (itr.hasMoreTokens()) {
        	//              word.set(itr.nextToken());
        	//              context.write(word, one); //maps words
                     //      }               
        }
};