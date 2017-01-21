package apriori_map;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.*;

public class apriori {
	
	public static String inputFile = "data_for_project.txt";
	public static String outputFolder = "mapreduce";
	public static String outputFile;
	public static double supportThreshold = 0.01;
	public static int passNumber = 1;
	public static int total_read[] = new int[10];
	public static String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new java.util.Date());
	public static ArrayList<String> frequent_items;
	public static ArrayList<Map<String, Integer>> allFrequentItems = new ArrayList<Map<String, Integer>>();
    
	private static void readFrequentItemsFromFile(String filename) {
		frequent_items = new ArrayList<String>();
		Map<String, Integer> freqItemset = new HashMap<String, Integer>();
		
		try {
			 //System.err.println("Output file: " + apriori.outputFile ); 
			BufferedReader bReader = new BufferedReader(new FileReader(filename));
			String line;
			
			while ((line = bReader.readLine()) != null) {
				String itemName = new String();
				Integer itemFreq;
				itemName = line.split("\\t")[0];
				itemFreq = Integer.valueOf(line.split("\\t")[1]);
				frequent_items.add(itemName);
				freqItemset.put(itemName, itemFreq);
			}
			bReader.close();
		}
		catch (Exception e) {
			System.out.println("Error reading input file.");
		}

		allFrequentItems.add(freqItemset);
	}
	
	public static boolean checkSupport(int sum, int total) {
		boolean isFrequent;
		double s = 1.0 * sum/total;
		if(s >=  apriori.supportThreshold)
			isFrequent = true;
		else
			isFrequent = false;
		
		return isFrequent;
	}
	
	public static boolean createJob(String input, String output,Object aprioriMapper, Object aprioriReducer,  Configuration conf){
		boolean success = false;
		
		try {
	        @SuppressWarnings("deprecation")
	       Job job = new Job(conf, "aprior");
	       job.setJarByClass(apriori.class); //Tell hadoop the name of the class every cluster has to look for
	       
	       job.setMapperClass(aprioriMapper.class); //Set class that will be executed by the mapper
	       job.setReducerClass(aprioriReducer.class); //Set the class that will be executed as the reducer
	      
	       job.setOutputKeyClass(Text.class); //Set the class to be used as the key for outputting data to the user
	       job.setOutputValueClass(IntWritable.class); //Set class that will be used as the vaue for outputting data
	     
	       FileInputFormat.addInputPath(job,  new Path(input)); //Get input file name
	       FileOutputFormat.setOutputPath(job, new Path(output));
		
	       success = job.waitForCompletion(true);
		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	       return success;
	}
	
	public static boolean createJob2(String input, String output,Object aprioriMapper, Object aprioriReducer,  Configuration conf){
		boolean success = false;
		
		try {
	        @SuppressWarnings("deprecation")
	       Job job = new Job(conf, "aprior");
	       job.setJarByClass(apriori.class); //Tell hadoop the name of the class every cluster has to look for
	       
	       job.setMapperClass(aprioriMapper3.class); //Set class that will be executed by the mapper
	       job.setReducerClass(aprioriReducer3.class); //Set the class that will be executed as the reducer
	      
	       job.setOutputKeyClass(Text.class); //Set the class to be used as the key for outputting data to the user
	       job.setOutputValueClass(IntWritable.class); //Set class that will be used as the vaue for outputting data
	     
	       FileInputFormat.addInputPath(job,  new Path(input)); //Get input file name
	       FileOutputFormat.setOutputPath(job, new Path(output));
		
	       success = job.waitForCompletion(true);
		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	       return success;
	}
	public static void main(String[] args) throws InterruptedException, ClassNotFoundException {

		try {
		Configuration conf = new Configuration();
		
        String[] otherArgs;
		otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();	
		
        String outputTempDir1 = outputFolder + "/" + timeStamp + "/Temp/1";
        String outputTempDir2 = outputFolder + "/" + timeStamp + "/Temp/2"; 
        
        //Start new job to get frequent items
        boolean success = createJob(inputFile, outputTempDir1,aprioriMapper.class,aprioriReducer.class,conf);
        if(success == true){
           System.err.println("\nPass 1 Completed Succesfully\n");
        }
        
        //Read output from Pass 1
        outputFile = "./"  + outputTempDir1 + "/part-r-00000";
        readFrequentItemsFromFile(outputFile);

      //Start new job to get frequent items
        boolean success2 = createJob2(inputFile, outputTempDir2,aprioriMapper3.class,aprioriReducer3.class,conf);
        if(success2 == true){
        	System.err.println("\nPass 2 Completed Succesfully\n");
        }
        
        //Read output from Pass 2
        outputFile = "./"  + outputTempDir2 + "/part-r-00000";
        readFrequentItemsFromFile(outputFile);
     
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
		System.out.println("\n============== All Frequent Items ============\n " + allFrequentItems);
}
}