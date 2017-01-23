/*
 * Project: Big Data Project 2016/17
 * Group Members: M2016201, M2016013, M2016014
 * Objective: Generates association rules from a data set using Apriori algorithm and map reduce
 */
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
import java.util.*; 
 
public class Apriori { 
  /* ========================
   * User Variables
   * ======================== */
  public static double confidence = 0.05; 
  public static int supportFrequency = 100;
     
  // Input/Output Folders 
  public static String inputFile = "data_for_project.txt"; 
  public static String outputFolder = "mapreduce"; 
  public static String associationRulesOutput = "Association Rules.txt";
  
  /* ========================
   * Global Variables
   * ======================== */
  public static String outputFile; 
  public static int total_read; 
  public static int passNumber; 
  public static String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new java.util.Date()); 
   
  // Data Structures to store frequent item set data 
  public static ArrayList<String> freqItems, candidateItems; 
  public static ArrayList<Map<String, Integer>> allFrequentItemsets = new ArrayList<Map<String, Integer>>(); 

  /*
   * Accepts: Basket and items
   * Returns: True if items are in the basket, False if not
   * Purpose: Used to generate candidate item sets from data from previous reducer based on Apriori Algorithm
   */  
  private static boolean isItemInBasket(String items, String basket) { 
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
  
  /*
   * Accepts: No input, but reads data from freqItems, which is populated using readFrequentItemsFromFile function
   * Returns: Nothing 
   * Purpose: Creates candidate item set of pairs from the frequent single items. Data used for 2nd Pass of Map Reduce
   */ 
  private static void getFrequentPairs() { 
    candidateItems = new ArrayList<String>(); 
    for(int i=0; i<freqItems.size(); i++) { 
      for(int j=i+1; j<freqItems.size(); j++) { 
        String line=freqItems.get(i) + "," + freqItems.get(j); 
        candidateItems.add(line); 
      } 
    } 
  } 
   
  /*
   * Accepts: Input filename to read reducer data from (assumes tab delimited data of 2 columns)
   * Returns: None
   * Purpose: 
   * Reads reducer data from previous pass and stores the data in 2 data structures:
   * 1. freqItems - only stores the string value, not the frequency count. To be used for generating candidate item sets
   * 2. allFrequentItemsets - stores both string and frequency. Used to generate association rules.
   */
  private static void readFrequentItemsFromFile(String filename) { 
    freqItems = new ArrayList<String>(); 
    Map<String, Integer> freqItemset = new HashMap<String, Integer>(); 

    try { 
      BufferedReader bReader = new BufferedReader(new FileReader(filename)); 
      String line; 

      while ((line = bReader.readLine()) != null) { 
        String itemName = new String(); 
        Integer itemFreq; 
        itemName = line.split("\\t")[0]; 
        itemFreq = Integer.valueOf(line.split("\\t")[1]); 
        freqItems.add(itemName); 
        freqItemset.put(itemName, itemFreq); 
      } 
      bReader.close(); 

    } 
    catch (Exception e) { 
      System.out.println("Error reading input file."); 
    } 
 
    allFrequentItemsets.add(freqItemset); 
  } 

  /*
   * Accepts: A string array of items
   * Returns: A string of the items joined together with a , delimiter
   * Purpose: Sorts and removes duplicates, and joins items into an item set
   */
  public static String combineStrings(String[] strings) { 
    Arrays.sort(strings); 
    Arrays.stream(strings).distinct(); 
    return String.join(",", strings); 
  } 

  /*
   * Accepts: None, but reads data from the freqItems data structure, which contains data from the previous reducer run
   * Returns: None, but creates candidate pairs for next mapper run
   * Purpose: Implements Apriori Algorithm to create all potential candidate item sets of n+1, to be mapped for frequency support count
   */
  private static void getCandidateItems() { 
    candidateItems = new ArrayList<String>(); 
    for(int i=0; i<freqItems.size(); i++) { 
      String line = freqItems.get(i); 
      String[] items = line.split(","); 
      String firstWord = items[0]; 
      String subString = combineStrings(Arrays.copyOfRange(items, 1, items.length)); 

      for(int j=i+1; j<freqItems.size(); j++) { 
        String tempItemset = freqItems.get(j); 
        if(isItemInBasket(subString, tempItemset)) { 
          String lastWord = tempItemset.split(",")[items.length-1]; 

          //Check if first and last word is in the code 
          Boolean isCandidate = false; 
          for(int k=0; k<freqItems.size(); k++) {   
            if(isItemInBasket(firstWord, freqItems.get(k)) && isItemInBasket(lastWord,freqItems.get(k))) { 
              isCandidate = true; 
            } 
          } 
           
          if(isCandidate) { 
            String[] newSet = {firstWord,tempItemset}; 
            String newCandidate = combineStrings(newSet); 
            if(!candidateItems.contains(newCandidate)) { 
              candidateItems.add(newCandidate); 
            } 
          } 
        } 
      } 
    } 
  } 

  /*
   * Accepts: None, but reads all the parameters needed for map reduce (Passnum, total_read, supportFrequency, candidateitems)
   * Returns: None, but writes to file the map reduce output
   * Purpose: Controls the execution of the map reduce jobs.
   */
  private static void findFrequentItems() { 
    try { 
      Configuration conf = new Configuration(); 

      //Find Frequent Items 
      passNumber = 1; 

			conf.setInt("passNumber", passNumber);
			conf.setInt("total_read", total_read);
			conf.setInt("supportFrequency", supportFrequency);

      String outputDir = outputFolder + "/" + timeStamp + "/" + passNumber; 
      boolean success = createJob(inputFile, outputDir,"apriori_map.aprioriMapper","apriori_map.aprioriReducer",conf); 
      outputFile = "./"  + outputDir + "/part-r-00000"; 
      if(success == true){ 
               System.err.println("\nPass 1 Completed Succesfully\n"); 

        } 
      readFrequentItemsFromFile(outputFile); 
      getCandidateItems(); 

      //Find k-item sets 
      passNumber = 2; 
      total_read = 0;
      getFrequentPairs(); 
      while(candidateItems.size() > 0) { 
        //Map Reduce 
		conf.setInt("passNumber", passNumber);
		conf.setInt("total_read", total_read);
		
		//Transform from ArrayList to Strings separated by a delimiter
        Object[] candidateItemsObj = candidateItems.toArray();               
        
        //Second Step: convert Object array to String array
        String[] candidateItemsArr = Arrays.copyOf(candidateItemsObj, candidateItemsObj.length, String[].class);
		String candidateItemsString = String.join(";:", candidateItemsArr);			
		conf.set("candidateItems", candidateItemsString);

        outputDir = outputFolder + "/" + timeStamp + "/" + passNumber;   
        success = createJob(inputFile, outputDir,"apriori_map.aprioriMapper","apriori_map.aprioriReducer",conf); 
        if(success == true) 
              System.err.println("\nPass " + passNumber + " Completed Succesfully\n"); 
        else  
          System.err.println("\nPass " + passNumber + " Failed \n"); 

        //Read new data 
        outputFile = "./"  + outputDir + "/part-r-00000"; 
        outputFile = "./"  + outputDir + "/part-r-00000"; 
        readFrequentItemsFromFile(outputFile); 
        getCandidateItems(); 
        passNumber += 1; 
		total_read = 0;
      }    
    }  
    catch (Exception e) { 
      e.printStackTrace(); 
    }   
  } 
 
  /*
   * Accepts: All parameters needed for a map reduce job
   * Returns: True, if job ran successfully; False, if job failed
   * Purpose: Executes map reduce jobs
   */
  public static boolean createJob(String input, String output,String aprioriMapper, String aprioriReducer,  Configuration conf){ 
    boolean success = false; 

    try { 
          @SuppressWarnings("deprecation") 
         Job job = new Job(conf, "aprior"); 
         job.setJarByClass(Apriori.class); //Tell hadoop the name of the class every cluster has to look for 
         Class cls_mapper = Class.forName(aprioriMapper); 
         Class cls_reducer = Class.forName(aprioriReducer); 
         job.setMapperClass(cls_mapper); //Set class that will be executed by the mapper 
         job.setReducerClass(cls_reducer); //Set the class that will be executed as the reducer 
         job.setOutputKeyClass(Text.class); //Set the class to be used as the key for outputting data to the user 
         job.setOutputValueClass(IntWritable.class); //Set class that will be used as the vaue for outputting data 
         FileInputFormat.addInputPath(job,  new Path(input)); //Get input file name 
         FileOutputFormat.setOutputPath(job, new Path(output)); 
         success = job.waitForCompletion(true); 
    }  
    catch (IOException | ClassNotFoundException | InterruptedException e) { 
      e.printStackTrace(); 
    }  
    return success; 
  } 

  /*
   * Accepts: None
   * Returns: None
   * Purpose: Controls the execution of the all the classes
   */
  public static void main(String[] args) throws InterruptedException, ClassNotFoundException { 
    findFrequentItems(); 
    AssociationRules rules = new AssociationRules(allFrequentItemsets, confidence);
    rules.displayAssociationRules();  // This displays the results in the console
    rules.writeAssociationRulesToFile(associationRulesOutput); // This writes the results to the specified file
  } 
}