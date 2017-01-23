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
  /* ==== USER INPUT ==== */ 
  public static double confidence = 0.05; 
  public static int supportFrequency = 100; 
  //public static double supportThreshold = 0.1; // Frequency as a fraction [Update checkFrequency() function accordingly] 
     
  /* ==== Input/Output Folders ==== */ 
  public static String inputFile = "data_for_project.txt"; 
  public static String outputFolder = "mapreduce"; 

  /* ==== Global Variables ==== */ 
  public static String outputFile; 
  public static int total_read; 
  public static int passNumber; 
  public static String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new java.util.Date()); 
   
  // Data Structures to store frequent itemset data 
  public static ArrayList<String> freqItems, candidateItems; 
  public static ArrayList<Map<String, Integer>> allFrequentItemsets = new ArrayList<Map<String, Integer>>(); 

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
 
  private static void getFrequentPairs() { 
    candidateItems = new ArrayList<String>(); 
    for(int i=0; i<freqItems.size(); i++) { 
      for(int j=i+1; j<freqItems.size(); j++) { 
        String line=freqItems.get(i) + "," + freqItems.get(j); 
        candidateItems.add(line); 
      } 
    } 
  } 
   
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

  public static String combineStrings(String[] strings) { 
    Arrays.sort(strings); 
    Arrays.stream(strings).distinct(); 
    return String.join(",", strings); 
  } 

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
 
  public static boolean checkFrequency(int sum, int total) { 
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

  public static void main(String[] args) throws InterruptedException, ClassNotFoundException { 
    findFrequentItems(); 
    AssociationRules rules = new AssociationRules(allFrequentItemsets, confidence); 
    rules.displayAssociationRules(); 
  } 
}