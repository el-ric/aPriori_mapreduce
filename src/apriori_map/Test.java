package apriori_map;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class Test {
	public static String inputFile = "data_for_project.txt";
	public static String itemSet = "A,B,C,D";
	public static ArrayList<String[]> associcationRules;

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
	
	private static String getAssociation(String itemset, String basket) {
		String[] items  = itemset.split(",");
		String[] basketItems = basket.split(",");
		String remainingItems = new String();
		
		remainingItems = "";
		for(int i=0; i<basketItems.length; i++) {
			boolean inItemset = false;
			for(int j=0; j<items.length; j++) {
				if(basketItems[i].equals(items[j]))
					inItemset = true;
			}
			if(!inItemset) {
				if(remainingItems.equals(""))
					remainingItems = basketItems[i];
				else
					remainingItems = remainingItems + "," + basketItems[i];
			}
		}
		
		String[] sorted = remainingItems.split(",");
		Arrays.sort(sorted);
		remainingItems = String.join(",", sorted);
		
		return remainingItems;
	}
	
	public static ArrayList<String[]> getCombinations(String string, int size) {
		ArrayList<String[]> assRules = new ArrayList<String[]>();
		String[] items  = string.split(",");
		
		for (int i=0; i+size<=items.length; i++) {
			for(int j=i+1; j<items.length; j++) {
				String comb = new String();
				comb = items[i];
				for(int k=0; k<size-1; k++)
					if(j+k < items.length) {
						comb = comb + "," + items[j+k];
					}
					else if(i+k+size <= items.length) {
						comb = comb + "," + items[k];
					}
				if(comb.length() == 2*size-1) {
					String associatedItems = getAssociation(comb, string);
					String[] association = new String[2];
					association[0] = comb;
					association[1] = associatedItems;
					assRules.add(association);
				}
			}
		}
		return assRules;
	}
	
	private static int checkResults(String string) {
		int count=0;
		try {
			BufferedReader bReader = new BufferedReader(new FileReader(inputFile));
			String line;
			while ((line = bReader.readLine()) != null) {
				String[] items = string.split(",");
				
				Boolean flag = true;
				for(int k=0; k<items.length; k++) {	
					if(!isItemInBasket(items[k], line))
						flag = false;
				}
				
				if(flag)
					count += 1;
			}
			bReader.close();
		}
		catch (Exception e) {
			System.out.println("Error reading input file.");
		}
		return count;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//System.out.println(checkResults("butter,other vegetables,whole milk"));
		ArrayList<String[]> assRules = new ArrayList<String[]>();
		assRules = getCombinations("A,B,C,D", 2);
		
		for(int i=0; i<assRules.size(); i++) {
			System.out.println(assRules.get(i)[0] + " -> " + assRules.get(i)[1]);
		}
		
	}

}
