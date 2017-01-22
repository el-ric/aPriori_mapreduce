package apriori_map;

import java.io.BufferedReader;
import java.io.FileReader;

public class Test {
	public static String inputFile = "data_for_project.txt";

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
		System.out.println(checkResults("butter,other vegetables,whole milk"));
	}

}
