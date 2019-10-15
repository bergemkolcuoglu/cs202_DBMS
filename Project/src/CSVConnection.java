import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class CSVConnection {
	private static final String DELIMITER = ";";
	private static final String COMMA_DELIMITER = ",";
	DB db = null;
	
	public CSVConnection(){
		db = new DB();
	}
	 public void  closeConnection(){
	    	db.closeConnection();
	    }
	public void addFarmers(String path){
		List<List<String>> records = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(new FileReader(path))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		        String[] values = line.split(DELIMITER);
		        records.add(Arrays.asList(values));
		    }
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		records.remove(0);
		Savepoint spt1 = db.setSavePoint("svpt1");
		for(List<String> record: records){
			System.out.println(record.toString());
			try {
				db.addFarmer(record.get(0), record.get(1), record.get(2), record.get(3), record.get(4), record.get(5), record.get(6));
				db.commitChanges();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				db.removeChanges(spt1);
				System.out.println("Changes removed");
				e.printStackTrace();
			}
		}
	}
	public void addProducts(String path){
		List<List<String>> records = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(new FileReader(path))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		        String[] values = line.split(COMMA_DELIMITER);
		        records.add(Arrays.asList(values));
		    }
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		records.remove(0);
		Savepoint spt1 = db.setSavePoint("svpt2");
		for(List<String> record: records){
			System.out.println(record.toString());
			try {
				db.addProduct(record.get(0), record.get(1), record.get(2), record.get(3), record.get(4), record.get(5));
				db.commitChanges();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				db.removeChanges(spt1);
				System.out.println("Changes removed");
				e.printStackTrace();
			}
		}
	}
	public void addMarkets(String path){
		List<List<String>> records = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(new FileReader(path))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		        String[] values = line.split(DELIMITER);
		        records.add(Arrays.asList(values));
		    }
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		records.remove(0);
		Savepoint spt1 = db.setSavePoint("svpt3");
		for(List<String> record: records){
			System.out.println(record.toString());
			try {
				db.addMarket(record.get(0), record.get(1), record.get(2), record.get(3), record.get(4), record.get(5));
				db.commitChanges();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				db.removeChanges(spt1);
				System.out.println("Changes removed");
				e.printStackTrace();
			}
		}
	}
	public void addProduces(String path){
		List<List<String>> records = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(new FileReader(path))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		        String[] values = line.split(DELIMITER);
		        records.add(Arrays.asList(values));
		    }
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		records.remove(0);
		Savepoint spt1 = db.setSavePoint("svpt4");
		for(List<String> record: records){
			System.out.println(record.toString());
			try {
				db.addProduce(record.get(0), record.get(1), record.get(2), record.get(3), record.get(4));
				db.commitChanges();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				db.removeChanges(spt1);
				System.out.println("Changes removed");
				e.printStackTrace();
			}
		}
	}
	public void registerProducts(String path){
		List<List<String>> records = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(new FileReader(path))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		        String[] values = line.split(DELIMITER);
		        records.add(Arrays.asList(values));
		    }
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		records.remove(0);
		Savepoint spt1 = db.setSavePoint("svpt5");
		for(List<String> record: records){
			System.out.println(record.toString());
			try {
				db.registerProduct(record.get(0), record.get(1), record.get(2), record.get(3), record.get(4), record.get(5));
				db.commitChanges();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				db.removeChanges(spt1);
				System.out.println("Changes removed");
				e.printStackTrace();
			}
		}
	}
	public void addBuys(String path){
		List<List<String>> records = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(new FileReader(path))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		        String[] values = line.split(DELIMITER);
		        records.add(Arrays.asList(values));
		    }
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		records.remove(0);
		Savepoint spt1 = db.setSavePoint("svpt6");
		for(List<String> record: records){
			System.out.println(record.toString());
			try {
				db.addBuy(record.get(0), record.get(1), record.get(2), record.get(3), record.get(4), record.get(5), record.get(6));
				db.commitChanges();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				db.removeChanges(spt1);
				System.out.println("Changes removed");
				e.printStackTrace();
			}
		}
	}
}
