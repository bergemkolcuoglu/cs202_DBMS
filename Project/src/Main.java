import java.sql.SQLException;
import java.util.Scanner;


public class Main {
	public static void main(String[] args) throws SQLException{
		DB db = new DB();
		MultipleConnection mc = new MultipleConnection();
		CSVConnection csvload = new CSVConnection();
		  
	        
		while(true){
			Scanner scanner = new Scanner(System.in);
			String val = scanner.nextLine();
			if(val.equals("SHOW TABLES")){
				System.out.println(db.getTables());
			}
			if(val.equals("EXIT")){
				db.closeConnection();
				mc.closeConnection();
				csvload.closeConnection();
			}
			if(val.equals("LOAD CSV")){
				csvload.addFarmers("farmers.csv");
				csvload.addProducts("products.csv");
				csvload.addMarkets("markets.csv");
				csvload.addProduces("produces.csv");
				csvload.registerProducts("registers.csv");
				csvload.addBuys("buys.csv");
			}
			if(val.equals("query5")){
				System.out.println(db.query5());
			}
			if(val.equals("query1")){
				System.out.println(db.query1());
			}
			if(val.equals("query4")){
				System.out.println(db.query4());
			}
////////////////////multiple inserts ////////////////
			 if(val.startsWith("ADD FARMERs") && val.endsWith(")")){
				 	val = val.substring(11,val.length());
					mc.addFarmers(val);
			 }
			 else if(val.startsWith("ADD PRODUCTs") && val.endsWith(")")){
					val = val.substring(12,val.length());
					mc.addProducts(val);
				 }
			 else if(val.startsWith("ADD MARKETs") && val.endsWith(")")){
					val = val.substring(11,val.length());
					mc.addMarkets(val);
				 }
			 else if(val.startsWith("REGISTER PRODUCTs") && val.endsWith(")")){
					val = val.substring(17,val.length());
					mc.registerProducts(val);
				 }
			else if(val.contains("ADD FARMER") && val.endsWith(")")){
				val = val.substring(11,val.length()-1);
				String[] farmer = val.split(",",7);
				try {
					db.addFarmer(farmer[0],farmer[1],farmer[2],farmer[3],farmer[4],farmer[5],farmer[6]);
					db.commitChanges();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	
			}
			else if(val.contains("ADD PRODUCT") && val.endsWith(")")){
				val = val.substring(12,val.length()-1);
				String[] product = val.split(",",6);
				db.addProduct(product[0],product[1],product[2],product[3],product[4],product[5]);
				db.commitChanges();
			}
			else if(val.contains("ADD MARKET") && val.endsWith(")")){
				val = val.substring(11,val.length()-1);
				String[] market = val.split(",",6);
				db.addMarket(market[0],market[1],market[2],market[3],market[4],market[5]);
				db.commitChanges();
			}
			else if(val.contains("REGISTER PRODUCT") && val.endsWith(")")){
				val = val.substring(17,val.length()-1);
				String[] market = val.split(",",6);
				db.registerProduct(market[0],market[1],market[2],market[3],market[4],market[5]);
				db.commitChanges();
			}
		}
	}
}
