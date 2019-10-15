import java.sql.SQLException;
import java.sql.Savepoint;


public class MultipleConnection {
	
	DB db = null;
	public MultipleConnection(){
		db = new DB();
	}
	 public void  closeConnection(){
	    	db.closeConnection();
	    }
	public void addFarmers(String multipleCommands) throws SQLException{
		Savepoint spt1 = db.setSavePoint("svpt1");
		String[] commands = multipleCommands.split(":");
		for(String command: commands){
			command = command.substring(1,command.length()-1);
			System.out.println(command);
			String[] column = command.split(",");
			try {
				db.addFarmer(column[0], column[1], column[2], column[3], column[4], column[5], column[6]);
				db.commitChanges();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				db.removeChanges(spt1);
				db.commitChanges();
				e.printStackTrace();
			}
		}
	}
	public void addProducts(String multipleCommands) throws SQLException{
		Savepoint spt1 = db.setSavePoint("svpt1");
		String[] commands = multipleCommands.split(":");
		for(String command: commands){
			command = command.substring(1,command.length()-1);
			System.out.println(command);
			String[] column = command.split(",");
			try {
				db.addProduct(column[0], column[1], column[2], column[3], column[4], column[5]);
				db.commitChanges();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				db.removeChanges(spt1);
				db.commitChanges();
				e.printStackTrace();
			}
		}
	}
	public void addMarkets(String multipleCommands) throws SQLException{
		Savepoint spt1 = db.setSavePoint("svpt1");
		String[] commands = multipleCommands.split(":");
		for(String command: commands){
			command = command.substring(1,command.length()-1);
			System.out.println(command);
			String[] column = command.split(",");
			try {
				db.addMarket(column[0], column[1], column[2], column[3], column[4], column[5]);
				db.commitChanges();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				db.removeChanges(spt1);
				db.commitChanges();
				e.printStackTrace();
			}
		}
	}
	public void registerProducts(String multipleCommands) throws SQLException{
		Savepoint spt1 = db.setSavePoint("svpt1");
		String[] commands = multipleCommands.split(":");
		for(String command: commands){
			command = command.substring(1,command.length()-1);
			System.out.println(command);
			String[] column = command.split(",");
			try {
				db.registerProduct(column[0], column[1], column[2], column[3], column[4], column[5]);
				db.commitChanges();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				db.removeChanges(spt1);
				db.commitChanges();
				e.printStackTrace();
			}
		}
	}
	
}
