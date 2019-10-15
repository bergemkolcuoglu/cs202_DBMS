
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;

public class DB {
	 Connection con = null;
    public  DB() {
       
        try {
            Class.forName("org.postgresql.Driver");
            con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "admin");
            con.setAutoCommit(false);
            System.out.println("Connection initalized");
        } catch (Exception e) {
            System.out.println(e);
        }
    }
    public void  closeConnection(){
    	try {
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    public void commitChanges() throws SQLException{
    	con.commit();
    }
    public void removeChanges(Savepoint sp){
    	try {
			con.rollback(sp);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    public Savepoint setSavePoint(String sName){
    	Savepoint s= null;
    	try {
			s = con.setSavepoint(sName);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return s;
    }
    public String getTables(){
    	String result = null;
        try {
             Statement statement = con.createStatement();
            String sql = "SELECT * FROM information_schema.tables " + "WHERE table_schema = '" +
            "public'";		
            ResultSet rs  = statement.executeQuery(sql);
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            for (int column = 1; column <= columnCount; column++) {
                result += metaData.getColumnName(column) + " ";
            }
            result += "\n";
            while (rs.next()) {
                for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
                    result += rs.getString(columnIndex) + " "; 
                }
               result += "\n";
            }
            statement.close();
          
        
        } catch (SQLException e) {
        	e.printStackTrace();
        	  try {
				con.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
        }
      
    	return result;
    }
    /////////////adfarmer////////////////
    public void addFarmer(String fname,String flastname,String adress,String fzipcode,String fcity,String phoneNums,String emails) throws SQLException{
    	String[] email = emails.split("\\|");
    	String[] phones = phoneNums.split("\\|");
    	 try {
             Statement statement = con.createStatement();
            String sql1 = "INSERT INTO FARMER VALUES('"+
             fname+"','" + flastname +"','" + fcity +"'," +Integer.parseInt(fzipcode.trim()) +",'" + adress+"')";
          
            statement.executeUpdate(sql1);
            ///// ýnsert phones////////////////////
            for(String phone : phones){
            	System.out.println(phone);
            	  String sql2 = "INSERT INTO Phone VALUES('"+
                          fname+"','" + flastname +"'," +Long.parseLong(phone.trim())+ ")" ;	
            	  statement.executeUpdate(sql2);
            	  
            }
        ///// ýnsert email////////////////////
            for(String em : email){
          	  String sql3 = "INSERT INTO email VALUES('"+
                        fname+"','" + flastname +"','" + em + "')" ;	
          	  statement.executeUpdate(sql3);
          }
            statement.close();
          
        
        } catch (SQLException e) {
        	e.printStackTrace();
        	  try {
				con.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
        }
    }
    ////////////////// add product order name, p_date, h_date, hardness, altitude, min_temp////////////
    public void addProduct(String name, String p_date, String h_date, String hardness, String altitude, String min_temp){
    	  Statement statement = null;
		try {
			statement = con.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
          String sql1 = "INSERT INTO PRODUCT VALUES('"+
           name+"','" + hardness +"'," + Integer.parseInt(min_temp.trim())  +"," +Integer.parseInt(altitude.trim()) +",'" + p_date+"','"+h_date+"')";
        
          try {
			statement.executeUpdate(sql1);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    /////////////////// ADD MARKET(name, address, zip_code, city, phone, budget)///////////
    public void addMarket(String name,String address, String zip_code, String city, String phone, String budget){
   	  Statement statement = null;
  		try {
  			statement = con.createStatement();
  		} catch (SQLException e) {
  			// TODO Auto-generated catch block
  			e.printStackTrace();
  		}
            String sql1 = "INSERT INTO MARKET VALUES('"+
             name+"'," + Integer.parseInt(zip_code.trim()) +"," + Long.parseLong(budget.trim())  +",'" +city +"','" + address+"',"+Long.parseLong(phone.trim())+")";
          
            try {
  			statement.executeUpdate(sql1);
  		} catch (SQLException e) {
  			// TODO Auto-generated catch block
  			e.printStackTrace();
  		}
    }
    ///////////////fname	flastnam	pname	amount	year

    public void addProduce(String fname, String flastname, String pname, String amount, String year){
    	  Statement statement = null;
    		try {
    			statement = con.createStatement();
    		} catch (SQLException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}
              String sql1 = "INSERT INTO PRODUCE VALUES('"+
               fname+"','" + flastname +"','" + pname  +"'," +Integer.parseInt(year.trim())+","+ Integer.parseInt(amount.trim())+")";
            
              try {
    			statement.executeUpdate(sql1);
    		} catch (SQLException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}
      }
    //////////////////Fname	Flastname	pname	amount	price	IBAN
    public void registerProduct(String fname, String flastname, String pname, String amount, String price,String IBAN){
  	  Statement statement = null;
  	price = price.trim().replaceAll(",",".");
  		try {
  			statement = con.createStatement();
  		} catch (SQLException e) {
  			// TODO Auto-generated catch block
  			e.printStackTrace();
  		}
            String sql1 = "INSERT INTO REGISTER VALUES('"+
             fname+"','" + flastname +"','" + pname  +"'," +Integer.parseInt(amount.trim())+","+ Double.parseDouble(price)+",'"+IBAN+"')";
          
            try {
  			statement.executeUpdate(sql1);
  		} catch (SQLException e) {
  			// TODO Auto-generated catch block
  			e.printStackTrace();
  		}
    }
    ////////////////Fname	Flastname	pname	mname	maddress	amount	creditcard
    public void addBuy(String fname, String flastname,String pname, String mname, String maddress, String amount,String creditcard){
    	 Statement statement = null;
   		try {
   			statement = con.createStatement();
   		} catch (SQLException e) {
   			// TODO Auto-generated catch block
   			e.printStackTrace();
   		}
             String sql1 = "INSERT INTO BUY VALUES('"+
              fname+"','" + flastname +"','" + pname  +"','" +mname+"','"+ maddress+"',"+Integer.parseInt(amount.trim())+",'"+ creditcard +"')";
           
             try {
   			statement.executeUpdate(sql1);
   		} catch (SQLException e) {
   			// TODO Auto-generated catch block
   			e.printStackTrace();
   		}
    }
    public  ArrayList<String>  getProducts(){
    ArrayList<String> arr = null;
   	 ResultSet rs = null;
       try {
            Statement statement = con.createStatement();
           String sql = "SELECT pname  FROM PRODUCT";		
            rs  = statement.executeQuery(sql);
            arr = new ArrayList<String>();
            while (rs.next()) {
               arr.add(rs.getString("pname"));
            }
           statement.close();
         
       
       } catch (SQLException e) {
       	e.printStackTrace();
       }
     
   	return arr;
   }
   
    //////////////Find the names of farmers who produces most for each product.
    public String query1() throws SQLException{
    ArrayList<String> products = getProducts();
    String result = null;
    for(int i=0; i< products.size(); i++){
    	 try {
             Statement statement = con.createStatement();
            String sql = "SELECT pname ,fname , flastname ,total FROM (SELECT pname,fname, flastname, SUM(pquantity) AS total FROM Produce P "+
            "WHERE P.pname ='" + products.get(i) + "' GROUP BY pname,fname, flastname ORDER BY total ) AS extracted WHERE total = (SELECT MAX(total) FROM "+
            "(SELECT pname,fname, flastname, SUM(pquantity) AS total FROM Produce P WHERE P.pname = '" + products.get(i) + "'  GROUP BY pname,fname, flastname ORDER BY total ) AS extracted2)";		
            ResultSet rs  = statement.executeQuery(sql);
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (rs.next()) {
                for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
                    result += rs.getString(columnIndex) + " "; 
                }
               result += "\n";
            }
            statement.close();
          
        
        } catch (SQLException e) {
        	e.printStackTrace();
        	  try {
				con.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
        }
    }
    	
       
      
    	return result.replaceAll("null", "");
    	
    }
    public  ArrayList<String>  getCities(){
        ArrayList<String> arr = null;
       	 ResultSet rs = null;
           try {
                Statement statement = con.createStatement();
               String sql = "SELECT mcity  FROM MARKET GROUP BY mcity";		
                rs  = statement.executeQuery(sql);
                arr = new ArrayList<String>();
                while (rs.next()) {
                   arr.add(rs.getString("mcity"));
                }
               statement.close();
             
           
           } catch (SQLException e) {
           	e.printStackTrace();
           }
         
       	return arr;
       }
    //////////Find the names of markets with the biggest budget in each city.
    public String query4(){
        ArrayList<String> cities = getCities();
        String result = null;
        for(int i=0; i< cities.size(); i++){
        	 try {
                 Statement statement = con.createStatement();
                String sql = "SELECT mcity, mname , maxbud FROM (SELECT mcity, mname , MAX(mbudget) AS maxbud FROM MARKET M "+
                		"WHERE M.mcity = '" + cities.get(i) +  "' GROUP BY mcity, mname ) AS extracted1 WHERE maxbud = (SELECT MAX(maxbud) FROM "+
                		"(SELECT mcity, mname , MAX(mbudget)  AS maxbud FROM MARKET M WHERE  M.mcity = '" + cities.get(i) +  "'  GROUP BY mcity, mname ) AS extracted2)";		
                ResultSet rs  = statement.executeQuery(sql);
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                while (rs.next()) {
                    for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
                        result += rs.getString(columnIndex) + " "; 
                    }
                   result += "\n";
                }
                statement.close();
              
            
            } catch (SQLException e) {
            	e.printStackTrace();
            	  try {
    				con.rollback();
    			} catch (SQLException e1) {
    				// TODO Auto-generated catch block
    				e1.printStackTrace();
    			}
            }
        }
        	
           
          
        return result.replaceAll("null", "");
    }
    ///////////////////Find the names of farmers who produces most for each product.////////
    public String query5(){
    	String result = null;
        try {
             Statement statement = con.createStatement();
            String sql = "SELECT COUNT(*)" +
            			" FROM FARMER FULL OUTER JOIN  MARKET ON (FARMER.fname = MARKET.mname)";		
            ResultSet rs  = statement.executeQuery(sql);
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            for (int column = 1; column <= columnCount; column++) {
                result += metaData.getColumnName(column) + " ";
            }
            result += "\n";
            while (rs.next()) {
               
                for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
                    result += rs.getString(columnIndex) + " "; 
                }
               result += "\n";
            }
            statement.close();
          
        
        } catch (SQLException e) {
        	e.printStackTrace();
        	  try {
				con.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
        }
      
    	return result;
    }
 
}