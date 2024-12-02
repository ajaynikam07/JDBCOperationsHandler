package sample;

import java.io.File;
import java.io.FileInputStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Types;
import java.util.InputMismatchException;
import java.util.Scanner;
  
public class AllJdbcOperation 
{
	static Scanner sc=new Scanner(System.in);
	static Connection con=Test.getconn();
	
	
	// this method retrive data based on user input 
	public void usingStatement()
	{
		try 
		{
			 
			Statement stm=con.createStatement();
			sc.nextLine();
			System.out.println("Enter Table name to Retrive data: ");
			String table=sc.nextLine();
			String sql="select * from "+table;
			ResultSet result=stm.executeQuery(sql);
			table=table.toUpperCase();
			System.out.println(table);
			switch (table)
			{
			case "EMP_INFO": 
			{
				System.out.println("Employees Data");
				while(result.next())
				{
					System.out.println(result.getInt(1)+"\t"+result.getString(2)+"\t"+result.getString(3)+
							"\t"+result.getLong(4)+"\t"+result.getLong(5));
					
				}
				
			}
			break;
			case "EMPLOYEE_INFO":
			{
				System.out.println("Employees Data");
				while(result.next())
				{
					System.out.println(result.getInt(1)+"\t"+result.getString(2)+"\t"+result.getLong(3)+
							"\t"+result.getString(4)+"\t"+result.getString(5)+"\t"+result.getLong(6));
					
				}
			}
			break;
			case "STUDENT":
			{
				System.out.println("Student data: ");
				while(result.next())
				{
					System.out.println(result.getInt(1)+"\t"+result.getString(2)+"\t"+result.getString(3));
				}
			}
			break;
			case "BANK":
			{
				System.out.println("Customer Data: ");
				while(result.next())
				{
					System.out.println(result.getInt(1)+"\t"+result.getString(2)+"\t"+result.getLong(3)+"\t"+result.getString(4));
				}
			}
			break;
			default:
				 System.err.println("Table Not found in the database");
			}
			
			 
			 
			
			
		} catch (Exception e) {
			 e.printStackTrace();
		}
	}
	
	// This method inserting data into table...
	public void insertingData()
	{
		try {
		Connection con=Test.getconn();
		Statement stm=con.createStatement();
		System.out.println("Enter Table name to Insert data: ");
		String table=sc.next();
//		sc.nextLine();
		String sql="select * from "+table;
		table=table.toUpperCase();
		System.out.println(table);
		
		switch (table)
		{
		case "EMP_INFO": 
		{
			sc.nextLine();
			 PreparedStatement ps=con.prepareStatement("Insert into EMP_INFO values (?,?,?,?,?)");
			 System.out.println("Enter emp id: ");
			 int id=Integer.parseInt(sc.nextLine());
			 
			 System.out.println("Enter emp name: ");
			 String name=sc.nextLine();
			 
			 System.out.println("Enter emp Address: ");
			 String address=sc.nextLine();
			 
			 System.out.println("Enter emp salary: ");
			 long salary=Long.parseLong(sc.nextLine());
			 
			 System.out.println("Enter emp PhoneNO: ");
			 long phono=Long.parseLong(sc.nextLine());
			 
			 ps.setInt(1, id);
			 ps.setString(2, name);
			 ps.setString(3, address);
			 ps.setLong(4, salary);
			 ps.setLong(5, phono);
			 
			 int k=ps.executeUpdate();
			 
			 if(k>0)
			 {
				 System.out.println("Data inserted Successful...");
			 }
			 
			
		}
		break;
		
		case "EMPLOYEE_INFO":
		{
			 sc.nextLine();
			 
			 System.out.println("Enter emp id: ");
			 int id=Integer.parseInt(sc.nextLine());
			 
			 System.out.println("Enter emp name: ");
			 String name=sc.nextLine();
			 
			 System.out.println("Enter emp Address: ");
			 String address=sc.nextLine();
			 
			 System.out.println("Enter emp salary: ");
			 long salary=Long.parseLong(sc.nextLine());
			 
			 System.out.println("Enter emp email-id: ");
			 String email=sc.nextLine();
			 
			 System.out.println("Enter emp PhoneNO: ");
			 long phono=Long.parseLong(sc.nextLine());
			 
			 int k=stm.executeUpdate("insert into employee_info values ("+id+",'"+name+"',"+salary+",'"+address+"','"+email+"',"+phono+")");
			 
			 if(k>0)
			 {
				 System.out.println("Data Inserted...");
			 }
			 
			 
		}
		break;
		case "STUDENT":
		{
			 sc.nextLine();
			 PreparedStatement ps=con.prepareStatement("Insert into student values (?,?,?)");
			 System.out.println("Enter student id: ");
			 int id=Integer.parseInt(sc.nextLine());
			 
			 System.out.println("Enter student name: ");
			 String name=sc.nextLine();
			 
			 System.out.println("Enter studnet address: ");
			 String address=sc.nextLine();
			 
			 ps.setInt(1, id);
			 ps.setString(2, name);
			 ps.setString(3, address);
			 
			 int k=ps.executeUpdate();
			 
			 if(k>0)
			 {
				 System.out.println("Data inserted successfuly");
			 }
			
		}
		break;
		case "BANK":
		{
			 sc.nextLine();
			 
			 System.out.println("Enter account number: ");
			 long accno=Long.parseLong(sc.nextLine());
			 
			 System.out.println("Enter customer name: ");
			 String name=sc.nextLine();
			 
			 System.out.println("Enter Bank Balance: ");
			 long balance=Long.parseLong(sc.nextLine());
			 
			 System.out.println("Enter account type: ");
			 String type=sc.nextLine();
			 
			 int rs=stm.executeUpdate("insert into bank values ("+accno+",'"+name+"',"+balance+",'"+type+"')");
			 
			 if(rs>0)
			 {
				 System.out.println("Data Inserted successfully");
			 }
		}
		break;
		
		default:
			 System.err.println("Table Not found in the database");
		}	
		
	} catch (Exception e) {
		 e.printStackTrace();
	}
	}
	
	
	
	public void createtable()
	{
		try
		{
			sc.nextLine();
			Statement stm=con.createStatement();
			System.out.println("Enter create table query with datatypess.");
			String query=sc.nextLine();
			
			int k=stm.executeUpdate(query);
			
			if(k>0)
			{
				System.out.println("Table Created Successfully");
			}
			
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	// This method insert insert into Insert image into the database
	public void insertImage()
	{
		try 
		{
			
			PreparedStatement ps=con.prepareStatement("Insert into player values(?,?,?,?)");
			sc.nextLine();
			System.out.println("Enter the player Id: ");
			int id =Integer.parseInt(sc.nextLine());
			
			System.out.println("Enter the player Name: ");
			String name=sc.nextLine();
			
			System.out.println("Enter Player DOB: ");
			String dob=sc.nextLine();
			
			System.out.println("Enter the player Photo Path: ");
			String path=sc.nextLine();
			
			File Fe=new File(path);
			
			if(Fe.exists())
			{
				FileInputStream io= new FileInputStream(Fe);
				ps.setInt(1, id);
				ps.setString(2, name);
				ps.setString(3,dob);
				ps.setBinaryStream(4, io);
				int k=ps.executeUpdate();
				
				if(k>0)
				{
					System.out.println("Data inserted Successfully");
				}
				
				
			}
			else
			{
				System.err.println("Path Not exist ");
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	// This method will insert video into the database.
	public void InsertVideo()
	{
		try 
		{
			PreparedStatement ps = con.prepareStatement("Insert into video values(?,?)");
			
			System.out.println("Enter Video Id : ");
			int id=Integer.parseInt(sc.nextLine());
			
			System.out.println("Enter video path: ");
			String path=sc.nextLine();
			
			File Fe=new File(path);
			
			if(Fe.exists())
			{
				FileInputStream io= new FileInputStream(Fe);
				
				ps.setInt(1, id);
				ps.setBinaryStream(2, io);
				
				int k=ps.executeUpdate();
				
				if(k>0)
				{
					System.out.println("Video Inserted Succesfully");
				}
			}
			else
			{
				System.err.println("Path Not Exist");
			}

		} catch (Exception e) 
		{
			 e.printStackTrace();
		}
	}
	
	public void RetrieveOnCustomerId() {
	    try {
	    	sc.nextLine();
	        System.out.println("Enter id: ");
	        int id = Integer.parseInt(sc.nextLine());
	        
	        PreparedStatement ps = con.prepareStatement("SELECT * FROM student WHERE id = ?");
	        PreparedStatement ps2 = con.prepareStatement("SELECT * FROM employee_info WHERE empid = ?");
	        
	        ps.setInt(1, id);
	        ps2.setInt(1, id);
	        
	        ResultSet rs = ps.executeQuery();
	        ResultSet rs1 = ps2.executeQuery();
	        
	        // Process student data
	        while (rs.next()) {
	            System.out.println("This id of Student: " + rs.getString(2));
	            System.out.println("Here Data");
	            System.out.println(rs.getInt(1) + "\t" + rs.getString(2) + "\t" + rs.getString(3));
	        }
	        
	        // Process employee data
	        while (rs1.next()) {
	            System.out.println("This id of Employee: " + rs1.getString(2));
	            System.out.println("Here Data");
	            System.out.println(rs1.getInt(1) + "\t" + rs1.getString(2) + "\t" + rs1.getString(3)+"\t"+rs1.getString(4)+"\t"+rs1.getString(5)+"\t"+rs1.getString(6));
	        }
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	}

	public void StroedProcedure()
	{
		 try
		 {
			 sc.nextLine();
			 CallableStatement cs = con.prepareCall("{call insertdetails(?,?,?,?,?,?,?,?,?)}");

				System.out.println("------Custome Details--");
				System.out.println("Enter the customer id: ");
				String cid = sc.nextLine();

				System.out.println("Enter the Customer Name: ");
				String cname = sc.nextLine();

				System.out.println("Enter the customer Hno: ");
				String hno = sc.nextLine();

				System.out.println("Enter the Cust-SName:");
				String sName = sc.nextLine();

				System.out.println("Enter the Cust-City:");
				String city = sc.nextLine();

				System.out.println("Enter the Cust-State:");
				String state = sc.nextLine();

				System.out.println("Enter the Cust-PinCode:");
				int pinCode = Integer.parseInt(sc.nextLine());

				System.out.println("Enter the Cust-MailId:");
				String mId = sc.nextLine();

				System.out.println("Enter the Cust-PhoneNo:");
				long phNo = sc.nextLong();

				
	            cs.setString(1,cid);
	            cs.setString(2, cname);
	            cs.setString(3, hno);
	            cs.setString(4, sName);
	            cs.setString(5, city);
	            cs.setString(6, state);
	            cs.setInt(7, pinCode);
	            cs.setString(8, mId);
	            cs.setLong(9, phNo);
	            
	            cs.execute();
	            System.out.println("Customer Details Added Succesfully...!");
	             
		 }catch(Exception e)
		 {
			 e.printStackTrace();
		 }
	}
public void ExecuteFunction()
{
	try
	{
		CallableStatement cs=con.prepareCall
				("{call ?:= studentdata(?)}");
		
		System.out.println("Enter the student to retrive phoneNO: ");
		int sid=sc.nextInt();
		
		cs.setInt(2, sid);
		cs.registerOutParameter(1, Types.VARCHAR);
		
		cs.execute();
		System.out.println("Student id: "+sid);
		System.out.println("Student Name: "+cs.getString(1));
		
	}
	catch(SQLException e)
	{
		System.out.println("Data Not found");
	}
	catch (InputMismatchException e) {
		 System.err.println("Enter number only");
	}
	catch (Exception e) {
		e.printStackTrace();
	}
}

public void BatchProcessing()
{
	try {
		sc.nextLine();
		 Statement stm= con.createStatement();
		PreparedStatement ps=con.prepareStatement("select * from employee_info where empid=(?)");
		System.out.println("Inserting values in the table: ");
		System.out.println("Enter the student id: ");
		int id=Integer.parseInt(sc.nextLine());
		
		System.out.println("Enter Student name: ");
		String name=sc.nextLine();
		
		System.out.println("Enter student address: ");
		String address=sc.nextLine();
		
		stm.addBatch("insert into student values ("+id+",'"+name+"','"+address+"')");
		
		System.out.println("Enter the Employee id to update salary: ");
		int empid=Integer.parseInt(sc.nextLine());
		ps.setInt(1, empid);
		ResultSet rs=ps.executeQuery();
		
		while(rs.next())
		{
			System.out.println("Enter "+rs.getString(2)+" New salary: ");
			double salary=Double.parseDouble(sc.nextLine());
			
			stm.addBatch("update employee_info set empsalary ="+salary+" where empid="+empid);
			
			
		}
		stm.addBatch("DELETE FROM employee_info where empsalary =(select MAX(empsalary) from employee_info)");
		
		int k[] = stm.executeBatch();
		
		for (int i : k) {
			
			System.out.println(i+" Query executed");
		}
		
		stm.clearBatch();
	} catch (Exception e) {
		// TODO: handle exception
	}
	
}
//Trasaction management
public void TrsactionManagent()
{
	try
	{
		System.out.println("Commit Status: "+con.getAutoCommit());
		con.setAutoCommit(false);
		
		System.out.println("Commit Status:"+con.getAutoCommit() );
		
		//prepared statement to execute queres 
		PreparedStatement ps= con.prepareStatement
				("Select * from Bank where accno=?");
		
		PreparedStatement  ps1=con.prepareStatement("update Bank set balance =balance+? where accno=?");
		Savepoint sp=con.setSavepoint();
		
		System.out.println("Enter the Account No: ");
		Long cusacc=sc.nextLong();
		
		ps.setLong(1, cusacc);
		ResultSet rs1=ps.executeQuery();
		
		if(rs1.next())
		{
			float bal=rs1.getFloat(3);
			System.out.println("Enter the Benifeciery Accno: ");
			long accno=sc.nextLong();
			
			ps.setLong(1, accno);
			
			ResultSet rs2=ps.executeQuery();
			
			if(rs2.next())
			{
				System.out.println("Enter the amount to be Transfered: ");
				float amt=sc.nextFloat();
				
				if(amt<=bal)
				{
					ps1.setFloat(1, -amt);
					ps1.setLong(2, accno);
					int p=ps1.executeUpdate();
					
					ps1.setFloat(1, +amt);
					ps1.setLong(2,cusacc);
					int q=ps1.executeUpdate();
					
					if(p==1 && q==1)
					{
						con.commit();
						System.out.println("Trasaction Successfull...!");
					}
					else
					{
						con.rollback(sp);
						System.out.println("Trasaction Faild ..");
					}
				}
				else
				{
					System.out.println("Insufficient Fund ");
				}
				
			}
			else
			{
				System.out.println("Invalid Account number...!");
			}
		}
		else
		{
			System.out.println("Invalid bank Account number..");
		}
		
	}
	catch (Exception e) {
		e.printStackTrace();
	}
}
	public static void main(String[] args) 
	{
		AllJdbcOperation obj=new AllJdbcOperation();
			 
		while(true)
		{
			
			System.out.println("Enter Your choice : ");
			System.out.println("1 for retriving data of all tables");
			System.out.println("2 for Insert Data into tables");
			System.out.println("3 for create table and insert data in that table.");
			System.out.println("4 for Insert Image/ in the data");
			System.out.println("5 For Insert Video in the database:");
			System.out.println("6 for Retrive Data based on id : ");
			System.out.println("7 for execute a procedure");
			System.out.println("8 for execute a Function");
			System.out.println("9 for BatchProcessing");
			System.out.println("10 for Trasaction Management");
			int choice=sc.nextInt();
			
			switch(choice)
			{
			case 1:
			{
				obj.usingStatement();
			}
			
			break;
			case 2:
			{
				obj.insertingData();
			}
			break;
			case 3:
			{
				obj.createtable();
			}
			break;
			case 4: 
			{
				obj.insertImage();
			}
			break;
			case 5:
			{
				obj.InsertVideo();
			}
			break;
			case 6:
			{
				obj.RetrieveOnCustomerId();
			}
			break;
			case 7:
			{
				obj.StroedProcedure();
			}
			break;
			case 8:
			{
				obj.ExecuteFunction();
			}
			break;
			case 9:
			{
				obj.BatchProcessing();
			}
			break;
			case 10:
			{
				obj.TrsactionManagent();
			}
			default :
			{
				System.out.println("Invalid Choice...!");
			}
			
			}	
		}
	}
}
