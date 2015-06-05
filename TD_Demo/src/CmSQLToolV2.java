import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Writer;

import org.msgpack.type.ArrayValue;
import org.msgpack.type.Value;
import org.msgpack.unpacker.Unpacker;
import org.msgpack.unpacker.UnpackerIterator;

import com.treasure_data.client.ClientException;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.model.Database;
import com.treasure_data.model.DatabaseSummary;
import com.treasure_data.model.Job;
import com.treasure_data.model.Job.Type;
import com.treasure_data.model.JobResult;
import com.treasure_data.model.JobSummary;
import com.treasure_data.model.TableSummary;
	
	
public class CmSQLToolV2 {

	
	public static String db_name;
	public static String tb_name;
	public String min_Tstamp;
	public String max_Tstamp; 
	public static String col_names;
	public static String sql_engine;
	public static String query_txt;
	public String result_tbl;
	public static String dFile_format; 
	
	public static String out_file = "download.";

	// get TD property file 
	static {
		try {
			Properties props = System.getProperties();
			props.load(CmSQLToolV2.class.getClassLoader().getResourceAsStream("treasure-data.properties"));
		} catch (IOException e) {
			// do something
			System.out.println("Get Resource Failed!!! ");
		}
	}

	
    public static void main(String[] args) throws IOException {
    	System.out.println("=======================================");
		System.out.println("========== CmSQLTool Started ==========");
		System.out.println("=======================================");
		System.out.println(" ");
		
		System.out.println("Please provide the details of your query");
		System.out.println(" ");
				
		// Get Query parameters from Command Line
		
		 BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
	        
		 	System.out.println("Enter Database Name - ");
	        System.out.println("Possible DB name is trades_db" );	        
	        db_name = br.readLine();
		
	        System.out.println("Enter Table Name - ");
	        System.out.println("Possible Table name is trans_tb" );	        
	        tb_name = br.readLine();
		
	        System.out.println("Enter Names of Columns - ");
	        System.out.println("Possible Columns names are > last_trade, trend" );	 
	        System.out.println("> symbol, my_shares, change, name, value,");
	        System.out.println("> volume, amt_to_sell, profits" );	
	        col_names = br.readLine();
	        
	        System.out.println("Enter SQL Engine - ");
	                          
	        System.out.println("Possible SQL Engines are > HIVE or PRESTO" );	        
	        sql_engine = br.readLine();
		
	        System.out.println("Enter Download File Format - ");
	        System.out.println("Possible values are > csv or txt" );	        
	        dFile_format = br.readLine();
	        
	        // put it all together
	        query_txt = "SELECT " + col_names + " FROM "  + tb_name;
	        out_file = "download." + dFile_format; 
	        
	        System.out.println("Query to run > " + query_txt); 
	       	        
	    // run query     
		CmSQLToolV2 thisObj = new CmSQLToolV2();
		try {
			thisObj.runQuery();
		} catch (ClientException e) {
			System.out.println("Client Exception");
		}
		//  end of program 
		System.out.println(" ");
		System.out.println("=======================================");
		System.out.println("==========  CmSQLTool Ended  ==========");
		System.out.println("=======================================");	
	
			
	} // end of main 
	    
    
//  Bulk of program: run query and write result to the files system 
	
	public void runQuery() throws ClientException, IOException {
        TreasureDataClient client = new TreasureDataClient();
		
        Job job = new Job(new Database(db_name), Type.valueOf(sql_engine) , query_txt, result_tbl);

        client.submitJob(job);
        // client.submitJob(job);    ********************** Not sure why a second Job was submitted 
        String jobID = job.getJobID();
        System.out.println("Job id > " + jobID);

        while (true) {
            JobSummary.Status stat = client.showJobStatus(job);
            if (stat == JobSummary.Status.SUCCESS) {
                break;
            } else if (stat == JobSummary.Status.ERROR) {
                String msg = String.format("Job '%s' failed: got Job status 'error'", jobID);
                JobSummary js = client.showJob(job);
                if (js.getDebug() != null) {
                    System.out.println("cmdout:");
                    System.out.println(js.getDebug().getCmdout());
                    System.out.println("stderr:");
                    System.out.println(js.getDebug().getStderr());
                }
                throw new ClientException(msg);
            } else if (stat == JobSummary.Status.KILLED) {
                String msg = String.format("Job '%s' failed: got Job status 'killed'", jobID);
                throw new ClientException(msg);
            }

            try {
                Thread.sleep(4 * 1000);
            } catch (InterruptedException e) {
                // do something
            }
        }
             
        // Setup File Writer 
        
        FileWriter fWriter = new FileWriter(out_file);	
        PrintWriter pWriter  = new PrintWriter(fWriter);
        pWriter.println(col_names);
        
        JobResult jobResult = client.getJobResult(job);
        Unpacker unpacker = jobResult.getResult(); // Unpacker class is MessagePack's deserializer
        UnpackerIterator iter = unpacker.iterator();
        while (iter.hasNext()) {
            ArrayValue row = iter.next().asArrayValue();
            for (Value elm : row) {
                System.out.print(elm + ","); 
                pWriter.println(row);              // write to file system
            }
            System.out.println();
        }
        pWriter.flush();
        pWriter.close();
    }
				

	
} // end CmSQLToolV2 	

