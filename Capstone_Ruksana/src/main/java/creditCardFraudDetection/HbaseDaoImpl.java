package creditCardFraudDetection;

import java.io.IOException;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Result;

public class HbaseDaoImpl implements Serializable {
	private static final long serialVersionUID = 1L;
	public static void validateRules(CreditTransactionsUtil cardTransactionDtls) throws IOException, ParseException {
		//Function to fetch the latest details corresponding to the cardid of the incoming transaction.
		String card_dtls_hbase = getCardDtls(cardTransactionDtls);
		
		//The latest details of the cardid are retrieved with # deleimeter and processed.
		String[] card_dtls_hbase_split = card_dtls_hbase.split("#");
		
		//Setting the date format for further processing.
	    SimpleDateFormat formatter=new SimpleDateFormat("yyy-MM-dd HH:mm:ss");
	    
	    //Initializing the required varibles to validate the rules of a transaction.
		double ucl = 0d;
		int score = 0;
		String last_post_code = null;
		Date last_trans_dt = null;
		
		//Assigning the values of latest transaction to their corresponding variables.
		if (card_dtls_hbase_split != null )
		{
			 for (int i=0; i < card_dtls_hbase_split.length; i++)
			  {
			    if(i==0)
			    	ucl = Double.parseDouble(card_dtls_hbase_split[i]);
			    if(i==1)
			    	score = Integer.parseInt(card_dtls_hbase_split[i]);
			    if(i==2)
			    	last_post_code = card_dtls_hbase_split[i];
			    if(i==3)
			    	last_trans_dt =formatter.parse(card_dtls_hbase_split[i]); 
			  }
		}
		
		//Creating the object distanceUtility to calculate the distance between the latest zipcode and current zipcode.
		DistanceUtility distanceUtility = new DistanceUtility();
		
		//Passing the transaction date, current and latest zipcodes to validate the third rule.
		double speed = distanceUtility.calcDistance(cardTransactionDtls.getPostcode(),cardTransactionDtls.getTransaction_dt(),last_post_code,last_trans_dt);		
		
		//If the amount from the incoming transaction is  less than equal to the ucl from look-up table and if the score
		// is greater than equal to 200 and the speed of travel between the last zipcode and current zipcoce is <= 0.25 km
		// Then that transaction has to be classified as GENUINE. else it has to be marked as FRAUD.
		
		if(cardTransactionDtls.getAmount()<=ucl && score >=200 && speed <=0.25)
		{
			//Set the status of the transaction as GENUINE.
			cardTransactionDtls.setStatus("GENUINE");
			System.out.println("Set status as GENUINE for cardid ::"+cardTransactionDtls.getCard_id()+" , member_id ::"+cardTransactionDtls.getMember_id()+" , amount ::"+cardTransactionDtls.getAmount()+" , pos_id ::"+cardTransactionDtls.getPos_id()
			+" ,post_code ::"+cardTransactionDtls.getPostcode()+" , transaction_dt ::"+cardTransactionDtls.getTransaction_dt()+" , status::"+cardTransactionDtls.getStatus());
		}
		else{
			
			//Set the status of the transaction as FRAUD.
			cardTransactionDtls.setStatus("FRAUD");
			System.out.println("Set status as FRAUD for cardid ::"+cardTransactionDtls.getCard_id()+" , member_id ::"+cardTransactionDtls.getMember_id()+" , amount ::"+cardTransactionDtls.getAmount()+" , pos_id ::"+cardTransactionDtls.getPos_id()
			+" ,post_code ::"+cardTransactionDtls.getPostcode()+" , transaction_dt ::"+cardTransactionDtls.getTransaction_dt()+" , status::"+cardTransactionDtls.getStatus());
		}
		
		//Insert this transaction to the hbase along with the classified status.
			try {
				Admin hBaseAdmin = HbaseConnection.getHbaseAdmin();
				Connection connection = HbaseConnection.getHbaseAdmin().getConnection();
				String card_trans_table = "CARD_TRANSACTIONS_FINAL_HBASE";
				String update_lookup_table = "LOOKUP_FINAL_HBASE";
				Date date= new Date();
				long time_in_ms = date. getTime();
				DateFormat format_date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				String trans_dt = format_date.format(cardTransactionDtls.getTransaction_dt());
					Table hbase_trans_table = null;
					hbase_trans_table = connection.getTable(TableName.valueOf(card_trans_table));
					String row_key = cardTransactionDtls.getCard_id()+"_"+cardTransactionDtls.getMember_id()+"_"+Long.toString(time_in_ms);
					String columnfamily= "cf1";
					System.out.println("Adding a row to the card transaction table.");
					
					/*********** adding a new row ***********/
					Put p =  new Put(Bytes.toBytes(row_key));
					p.addColumn(Bytes.toBytes(columnfamily), Bytes.toBytes("card_id"), Bytes.toBytes(cardTransactionDtls.getCard_id()));
					p.addColumn(Bytes.toBytes(columnfamily), Bytes.toBytes("member_id"), Bytes.toBytes(cardTransactionDtls.getMember_id()));
					p.addColumn(Bytes.toBytes(columnfamily), Bytes.toBytes("amount"), Bytes.toBytes(cardTransactionDtls.getAmount()));
					p.addColumn(Bytes.toBytes(columnfamily), Bytes.toBytes("postcode"), Bytes.toBytes(cardTransactionDtls.getPostcode()));
					p.addColumn(Bytes.toBytes(columnfamily), Bytes.toBytes("pos_id"), Bytes.toBytes(cardTransactionDtls.getPos_id()));
					p.addColumn(Bytes.toBytes(columnfamily), Bytes.toBytes("transaction_dt"), Bytes.toBytes(trans_dt));
					p.addColumn(Bytes.toBytes(columnfamily), Bytes.toBytes("status"), Bytes.toBytes(cardTransactionDtls.getStatus()));
					// add the row to the table
					hbase_trans_table.put(p);
					
					//If the transaction is classified as GENUINE, then look-up table has to be updated with post code and transaction date.
					if(cardTransactionDtls.getStatus()!=null && cardTransactionDtls.getStatus() == "GENUINE" )
					{
						Table hbase_lookup_table = connection.getTable(TableName.valueOf(update_lookup_table));
						System.out.println("Updating a row to the Look-up table.");
						Put put_lookup =  new Put(Bytes.toBytes(cardTransactionDtls.getCard_id()));
						put_lookup.addColumn(Bytes.toBytes(columnfamily), Bytes.toBytes("postcode"), Bytes.toBytes(cardTransactionDtls.getPostcode()));
						put_lookup.addColumn(Bytes.toBytes(columnfamily), Bytes.toBytes("transaction_dt"), Bytes.toBytes(trans_dt));
						hbase_lookup_table.put(put_lookup);
					}
					//Close the connection
				hBaseAdmin.close();
			}catch(Exception e) {
				e.printStackTrace();
			}
		
		
	}
	@SuppressWarnings({ "finally" })
	private static String getCardDtls(CreditTransactionsUtil cardTransactionDtls) throws IOException {
		//Method to get the details from the look-up table corresponding to the card_id of the incoming transaction.
		Connection connection = HbaseConnection.getHbaseAdmin().getConnection();
		String table = "LOOKUP_FINAL_HBASE";
		double ucl = 0;
		int score = 0;
		String last_post_code = null;
		String last_trans_dt = null;
		Table htable = null;
		
		try {
			Get card_id = new Get(Bytes.toBytes(cardTransactionDtls.getCard_id()));
			htable = connection.getTable(TableName.valueOf(table));
			Result card_id_cell = htable.get(card_id);
			byte[] ucl_bytes = card_id_cell.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("ucl"));
			byte[] score_bytes = card_id_cell.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("score"));
			byte[] last_post_code_bytes = card_id_cell.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("postcode"));
			byte[] last_trans_dt_bytes = card_id_cell.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("transaction_dt"));
			if (ucl_bytes != null) {
				ucl = Double.parseDouble(Bytes.toString(ucl_bytes));
			}else ucl = 0d;
			if (score_bytes != null) {
				score = Integer.parseInt(Bytes.toString(score_bytes));
			}else score = 0;
			if (last_post_code_bytes != null) {
				last_post_code = Bytes.toString(last_post_code_bytes);
			}else last_post_code="null";
			if (last_trans_dt_bytes != null) {
				last_trans_dt = Bytes.toString(last_trans_dt_bytes);
			}else last_trans_dt = "null";
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			try {
			if (htable != null)
				htable.close();
			} catch (Exception e) {
			e.printStackTrace();
			}
		//Returning the ucl, memberscore , post code and transaction date of teh cardid from the look up table with the # delimeter.
		return ucl+"#"+score+"#"+last_post_code+"#"+last_trans_dt;
		}
	}
	
}