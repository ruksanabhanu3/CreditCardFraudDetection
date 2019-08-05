package creditCardFraudDetection;

import java.util.Date;
import java.io.IOException;
import java.text.SimpleDateFormat;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class CreditTransactionsUtil {
	
	//Getter and setter attributes for the parameters coming from the kafka topic for each of the transactions. 
	private String card_id;
	private String member_id;
	private double amount;
	private String postcode;
	public String getCard_id() {
		return card_id;
	}
	public void setCard_id(String card_id) {
		this.card_id = card_id;
	}
	public String getMember_id() {
		return member_id;
	}
	public void setMember_id(String member_id) {
		this.member_id = member_id;
	}
	public double getAmount() {
		return amount;
	}
	public void setAmount(double amount) {
		this.amount = amount;
	}
	public String getPostcode() {
		return postcode;
	}
	public void setPostcode(String postcode) {
		this.postcode = postcode;
	}
	public String getPos_id() {
		return pos_id;
	}
	public void setPos_id(String pos_id) {
		this.pos_id = pos_id;
	}
	public Date getTransaction_dt() {
		return transaction_dt;
	}
	public void setTransaction_dt(Date transaction_dt) {
		this.transaction_dt = transaction_dt;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	private String pos_id;
	private Date transaction_dt;
	private String status;
	
	public CreditTransactionsUtil() {}
	public CreditTransactionsUtil(String transactionJson){
		//Constructor which takes the incoming transaction as string argument , parses and converts to json object.
		JSONParser parser = new JSONParser();
		try 
		{
			JSONObject obj = (JSONObject) parser.parse(transactionJson);


			this.card_id = obj.get("card_id") + "";
			this.member_id = obj.get("member_id") + "";
			this.amount = Double.parseDouble(obj.get("amount") + "");
			this.postcode = obj.get("postcode") + "";
			this.pos_id = obj.get("pos_id") + "";
			this.transaction_dt = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").parse(obj.get("transaction_dt") + "");
		}
		catch(Exception e) 
		{
			e.printStackTrace();
		}
	}
	public void validateTransactionsRules(CreditTransactionsUtil x) throws IOException, java.text.ParseException
	{
	//Function to validate the rules.
	 HbaseDaoImpl.validateRules(x);
	}
}
