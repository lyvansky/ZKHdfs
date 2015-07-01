package old.fan.server.ops;

public enum TxnType {
	
	RENAME("mv"),
	
	DELETE("del");
	
	public final String text;
	
	TxnType(String text){
		this.text = text;
	}
	
	public String toString(){
		return text;
	}
}
