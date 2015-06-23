package fan.server.ops;

public enum TxnState {
	
	PREPARE_LOCK("PREPARE_LOCK"),
	
	EXEC_DELETE("EXEC_DELETE"),
	
	EXEC_RENAME("EXEC_RENAME"),
	
	RELEASE_LOCK("RELEASE_LOCK");
	
	final String text;
	
	TxnState(String text){
		this.text = text;
	}
	
	public String toString(){
		return text;
	}
}
