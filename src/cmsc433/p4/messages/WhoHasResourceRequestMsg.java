package cmsc433.p4.messages;

public class WhoHasResourceRequestMsg {	
	private final String resource_name;
	// Maybe add the user asking for access here?
	
	public WhoHasResourceRequestMsg (String resource) {
		this.resource_name = resource;
	}
	
	public String getResourceName () {
		return resource_name;
	}
	
	@Override 
	public String toString () {
		return "Who has " + resource_name + "?";
	}
}
