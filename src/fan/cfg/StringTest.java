package fan.cfg;

public class StringTest {

	public static void main(String[] args) {
		String state = "mv@b@c@#";
		//state = state.substring(0, state.length() - 1);
		//state = state.split("#").length;
		System.out.println("state:" + state + ", len:" + state.split("#").length);
	}
}
