package filesync;

/**
 * @author aaron
 * @date 7th April 2013
 */

import org.apache.commons.codec.binary.Base64;
import org.json.simple.JSONObject;

/*
 * The EndUpdate instruction is used to specify that no more
 * blocks are coming. The temporary file can now become the
 * synchronised file.
 */

public class EndUpdateInstruction extends Instruction{

	@Override
	public String Type() {
		return "EndUpdate";
	}

	@SuppressWarnings("unchecked")
	@Override
	public String ToJSON() {
		JSONObject obj=new JSONObject();
		obj.put("Type", Type());
		return obj.toJSONString();
	}

	@Override
	public void FromJSON(String jst) {
		// TODO Auto-generated method stub
		
	}
}
