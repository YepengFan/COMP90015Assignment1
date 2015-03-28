package filesync;

/**
 * @author aaron
 * @date 7th April 2013
 */

import java.io.File;

import org.apache.commons.codec.binary.Base64;
import org.json.simple.JSONObject;

/*
 * The StartUpdate instruction prepares the receiver to synchronise
 * the file. It must be issued prior to receiving updates.
 */

public class StartUpdateInstruction extends Instruction {

	private String fileName;
	
	public StartUpdateInstruction(String fileName) {
		this.fileName = new File(fileName).getName();
	}

	public StartUpdateInstruction() {
	}

	@Override
	public String Type() {
		return "StartUpdate";
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	@SuppressWarnings("unchecked")
	@Override
	public String ToJSON() {
		JSONObject obj=new JSONObject();
		obj.put("Type", Type());
		obj.put("FileName", getFileName());
		return obj.toJSONString();
	}

	@Override
	public void FromJSON(String jst) {
		JSONObject obj = null;
		try {
			obj = (JSONObject) parser.parse(jst);
		} catch (org.json.simple.parser.ParseException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		if(obj!=null){
			setFileName(obj.get("FileName").toString());
		}
	}

}
