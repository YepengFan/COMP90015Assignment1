package filesync;

/**
 * Created by yepengfan on 28/03/15.
 */

import java.net.*;
import java.io.*;

/**
 * Implement TCP Protocol.
 *
 */

public class SyncClient {
    public static void main(String[] args){
        Socket s = null;
        String hostname = args[1];
        String message = args[0];
        try{
            int serverPort = 4444;  // default server port is 4444
            s = new Socket(hostname, serverPort);
            System.out.println("Connection Established");
            DataInputStream in = new DataInputStream(s.getInputStream());
            DataOutputStream out = new DataOutputStream(s.getOutputStream());
            System.out.println("Sending Data");
            out.writeUTF(message);
            String data = in.readUTF();
            System.out.println("Received: " + data);
        }catch (UnknownHostException e){
            System.out.println("Socket: " + e.getMessage());
        }catch (EOFException e){
            System.out.println("EOF: " + e.getMessage());
        }catch (IOException e){
            System.out.println("Readline: " + e.getMessage());
        }finally {
            if(s != null) try{
                s.close();
            }catch (IOException e){
                System.out.println("close: " + e.getMessage());
            }
        }
    }
}
