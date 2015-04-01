package filesync;

import java.net.*;
import java.io.*;

/**
 * Created by yepengfan on 28/03/15.
 */
public class SyncServer {
    public static void main(String[] args){
        try{
            int serverPort = 4444; // default server port is 4444
            ServerSocket listenSocket = new ServerSocket(serverPort);
            int i = 0;
            while(true) {
                System.out.println("Server listening fro a connection");
                Socket clientSocket = listenSocket.accept();
                i++;
                System.out.println("Received connection: " + i);
                Connection c = new Connection(clientSocket);
                }
            }catch (IOException e){
                System.out.println("Listen socket: " + e.getMessage());
            }
        }
    }

class Connection extends Thread {
    DataInputStream in;
    DataOutputStream out;
    Socket clientSocket;
    public Connection (Socket aClientSocket){
        try{
            clientSocket = aClientSocket;
            in = new DataInputStream(clientSocket.getInputStream());
            out = new DataOutputStream(clientSocket.getOutputStream());
            this.start();
        }catch (IOException e){
            System.out.println("Connection: " + e.getMessage());
        }
    }
    public void run(){
        try{ // an echo server
            System.out.println("Server reading data");
            String data = in.readUTF();
            System.out.println("Server writing data");
            out.writeUTF(data);
        }catch (EOFException e){
            System.out.println("EOF: " + e.getMessage());
        }catch (IOException e){
            System.out.println("Readline: " + e.getMessage());
        }finally {
            try{
                clientSocket.close();
            }catch (IOException e){
                System.out.println("Socket closed failed.");
            }
        }
    }
}
