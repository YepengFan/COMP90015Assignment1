package filesync;

/**
 * Created by yepengfan on 28/03/15.
 */

import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;

import java.net.*;
import java.io.*;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Implement TCP Protocol - Server Side
 */

public class SyncServer {
    private static ServerSocket listenSocket = null;
    private static int serverPort = 4444; // default port is 4444
    private static String toDirectory = null;

    private void processConnections(Socket clientSocket) {
        new Connection(clientSocket);
    }

    public static void main(String[] args) {
        toDirectory = args[0];
        try {
            listenSocket = new ServerSocket(serverPort);
            int i = 0;
            while (true) {
                System.out.println("Server listening for a connection");
                Socket clientSocket = listenSocket.accept();
                i++;
                System.out.println("Received connection: " + i);
                new SyncServer().processConnections
                        (clientSocket);
            }
        } catch (IOException e) {
            System.out.println("Listen socket: " + e.getMessage());
        }
    }

    protected class Connection extends Thread {
        DataInputStream in;
        DataOutputStream out;
        Socket clientSocket;

        public Connection(Socket aClientSocket) {
            try {
                clientSocket = aClientSocket;
                in = new DataInputStream(clientSocket.getInputStream());
                out = new DataOutputStream(clientSocket.getOutputStream());
                this.start();
            } catch (IOException e) {
                System.out.println("Connection: " + e.getMessage());
            }
        }

        private Map parseJSON(String data) {
            JSONParser parser = new JSONParser();
            ContainerFactory containerFactory = new ContainerFactory() {
                @Override
                public Map createObjectContainer() {
                    return new LinkedHashMap();
                }

                @Override
                public List creatArrayContainer() {
                    return new LinkedList();
                }
            };

            Map json = null;
            try {
                json = (Map) parser.parse(data, containerFactory);
            } catch (org.json.simple.parser.ParseException pe) {
                System.out.println(pe.getMessage());
            }
            return json;
        }

        public synchronized void run() {
            try { // an echo server
                System.out.println("Server reading data");
                String data = in.readUTF();
                System.out.println(data);
//                Map json = parseJSON(data);
//
//                if (json.get("Type").equals("StartUpdate")) {
//                    String path = toDirectory + "/" + String.valueOf(json.get
//                            ("FileName"));
//                    SynchronisedFile toFile = new SynchronisedFile(path);
//                    FileSync tf = new FileSync(toFile);
//                    tf.pushInst(data);
////                    while (json.get("Type").equals("EndUpdate") == false) {
////                        data = in.readUTF();
////                        System.out.println("new" + data);
////                        json = parseJSON(data);
////                        tf.pushInst(data);
////                    }
////                    data = in.readUTF();
////                    tf.pushInst(data);
//                }

                System.out.println("Server writing data");
                out.writeUTF(data);
            } catch (EOFException e) {
                System.out.println("EOF: " + e.getMessage());
            } catch (IOException e) {
                System.out.println("Readline: " + e.getMessage());
            } finally {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    System.out.println("Socket closed failed.");
                }
            }
        }

        protected class FileSync implements Runnable {
            SynchronisedFile toFile;
            private ArrayBlockingQueue<Instruction> instQueue = new
                    ArrayBlockingQueue<Instruction>(1024 * 1024);

            FileSync(SynchronisedFile tf) {
                toFile = tf;
            }

            public void pushInst(String msg) {
                Instruction inst;
                InstructionFactory instFact = new InstructionFactory();
                inst = instFact.FromJSON(msg);
                try {
                    instQueue.put(inst);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public synchronized void run() {
                Instruction inst;
                InstructionFactory instFact = new InstructionFactory();
//            Instruction receivedInst = instFact.FromJSON(msg);

            }
        }
    }
}
