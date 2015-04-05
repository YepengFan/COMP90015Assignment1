package filesync;

/**
 * Created by yepengfan on 28/03/15.
 */

import java.net.*;
import java.io.*;
import java.util.*;

import org.json.simple.JSONArray;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;

/**
 * Implement TCP Protocol - Server Side
 */

public class SyncServer {
    private static ServerSocket listenSocket = null;
    private static int serverPort = 4444; // default port is 4444
    private static String toDirectory = null;
    private static Map<String, SynchronisedFile> instanceMapper = new
            HashMap<>();
    // Single Client Version
    private static int NUMBER = 0;


    private void processConnections(Socket clientSocket) {
        new Connection(clientSocket);
    }

    public static void main(String[] args) {
        toDirectory = args[0];
        SyncServer syncServer = new SyncServer();
        syncServer.registerDir();

        try {
            listenSocket = new ServerSocket(serverPort);
            while (true) {
                System.out.println("Server listening for a connection");
                Socket clientSocket = listenSocket.accept();
                NUMBER++;
                System.out.println("Received connection: " + NUMBER);
                syncServer.processConnections
                        (clientSocket);
            }
        } catch (IOException e) {
            System.out.println("Listen socket: " + e.getMessage());
        }
    }

    private void registerDir() {
        // destination directory
        File dir = new File(toDirectory);
        if (!dir.exists()) {
            // if directory doesnt exist, then
            // create it.
            dir.mkdir();
        }

        // register files to instanceMapper
        File[] files = dir.listFiles();
        for (File file : files) {
            try {
                SynchronisedFile tf = new SynchronisedFile(file.getAbsolutePath()
                        .toString());
                instanceMapper.put(file.getName(), tf);
            } catch (IOException e) {
                e.printStackTrace();
            }
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
                    return null;
                }

                @Override
                public List creatArrayContainer() {
                    return null;
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

        private String[] parseDir(String jsonIndex) {
            String[] file_list = jsonIndex.split(",");

            return file_list;
        }

        public void run() {
            InstructionFactory instFact = new InstructionFactory();
            try { // an echo server
                String fileName = null;
                while (true) {
                    synchronized (this) {
                        System.out.println("Server reading data");
                        String data = in.readUTF();
                        //parse input stream JSON
                        Map json = parseJSON(data);

                        if (json.get("Type").equals("Index")) {
                            // parse directory index
                            JSONArray file_list = (JSONArray) json.get("Index");
//                            System.out.println(file_list);
                            for (int i = 0; i < file_list.size(); i++) {
//                                System.out.println(file_list.get(i));
                                if(instanceMapper.get(file_list.get(i)) == null){
//                                    // destination directory
                                    File dir = new File(toDirectory);
                                    // create files
                                    File file = new File(dir.getAbsoluteFile
                                            ().toString() + File.separator +
                                            file_list.get(i));
                                    if (!file.exists()) {
                                        file.createNewFile();
                                    }
                                    SynchronisedFile tf = new
                                            SynchronisedFile(file
                                            .getAbsolutePath().toString());
                                    // add new instance to instance mapper
                                    instanceMapper.put(file.getName(), tf);
                                }
                            }
                        } else if (json.get("Type").equals("StartUpdate")) {
                            fileName = json.get("FileName").toString();
                            Instruction receivedInst = instFact.FromJSON(json
                                    .toString());
                            try {
                                instanceMapper.get(fileName).ProcessInstruction(receivedInst);
                            } catch (BlockUnavailableException e) {
//                                e.printStackTrace();
                            }
                        } else {
                            Instruction receivedInst = instFact.FromJSON(json
                                    .toString());
                            try {
                                instanceMapper.get(fileName)
                                        .ProcessInstruction(receivedInst);
                            } catch (BlockUnavailableException e){
                                out.writeUTF("BlockUnavailableException");
                            }
                        }

                        System.err.println(json.toString());
                        out.writeUTF(json.toString());
                    }
                }
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

    }
}
