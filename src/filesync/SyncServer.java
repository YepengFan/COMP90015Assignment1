package filesync;

/**
 * Created by yepengfan on 28/03/15.
 */

import java.net.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

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
    private static Map<String, FileSync> threadMapper = new HashMap<>();
//    private static Map<String, Connection> connectionMapper = new HashMap<>();
    // Single Client Version
    private static Connection connection = null;
    private static int NUMBER = 0;


    private void processConnections(Socket clientSocket) {
//        connectionMapper.put(("Connection " + NUMBER), new Connection(clientSocket));
        connection = new Connection(clientSocket);
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

        // register files to threadMap
        File[] files = dir.listFiles();
        for (File file : files) {
            try {
                SynchronisedFile tf = new SynchronisedFile(file.getAbsolutePath()
                        .toString());
//                Thread fileSync = new Thread(new FileSync(tf));
                FileSync fileSync = new FileSync(tf);
                threadMapper.put(file.getName(), fileSync);
                fileSync.start();
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
            try { // an echo server
                String fileName = null;
                while (true) {
                    synchronized (this) {
                        System.out.println("Server reading data");
                        String data = in.readUTF();
                        //parse input stream JSON
                        Map json = parseJSON(data);
                        if (json.get("Type").equals("Index")) {
//                            System.out.println("Index Information: " + data);
                            // parse directory index
                            JSONArray file_list = (JSONArray) json.get("Index");
//                            System.out.println(file_list);
                            for (int i = 0; i < file_list.size(); i++) {
//                                System.out.println(file_list.get(i));
                                // check file name with thread map
                                if (threadMapper.get(file_list.get(i)) ==
                                        null) {
//                                    System.out.println("dont");
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
                                    System.out.println(toDirectory);
                                    System.out.println(file.getName());
//                                    Thread fileSync = new Thread(new FileSync
//                                            (tf));
                                    FileSync fileSync = new FileSync(tf);

                                    // add thread instance to map
                                    threadMapper.put(file.getName(), fileSync);
//                                    fileSync.start();
                                }
                            }
                        } else if (json.get("Type").equals("StartUpdate")) {
//                            System.out.println("Start Update!!!");
                            fileName = json.get("FileName").toString();
//                            System.out.println("!!!!!!!");
                            System.out.println(threadMapper.get(fileName));
//                            threadMap.get(json.get(fileName)).insertQue
//                                    (json.toString());
                            threadMapper.get(fileName).insertQue(json
                                    .toString());
                            System.out.println(json.toString());
                            System.out.println(threadMapper.get(json.get
                                    ("FileName")));
                        } else if (json.get("Type").equals("EndUpdate")) {
//                            System.out.println("End Update!!!");
                            threadMapper.get(fileName).insertQue(json
                                    .toString());
                            fileName = null;
                        } else {
                            threadMapper.get(fileName).insertQue(json
                                    .toString());
                        }

                        System.out.println("Server writing data");
                        System.out.println(data);
                        out.writeUTF(data);
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

    protected class FileSync extends Thread {
        SynchronisedFile toFile;
        //        private ArrayBlockingQueue<Instruction> instQueue  = new
//                ArrayBlockingQueue<>(1024 * 1024);
        private BlockingQueue<Instruction> instQueue = new
                ArrayBlockingQueue<>(1024 * 1024);

        FileSync(SynchronisedFile tf) {
            toFile = tf;
        }

        public void insertQue(String msg) {
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
            while (true) {
                try {
                    toFile.ProcessInstruction(instQueue.take());
                } catch (BlockUnavailableException e) {
                    try {
                        System.out.println("Block Unavailable Exception!!!");
                        connection.out.writeUTF("BlockUnavailableException");
                    } catch (IOException e1) {
//                        e1.printStackTrace();
                    }
//                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }
    }
}
