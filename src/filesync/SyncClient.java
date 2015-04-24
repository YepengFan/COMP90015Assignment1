package filesync;

/**
 * Created by yepengfan on 28/03/15.
 */

import java.net.*;
import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.json.simple.JSONObject;

import static java.nio.file.StandardWatchEventKinds.*;
import static java.nio.file.LinkOption.*;

/**
 * Implement TCP Protocol - Client Side
 */

public class SyncClient {
    private static int serverPort = 4444; // default port is 4444
    private static DataInputStream in = null;
    private static DataOutputStream out = null;

    private static Map<String, FileSync> threadMapper = new HashMap<>();
    private static BlockingQueue<Object> msgQueue = new
            ArrayBlockingQueue<>(1024);

    private final WatchService watcher;
    private final Map<WatchKey, Path> keys;
    private final boolean recursive;
    private boolean trace = false;

    public static void main(String[] args) throws IOException {
        String hostname = args[1];
        String directory = args[0];

        try (Socket socket = new Socket(hostname, serverPort)) {
            System.out.println("Connection Established");
            in = new DataInputStream(socket.getInputStream());
            out = new DataOutputStream(socket.getOutputStream());
            boolean recursive = false;
            Path dir = Paths.get(directory);
            SyncClient client = new SyncClient(dir, recursive);
            Messenger msger = new Messenger();

            // set msger as daemon thread
            msger.setDaemon(true);
            msger.start();
            client.indexingDirectory(directory);
            client.processEvents();
        }
    }

    @SuppressWarnings("unchecked")
    static <T> WatchEvent<T> cast(WatchEvent<?> event) {
        return (WatchEvent<T>) event;
    }

    private void register(Path dir) throws IOException {
        WatchKey key = dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE,
                ENTRY_MODIFY);
        if (trace) {
            Path prev = keys.get(key);
            if (prev == null) {
                System.out.format("register: %s\n", dir);
            } else {
                if (!dir.equals(prev)) {
                    System.out.format("update: %s -> %s\n", prev, dir);
                }
            }
        }
        keys.put(key, dir);
    }

    private void registerAll(final Path start) throws IOException {
        // register directory and sub-directories
        Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir,
                                                     BasicFileAttributes
                                                             attrs) throws
                    IOException {
                register(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public SyncClient(Path dir, boolean recursive) throws IOException {
        this.watcher = FileSystems.getDefault().newWatchService();
        this.keys = new HashMap<WatchKey, Path>();
        this.recursive = recursive;

        if (recursive) {
            System.out.format("Scanning %s ...\n", dir);
            registerAll(dir);
            System.out.println("Done.");
        } else {
            register(dir);
        }
        // enable trace after initial registration
        this.trace = true;
    }

    // put instruction to the instQueue
    private static void putMsg(Queue<Object>
                                       fileInstQueue) throws
            InterruptedException {
        Iterator iterator = fileInstQueue.iterator();

        if (fileInstQueue.size() == 1) {
            JSONObject json = (JSONObject) fileInstQueue.poll();
            msgQueue.put(json);
        } else {
            Instruction instruction;
            while (iterator.hasNext()) {
                instruction = (Instruction) iterator.next();
                msgQueue.put(instruction);
            }
        }
    }

    // take instruction from the instQueue
    private static Object takeMsg() throws InterruptedException {
        return msgQueue.take();
    }

    // main thread which invoke check file state when file changed
    private void processEvents() {
        for (; ; ) {
            // wait for key to be signalled
            WatchKey key;
            try {
                key = watcher.take();
            } catch (InterruptedException e) {
                return;
            }

            Path dir = keys.get(key);
            if (dir == null) {
                System.err.println("WatchKey not recognised!");
                continue;
            }

            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent.Kind kind = event.kind();

                if (kind == OVERFLOW) {
                    continue;
                }

                // Context for directory entry event is the file name of entry
                WatchEvent<Path> ev = cast(event);
                Path name = ev.context();
                Path child = dir.resolve(name);

                // print out event
                System.out.format("%s: %s\n", event.kind().name(), child);

                // start a thread to service the Instruction queue.
                try {
                    // todo: check
                    if (event.kind().name().equals("ENTRY_CREATE")) {
//                        System.out.println("new file created " + child
//                                .getFileName());
                        Queue<Object> msgQ = new LinkedList<>();
                        JSONObject json = new JSONObject();

                        json.put("Type", "CreateFile");
                        json.put("FileName", child.getFileName().toString());
                        msgQ.offer(json);
                        SyncClient.putMsg(msgQ);
                        msgQ.clear();

                        File file = child.toFile();
                        FileSync fileSync = new FileSync(new SynchronisedFile
                                (file.getAbsolutePath()));
                        threadMapper.put(file.getName(), fileSync);
                        fileSync.setDaemon(true);
                        fileSync.start();
                    } else if (event.kind().name().equals("ENTRY_DELETE")) {
                        System.out.println("file deleted " + child.getFileName());


                    } else {
                        threadMapper.get(String.valueOf(child.getFileName())).fromFile
                                .CheckFileState();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                // if directory is created, and watching recursively, then
                // register it and its sub-directories
                if (recursive && (kind == ENTRY_CREATE)) {
                    try {
                        if (Files.isDirectory(child, NOFOLLOW_LINKS)) {
                            registerAll(child);
                        }
                    } catch (IOException e) {
                    }
                }
            }

            // reset key and remove from set if directory no longer accessible
            boolean valid = key.reset();
            if (!valid) {
                keys.remove(key);
                // all directories are inaccessible
                if (keys.isEmpty()) {
                    break;
                }
            }
        }
    }

    private void indexingDirectory(String directory) {
        JSONObject index = new JSONObject();
        String[] dir = new File(directory).list();
        File folder = new File(directory);
        File[] files = folder.listFiles();
        LinkedList<String> list = new LinkedList<>(Arrays.asList(dir));
        index.put("Type", "Index");
        index.put("Index", list);

        try {
            out.writeUTF(index.toJSONString());
            System.err.println("Sending: " + index.toJSONString());
            String data = in.readUTF();
            System.out.println("Received: " + data);
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (File file : files) {
            try {
                FileSync fileSync = new FileSync(new SynchronisedFile(file
                        .getAbsolutePath()));

                try {
                    fileSync.fromFile.CheckFileState();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // set fileSync as daemon thread
                threadMapper.put(file.getName(), fileSync);
                fileSync.setDaemon(true);
                fileSync.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    protected static class Messenger extends Thread {
        @Override
        public void run() {
//            Instruction inst;
            Object obj;
            try {
                while ((obj = SyncClient.takeMsg()) != null) {
                    if (obj instanceof JSONObject) {
                        String msg = ((JSONObject) obj).toJSONString();
                        System.err.println("Sending: " + ((JSONObject) obj)
                                .toJSONString
                                        ());
                        try {
                            out.writeUTF(msg);
                            String feedback = in.readUTF();
                            System.out.println("Received: " + feedback);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    } else {
                        Instruction inst = (Instruction) obj;
                        String msg = inst.ToJSON();
                        System.err.println("Sending: " + msg);
                        try {
                            out.writeUTF(msg);
                            String feedback = in.readUTF();
                            System.out.println("Received: " + feedback);
                            if (feedback.equals("Success")) {
                                continue;
                            } else if (feedback.equals("BlockUnavailableException")) {
                                Instruction upgraded = new NewBlockInstruction(
                                        (CopyBlockInstruction) inst);
                                String msg2 = upgraded.ToJSON();
                                System.err.println("Sending: " + msg2);

                                out.writeUTF(msg2);
                                feedback = in.readUTF();
                                System.out.println("Received: " + feedback);
                                if (feedback.equals("Success")) {
                                    continue;
                                }
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    protected class FileSync extends Thread {
        SynchronisedFile fromFile;
        Queue<Object> fileInstQueue = new LinkedList<>();

        FileSync(SynchronisedFile ff) {
            fromFile = ff;
        }

        @Override
        public void run() {
            Instruction inst;
            while ((inst = fromFile.NextInstruction()) != null) {
                fileInstQueue.offer(inst);
                if (inst.Type().equals("EndUpdate")) {
                    try {
                        SyncClient.putMsg(fileInstQueue);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    fileInstQueue.clear();
                }
            }
        }
    }
}
