package filesync;

/**
 * Created by yepengfan on 28/03/15.
 */

import java.net.*;
import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.util.*;

import org.json.simple.JSONObject;

import static java.nio.file.StandardWatchEventKinds.*;
import static java.nio.file.LinkOption.*;

/**
 * Implement TCP Protocol - Client Side
 */

public class SyncClient {
    private static Socket socket = null;
    private static int serverPort = 4444; // default port is 4444
    private static DataInputStream in = null;
    private static DataOutputStream out = null;

    private static Object lock = new Object();
    private boolean processing = false;

    private static Map<String, FileSync> threadMapper = new HashMap<>();

    private final WatchService watcher;
    private final Map<WatchKey, Path> keys;
    private final boolean recursive;
    private boolean trace = false;

    @SuppressWarnings("unchecked")
    static <T> WatchEvent<T> cast(WatchEvent<?> event) {
        return (WatchEvent<T>) event;
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
                    threadMapper.get(String.valueOf(child.getFileName())).fromFile
                            .CheckFileState();
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
                        .getAbsolutePath()), lock);
                fileSync.start();
                threadMapper.put(file.getName(), fileSync);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    public static void main(String[] args) throws IOException {
        String hostname = args[1];
        String directory = args[0];

        socket = new Socket(hostname, serverPort);
        System.out.println("Connection Established");
        in = new DataInputStream(socket.getInputStream());
        out = new DataOutputStream(socket.getOutputStream());
        boolean recursive = false;
        Path dir = Paths.get(directory);
        SyncClient client = new SyncClient(dir, recursive);
        client.indexingDirectory(directory);
        client.processEvents();
    }

    protected class FileSync extends Thread {
        SynchronisedFile fromFile;
        Object lock;

        FileSync(SynchronisedFile ff, Object lock) {
            fromFile = ff;
            this.lock = lock;
        }

        @Override
        public void run() {
            Instruction inst;
            while((inst = fromFile.NextInstruction()) != null) {
                String msg = inst.ToJSON();
                System.err.println("Sending: " + msg);
                try {
                    out.writeUTF(msg);

                    String feedback = in.readUTF();
                    System.out.println("Received: " + feedback);
                     if (feedback.equals("BlockUnavailableException")) {
                        Instruction upgraded = new NewBlockInstruction(
                                (CopyBlockInstruction) inst);
                        String msg2 = upgraded.ToJSON();
                        System.err.println("Sending: " + msg2);

                        out.writeUTF(msg2);
                        feedback = in.readUTF();
                        System.out.println("Received: " + feedback);
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
    }
}
