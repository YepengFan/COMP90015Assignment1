package filesync;

/**
 * Created by yepengfan on 28/03/15.
 */

import static java.nio.file.StandardWatchEventKinds.*;
import static java.nio.file.LinkOption.*;

import java.net.*;
import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.util.*;

/**
 * Implement TCP Protocol - Client Side
 */

public class SyncClient {
    private static Socket socket = null;
    private static int serverPort = 4444; // default port is 4444
    private static DataInputStream in = null;
    private static DataOutputStream out = null;

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
                    SynchronisedFile fromFile = new SynchronisedFile
                            (String.valueOf(child));
                    fromFile.CheckFileState();
                    Thread stt = new Thread(new FileSync(fromFile));
                    stt.start();
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(-1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    System.exit(-1);
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

    public static void main(String[] args) throws IOException {
        String hostname = args[1];
        socket = new Socket(hostname, serverPort);
        System.out.println("Connection Established");
        in = new DataInputStream(socket.getInputStream());
        out = new DataOutputStream(socket.getOutputStream());
        boolean recursive = false;
        Path dir = Paths.get(args[0]);
        new SyncClient(dir, recursive).processEvents();

    }

    protected class FileSync implements Runnable {
        SynchronisedFile fromFile;

        FileSync(SynchronisedFile ff) {
            fromFile = ff;
        }

        @Override
        public synchronized void run() {
            Instruction inst;
            while ((inst = fromFile.NextInstruction()) != null) {
                String msg = inst.ToJSON();
                System.err.println("Sending: " + msg);
                // Client sends the message to the Server
                try {
                    out.writeUTF(msg);
                    String data = in.readUTF();
                    System.out.println("Received: " + data);
                } catch (IOException e) {
                    System.out.println(e.getMessage());
                }
            }
        }
    }
}
