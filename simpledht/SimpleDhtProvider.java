package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;

public class SimpleDhtProvider extends ContentProvider {
    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    private static final String DATABASE_NAME = "database1";
    private static final String TABLE_NAME = "table1";
    private static final String TABLE2_NAME = "table2";
    private static final int SERVER_PORT = 10000;
    private static final String KEY = "key";
    private static final String VALUE = "value";
    static Uri mUri;
    Cursor qResult = null;
    messageDatabase messagedb = null;
    SQLiteDatabase db = null;
    String[] hashOrdering;
    String[] ordering = new String[]{"11124", "11112", "11108", "11116", "11120"};
    ArrayList<String> activeRemotePorts = new ArrayList<String>();
    TelephonyManager tel;
    String portStr;
    String myPort;
    String[] remoteCursor = null;
    boolean remoteCursorDone = false;
    boolean lookForNeighbors = false;
    boolean lookForNeighborsDone = false;

    {
        try {
            hashOrdering = new String[]{genHash("5556"), genHash("5558"), genHash("5560"), genHash("5562"), genHash("5564")};
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    private ArrayList<String> cursorToArrayList(Cursor c) {
        // Method code sourced from https://stackoverflow.com/questions/1354006/how-can-i-create-a-list-array-with-the-cursor-data-in-android
        ArrayList<String> ArrayList = new ArrayList<String>();
        c.moveToFirst();
        while (!c.isAfterLast()) {
            ArrayList.add(c.getString(c.getColumnIndex("key"))); //add the item
            ArrayList.add(c.getString(c.getColumnIndex("value")));
            c.moveToNext();
        }
        return ArrayList;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        if (selection.equals("@")) {
            TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
            String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
            db.delete(TABLE_NAME, null, null);
            db.delete(TABLE2_NAME, null, null);
            Log.v(TAG, "'DELETED DATA ON " + myPort);
            return 0;
        } else if (selection.equals("*")) {
            db = messagedb.getWritableDatabase();
            db.delete(TABLE_NAME, null, null);
            db.delete(TABLE2_NAME, null, null);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DELETE");
            return 0;
        } else {
            db = messagedb.getWritableDatabase();
            db.delete(TABLE_NAME, '[' + KEY + "]='" + selection + "'", null);
            db.delete(TABLE2_NAME, '[' + KEY + "]='" + selection + "'", null);
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        checkForNeighbors();

        Log.v(TAG, "RECIEVED " + values.toString() + " TO INSERT");
        String k = values.getAsString("key");
        String kHash = null;
        try {
            kHash = genHash(k);
            Log.v(TAG, "key = " + k + " HASH = " + kHash);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        try {
            String correctStoragePort = findCorrectStoragePort(kHash);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "INSERT", correctStoragePort, values.toString());
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return null;
    }

    private void checkForNeighbors() {
        if (!lookForNeighbors) {
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "FIND");
        }
        lookForNeighbors = true;
        while (!lookForNeighborsDone) {

        }
    }

    private String findCorrectStoragePort(String kHash) throws NoSuchAlgorithmException {

        if (!order(kHash, genHash(getPortStr(activeRemotePorts.get(activeRemotePorts.size() - 1))))) {
            return activeRemotePorts.get(0);
        }
        for (String ports : activeRemotePorts) {
            String portCopy = ports;
            ports = getPortStr(ports);
            if (order(kHash, genHash(ports))) {
                return portCopy;
            }
        }
        return null;
    }

    private String getPortStr(String s) {
        int num = Integer.parseInt(s);
        num = num / 2;
        return String.valueOf(num);
    }

    private Boolean order(String a, String b) {
        /* IF String a is before b alphabetically,
            Return true;
            else
            Return False
        */
        int compare = a.compareTo(b);
        if (compare <= 0) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean onCreate() {
        initializaDatabase();
        return false;
    }

    private void initializaDatabase() {
        tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        String delete0 = "UPDATE " + TABLE_NAME + " SET [" + KEY + "] = ''";
        String delete1 = "UPDATE " + TABLE_NAME + " SET [" + VALUE + "] = ''";
        String delete00 = "UPDATE " + TABLE2_NAME + " SET [" + KEY + "] = ''";
        String delete10 = "UPDATE " + TABLE2_NAME + " SET [" + VALUE + "] = ''";
        messagedb = new messageDatabase(this.getContext());
        db = messagedb.getWritableDatabase();
        db.rawQuery(delete0, null);
        db.rawQuery(delete1, null);
        db.rawQuery(delete00, null);
        db.rawQuery(delete10, null);
        Arrays.sort(hashOrdering);
        mUri = buildUri("edu.buffalo.cse.cse486586.simpledht.provider", "content");
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (Exception e) {
        }
    }



    private Uri buildUri(String authority, String scheme) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        if (selection.equals("@")) {
            return localDataBase();
        }
        if (selection.equals("*")) {
            for (String storagePort : activeRemotePorts) {
                clearTempDatabase();
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "QUERY", storagePort, selection);
                while (!remoteCursorDone) {

                }
                remoteCursorDone = false;
                insertIntoTempDatabase(remoteCursor);
            }
            db = messagedb.getWritableDatabase();
            String s = "SELECT DISTINCT * FROM '" + TABLE2_NAME + "'";
            return db.rawQuery(s, null);

        }
        Boolean inMyDatabase = checkLocalDatabase(selection);
        if (inMyDatabase) {
            Log.v(TAG, selection + " WAS FOUND ON PORT " + portStr);
            String s = "SELECT DISTINCT " + "[" + KEY + "]," + VALUE + " FROM '" + TABLE_NAME + "' WHERE [" + KEY + "] = '" + selection + "';";
            qResult = db.rawQuery(s, null);
        } else {
            try {
                String storagePort = findCorrectStoragePort(genHash(selection));
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "QUERY", storagePort, selection);
                while (!remoteCursorDone) {

                }
                remoteCursorDone = false;
                insertIntoTempDatabase(remoteCursor);
                String s = "SELECT DISTINCT " + "[" + KEY + "]," + VALUE + " FROM '" + TABLE2_NAME + "' WHERE [" + KEY + "] = '" + selection + "';";
                qResult = db.rawQuery(s, null);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }

        return qResult;
    }

    private void clearTempDatabase() {
        db = messagedb.getWritableDatabase();
        String s = "Delete FROM '" + TABLE2_NAME + "'";
        db.rawQuery(s, null);
    }

    private Cursor localDataBase() {
        db = messagedb.getWritableDatabase();
        String s = "SELECT DISTINCT * FROM '" + TABLE_NAME + "'";
        return db.rawQuery(s, null);
    }

    private void insertIntoTempDatabase(String[] remoteCursor) {
        db = messagedb.getWritableDatabase();
        ContentValues cv = new ContentValues();
        for (int x = 0; x <= remoteCursor.length - 2; x += 2) {
            cv.put("key", remoteCursor[x]);
            cv.put("value", remoteCursor[x + 1]);
            db.insert(TABLE2_NAME, null, cv);
        }
    }

    private Boolean checkLocalDatabase(String selection) {
        db = messagedb.getReadableDatabase();
        String queryCheck = "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE [" + KEY + "] = '" + selection + "';";
        Cursor result = db.rawQuery(queryCheck, null);
        //dumpCursorToString found on stackoverflow
        //https://stackoverflow.com/questions/3105080/output-values-found-in-cursor-to-logcat-android
        String col = DatabaseUtils.dumpCursorToString(result);
        String found[] = col.split("=");
        int num = Integer.parseInt("" + found[1].charAt(0));
        if (num > 0) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public class messageDatabase extends SQLiteOpenHelper {

        //Code sourced from https://developer.android.com/training/data-storage/sqlite
        /*
                        public class FeedReaderDbHelper extends SQLiteOpenHelper {
                    // If you change the database schema, you must increment the database version.
                    public static final int DATABASE_VERSION = 1;
                    public static final String DATABASE_NAME = "FeedReader.db";

                    public FeedReaderDbHelper(Context context) {
                        super(context, DATABASE_NAME, null, DATABASE_VERSION);
                    }
                    public void onCreate(SQLiteDatabase db) {
                        db.execSQL(SQL_CREATE_ENTRIES);
                    }
                    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
                        // This database is only a cache for online data, so its upgrade policy is
                        // to simply to discard the data and start over
                        db.execSQL(SQL_DELETE_ENTRIES);
                        onCreate(db);
                    }
                    public void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) {
                        onUpgrade(db, oldVersion, newVersion);
                    }
                }
         */

        public messageDatabase(Context context) {
            super(context, DATABASE_NAME, null, 24);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {

            //Creating multiple tables sourced from https://stackoverflow.com/questions/41783000/how-to-create-multiple-tables-in-android-studio-database
            String CREATE_CMD = "CREATE TABLE " + TABLE_NAME + "([" + KEY + "] text ," + VALUE + " text " + ")";
            String CREATE_CMD2 = "CREATE TABLE " + TABLE2_NAME + "([" + KEY + "] text ," + VALUE + " text " + ")";

            db.execSQL(CREATE_CMD);
            db.execSQL(CREATE_CMD2);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
            db.execSQL("DROP TABLE IF EXISTS " + TABLE2_NAME);
            onCreate(db);
        }
    }

    class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {
            ServerSocket serverSocket = serverSockets[0];
            //Code modified and based on https://docs.oracle.com/javase/tutorial/networking/sockets/index.html
            //https://developer.android.com/reference/java/io/DataInputStream
            //https://developer.android.com/reference/java/io/BufferedReader
            //https://developer.android.com/reference/java/io/DataOutputStream
            //https://docs.oracle.com/javame/config/cldc/ref-impl/midp2.0/jsr118/java/io/DataOutputStream.html
            /* Code From Oracle link above

             BufferedReader in = new BufferedReader(new InputStreamReader(kkSocket.getInputStream()));
             */

            try {
                int count = 0;
                while (true) {
                    // count+=1;
                    Socket socket = serverSocket.accept();
                    BufferedReader textMessage = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String message = textMessage.readLine();
                    //2nd step of 3 way handshake. Socket needs to send message to client to let them know they received  a message.
                    DataOutputStream clientMessageReceived = new DataOutputStream(socket.getOutputStream());
                    TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
                    String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
                    String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
                    Log.e(TAG, "PORT " + myPort + " HAS RECIEVED " + message);
                    if (message != null) {
                        if (message.contains("AWAKE")) {
                            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                            out.println("YEA IM AWAKE");
                        }
                        if (message.contains("INSERT")) {
                            insertIntoDatabaseFromServer(message.split("SPACE")[1]);
                        }
                        if (message.contains("DELETE")) {
                            delete(mUri, "@", null);
                        }
                        if (message.contains("QUERY")) {
                            ArrayList<String> query = queryDatabaseFromServer(message.split("SPACE")[1]);

                            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                            out.println(query.toString().substring(1, query.toString().length() - 1));
                        }
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
            }

            return null;
        }

        private ArrayList<String> queryDatabaseFromServer(String selection) {
            db = messagedb.getReadableDatabase();
            if (selection.equals("@")) {
                String s = "SELECT DISTINCT * FROM '" + TABLE_NAME + "'";
                Cursor c = db.rawQuery(s, null);
                return cursorToArrayList(c);
            } else {
                String s = "SELECT DISTINCT * FROM '" + TABLE_NAME + "'";
                Cursor c = db.rawQuery(s, null);
                return cursorToArrayList(c);
            }
        }

        private void insertIntoDatabaseFromServer(String pair) {

            String[] divide = pair.split(" ");
            String value = divide[0].split("=")[1];
            String key = divide[1].split("=")[1];
            ContentValues cv = new ContentValues();
            cv.put("key", key);
            cv.put("value", value);
            db = messagedb.getWritableDatabase();
            db.insert(TABLE_NAME, null, cv);
            TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
            String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
            Log.e(TAG, "INSERTED " + pair + " @ " + myPort);
        }

        private String getValues(String data) {
            String[] s = data.split("\n");
            String key = s[2];
            String value = s[3];
            s = key.split("=");
            key = s[1];
            s = value.split("=");
            value = s[1];
            return key + "!" + value;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... strings) {
            String message = strings[0];
            if (message.contains("FIND")) {
                findNeighbors();
                Log.v(TAG, "ACTIVE PORTS ARE " + activeRemotePorts);
            }
            if (message.contains("INSERT")) {
                try {
                    sendToServerToInsert(message, strings[1], strings[2]);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (message.contains("DELETE")) {
                Log.v(TAG, "DELETING EVERYTHING");
                try {
                    deleteEverything();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (message.contains("QUERY")) {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(strings[1]));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    out.println("QUERY" + "SPACE" + strings[2]);
                    // sendToServerToQuery(message,strings[1],strings[2]);
                    String query = in.readLine();
                    remoteCursor = query.split(", ");
                    remoteCursorDone = true;

                } catch (IOException e) {
                    e.printStackTrace();
                }

            }

            return null;
        }

        private void sendToServerToQuery(String command, String port, String message) throws IOException {

        }

        private void deleteEverything() throws IOException {
            for (String ports : activeRemotePorts) {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(ports));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                out.println("DELETE");
            }
        }

        private void sendToServerToInsert(String command, String port, String message) throws IOException {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            out.println(command + "SPACE" + message);
        }

        private void findNeighbors() {
            TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
            String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
            for (String r : ordering) {
                if (r.equals(myPort)) {
                    activeRemotePorts.add(r);
                    continue;
                }
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(r));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader seq = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    out.println("AWAKE?");
                    Log.e(TAG, "SEEING IF PORT " + r + " IS ALIVE");
                    String status = seq.readLine();
                    Log.e(TAG, "Server responded with " + status);
                    if (status == null) {
                        continue;
                    }
                    if (!status.equals("YEA IM AWAKE")) {
                        continue;
                    }

                    activeRemotePorts.add(r);
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
            lookForNeighborsDone = true;
        }
    }
}
