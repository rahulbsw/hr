import com.mongodb.*;
import com.mongodb.gridfs.GridFS;

import java.net.UnknownHostException;

/**
 * Created by rjain on 5/7/2015.
 */
public class ExtactGridFsFiles {
    public static final String DATABASE="express";
    public static final String COLLECTION="fs.files";
    public static final String COUNTRY_COLLECTION="country";

    public static void main(String[] args) throws UnknownHostException {

        MongoClient client = new MongoClient(new ServerAddress("localhost", 27017));
        DB db = client.getDB(DATABASE);
        GridFS fs =new GridFS(db);
        DBCursor files=fs.getFileList();
        while(files.hasNext())
        {
           DBObject  fileMeta=  files.next();
           String filename=(String)fileMeta.get("filename");
            if(filename!=null && filename.matches("[0-9]*_[0-9]{4}_[0-9]{1,2}_[0-9]{1,2}_[0-9]{1,2}\\.xls"))
            {
                System.out.println(fileMeta.toString());
            }
        }

    }


}
