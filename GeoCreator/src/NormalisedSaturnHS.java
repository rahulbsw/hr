import com.ipass.firefly.hotel.util.Util;
import com.mongodb.*;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by rjain on 6/16/2015.
 */
public class NormalisedSaturnHS {
     private static String saturnHS="saturn_hs";
     private static String hoteldatabase = "locationhub";


    public static void main(String[] args) {

        try {
            MongoClient client = new MongoClient(new ServerAddress("localhost", 27017));
            DB popdb = client.getDB(hoteldatabase);
            DBCollection saturnHSColl = popdb.getCollection(saturnHS);
            DBCollection saturnHSNormColl = popdb.getCollection( Util.join('_',saturnHS,"norm"));

            DBCursor cursor=saturnHSColl.find();
            try {
                saturnHSNormColl.drop();
            } catch (Exception e) {
                System.out.println(e.toString());
            }
            while(cursor.hasNext())
            {
                DBObject orgObject=cursor.next();
                normalisedSaturnHS(saturnHSNormColl,orgObject,"HuntGroup");
                normalisedSaturnHS(saturnHSNormColl,orgObject,"CalledStationId");

            }

            saturnHSNormColl.createIndex(BasicDBObjectBuilder.start().add("LOCATION_TYPE", 1).add("LOCATION_VALUE", 1).get());

        } catch (Exception e) {
            System.out.println(e.toString());
            System.out.println("****** initialize Mongo Collection Failed at " + DateFormat.getDateTimeInstance(DateFormat.FULL, DateFormat.FULL).format(new Date()) + "******");
            e.printStackTrace();
            System.exit(-1);
        }
    }

    static void normalisedSaturnHS(DBCollection narmSaturnHS,DBObject orgObject,String location_type)
    {
        String attributeName=(location_type=="HuntGroup")?"HUNT_GROUP":"CALLED_STATION_ID";
        if(!orgObject.containsField("SSID")||orgObject.get("SSID")==null)
        {
            return ;
        }
        orgObject.removeField("_id");
        if(orgObject.containsField(attributeName))
        {
            String attributeValueStr=Util.getField(orgObject,attributeName,"");
            String[] attributeValues;
            if (attributeValueStr.contains(";"))
            {
                attributeValues=attributeValueStr.split(";");
            }else if (attributeValueStr.contains("\\|"))
            {
                attributeValues=attributeValueStr.split("\\|");
            }
            else
            {
                attributeValues=attributeValueStr.split(",");
            }
            for (String attributeValue : attributeValues) {
                if (attributeValue!=null && !attributeValue.isEmpty()) {
                    DBObject normSaturnHS = new BasicDBObject();
                    normSaturnHS.put("LOCATION_TYPE", location_type);
                    normSaturnHS.put("LOCATION_VALUE", attributeValue);
                    normSaturnHS.putAll(orgObject);
                    narmSaturnHS.insert(normSaturnHS);
                }
            }

        }
    }

}
