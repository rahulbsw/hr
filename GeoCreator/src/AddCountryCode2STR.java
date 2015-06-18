import com.mongodb.*;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map;

/**
 * Created by rjain on 5/4/2015.
 */
public class AddCountryCode2STR {
    public static final String STR_DATABASE="locationhub";
    public static final String STR_COLLECTION="str";
    public static final String COUNTRY_COLLECTION="country";
    private static GetCountryByGeocode geo;

    public static void main(String[] args) throws IOException {
        MongoClient client = new MongoClient(new ServerAddress("localhost", 27017));
        DB locationhub = client.getDB(STR_DATABASE);
        DBCollection str=locationhub.getCollection(STR_COLLECTION);
        DBCollection countryCollection=locationhub.getCollection(COUNTRY_COLLECTION);
        DBObject countryCodeNotExists =new BasicDBObject("CountryCode",new BasicDBObject("$exists",false));
        System.out.println(countryCodeNotExists.toString());
       // DBCursor strDbCursor=str.find(countryCodeNotExists).snapshot();
        DBCursor strDbCursor=str.find().snapshot();
        geo=new GetCountryByGeocode();
        boolean isUpdate;
        while(strDbCursor.hasNext())
        {
            isUpdate=false;
            try {
                DBObject strDocument = strDbCursor.next();
                Double lat=geo.getDouble(strDocument.get("Latitude"));
                Double lng=geo.getDouble(strDocument.get("Longitude"));
                String country= (String)strDocument.get("Country");
                //System.out.format("Lat %f,Lng %f.%n", lat, lng);
                Map result= geo.findFirstByLatLong(lat, lng);
                String countryCode="";
                if (result != null && result.containsKey("CountryCode")) {
                    isUpdate=true;
                    countryCode=(String)result.get("CountryCode");
                }
                else
                {
                    System.out.format("BY GEO:Country Code not found for Lat %f,Lng %f and Country Name(%s).%n", lat, lng,country);
                    DBObject findCountryByName = new BasicDBObject("properties.NAME", country);
                    DBObject resultByName=countryCollection.findOne(findCountryByName);
                    if (resultByName!=null && resultByName.get("properties")!=null && ((Map)resultByName.get("properties")).get("ISO_A2")!=null) {
                        isUpdate = true;
                        countryCode = (String) ((Map)resultByName.get("properties")).get("ISO_A2");
                    }else {
                        System.out.format("By Name:Country Code not found for Lat %f,Lng %f and Country Name().%n", country, lat, lng);
                    }

                }

                if(isUpdate)
                {
                    DBObject _id = new BasicDBObject("_id", strDocument.get("_id"));
                    DBObject countryCodeToSet = new BasicDBObject("CountryCode", countryCode);
                    DBObject set = new BasicDBObject("$set", countryCodeToSet);
                    str.update(_id, set);
                }
            }catch (Exception e)
            {
                System.out.println(" Exception: "+e.getMessage());
            }

        }

    }
}
