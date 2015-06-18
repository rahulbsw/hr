import com.mongodb.*;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by rjain on 5/11/2015.
 */
public class IPassLocationMapper
{

    public static final String LOCATION_DATABASE="locationhub";
    public static final String LOC_COLLECTION="location";
    public static final String SARTURN_HS_COLLECTION="saturn_hs";
    public static final String SARTURN_LOCATION_COLLECTION="saturn_with_location";
    private static Map<Integer,SortedMap<Integer,LocationType>> provLocTypeCache=new HashMap<Integer,SortedMap<Integer,LocationType>> () ;
    private  DB locationhub;
    private  DBCollection satrunLocationCollection;
    private  DBCollection locationCollection;
    private  DBCollection satrunHsCollection;
    private DBCollection diag;
    private void prepareProvLocTypeCache(DB db)
    {
        DBCollection provLocType = db.getCollection("prov_loc_type");

        DBCursor cur=provLocType.find().sort(new BasicDBObject("PROVIDER_ID", 1));
        System.out.println(cur.toString());
        Integer lastProviderId=-1;
        SortedMap<Integer,LocationType> locationRules=new TreeMap<Integer,LocationType>();
        while(cur.hasNext())
        {
           DBObject obj=cur.next();
            Integer provider_id=(Integer)obj.get("PROVIDER_ID");

            if(!lastProviderId.equals(provider_id) ) {
                if (lastProviderId!=-1 )
                   provLocTypeCache.put(lastProviderId, locationRules);
                locationRules=new TreeMap<Integer,LocationType>();
                lastProviderId = provider_id;
            }
            LocationType locationType=LocationType.getLocationType((String)obj.get("LOCATION_TYPE"));
            if (LocationType.ISP.equals(locationType) )
                locationRules.put(10,locationType);
            else
                locationRules.put((Integer)obj.get("PRIORITY"),locationType);
        }
        if(lastProviderId!=-1)
          provLocTypeCache.put(lastProviderId, locationRules);
    }

    public IPassLocationMapper() throws UnknownHostException {
        MongoClient client = new MongoClient(new ServerAddress("localhost", 27017));
        locationhub = client.getDB(LOCATION_DATABASE);
        satrunLocationCollection = locationhub.getCollection(SARTURN_LOCATION_COLLECTION);
        locationCollection = locationhub.getCollection(LOC_COLLECTION);
        satrunHsCollection = locationhub.getCollection(SARTURN_HS_COLLECTION);
        diag = locationhub.getCollection("DIAG");
        satrunLocationCollection.drop();
        diag.drop();
        prepareProvLocTypeCache(locationhub);
    }

    public static void main(String[] args) throws IOException {
        new IPassLocationMapper().doMapping();
    }
    public void doMapping() throws IOException {



        DBObject satrunHsOfHotelFilter = new BasicDBObject("SITE_TYPE", "HOTEL");
        DBCursor satrunHsOfHotel = satrunHsCollection.find(satrunHsOfHotelFilter).snapshot().addOption(Bytes.QUERYOPTION_NOTIMEOUT);

        boolean isHGFound=false;
        boolean isCSIDFound=false;
        boolean isNSIPFound=false;
        boolean isISPFound=false;
        boolean isPrefixFound=false;
        boolean isFoundRule=true;
        SortedMap<Integer,LocationType> locationRules=new TreeMap<Integer,LocationType>();
        while (satrunHsOfHotel.hasNext())
        {
             isHGFound=false;
             isCSIDFound=false;
             isNSIPFound=false;
             isISPFound=false;
             isPrefixFound=false;
            isFoundRule=false;
            boolean isFoundAtleastOne=false;
            DBObject hs=satrunHsOfHotel.next();

            // Provider Id Data type are different in each object of mongo convert into Integer
            Integer provider_id=getInteger(hs.get("PROVIDER_ID"));

            // get Location Rules from Cache
            if(hs.containsField(LocationType.ISP.name))
                locationRules=provLocTypeCache.get(provider_id);

            int counter =0;

            // Iterate through location Rules by priority order
            for(SortedMap.Entry entry: locationRules.entrySet())
            {
              counter++;

              if( LocationType.CALLED_STATION_ID.equals(entry.getValue())) {
                  isCSIDFound = hs.containsField(LocationType.CALLED_STATION_ID.name);
              }
              else if ( LocationType.NAS_IP.equals(entry.getValue())) {
                  isNSIPFound = hs.containsField(LocationType.NAS_IP.name);
              }
              else if ( LocationType.HUNT_GROUP.equals(entry.getValue())) {
                  isHGFound = hs.containsField(LocationType.HUNT_GROUP.name);
              }
              else if ( LocationType.PREFIX.equals(entry.getValue())) {
                  isPrefixFound = hs.containsField(LocationType.PREFIX.name);
              }
              else if ( LocationType.ISP.equals(entry.getValue())) {
                  isISPFound = hs.containsField(LocationType.ISP.name);
              }

             Object locationValue="Other";
             Object locationType=LocationType.OTHER.name;

             if(isCSIDFound) {
                 locationType=LocationType.CALLED_STATION_ID.type;
                 locationValue =  hs.get(LocationType.CALLED_STATION_ID.name);
             }
             else if(isNSIPFound) {
                 locationType=LocationType.NAS_IP.type;
                 locationValue =  hs.get(LocationType.NAS_IP.name);
             }
             else if(isHGFound) {
                 locationType=LocationType.HUNT_GROUP.type;
                 locationValue =  hs.get(LocationType.HUNT_GROUP.name);
             }
             else if(isPrefixFound) {
                 locationType=LocationType.PREFIX.type;
                 locationValue =  hs.get(LocationType.PREFIX.name);
             }
             else if(isISPFound ){
                 locationType=LocationType.ISP.type;
                locationValue = hs.get(LocationType.ISP.name);
             }

//                if (provider_id ==1042378 && getString(locationValue).equals("00-50-E8-00-8C-34:19 ;00-50-E8-00-8C-34:15"))
//                {
//                    //for debug
//                    System.out.println();
//                }

            int cnt=0;
            // Normal Insert
            isFoundRule=insertSaturnWithLocation(hs,provider_id,locationType,locationValue);
            //Separated by ;
            isFoundRule=multiValueLocationInsert(isFoundRule, hs, provider_id, locationType, locationValue, ";");
            //Separated by space
            isFoundRule=multiValueLocationInsert(isFoundRule,hs,provider_id,locationType,locationValue," ");

            if(isFoundRule)  break;
              isFoundRule=false;
             }


            if(!isFoundRule)
            {
                BasicDBObjectBuilder saturn=BasicDBObjectBuilder.start("SATURN_HS",hs);
                diag.insert(saturn.get());
            }
        }

    }

private boolean insertSaturnWithLocation(DBObject hs,Integer providerId,Object  locationType,Object locationValue)
{

    BasicDBObjectBuilder queryObject=BasicDBObjectBuilder.start("PROVIDER_ID", providerId).append("LOCATION_TYPE", locationType).append("LOCATION_VALUE", ((locationValue instanceof  String)?((String) locationValue).trim():locationValue));
    DBCursor findLocation=locationCollection.find(queryObject.get());

    int cnt=0;
    boolean isFoundRule=false;
    while(findLocation.hasNext())
    {
        isFoundRule=true;
        satrunLocationCollection.insert(BasicDBObjectBuilder.start("SATURN_HS", hs).append("LOCATION", findLocation.next()).get());
        cnt++;
        if(cnt>10) {
            System.out.println(queryObject.get());
            System.out.println(hs);
            System.out.println(findLocation.curr());
        }
    }
    return isFoundRule;
}

private boolean  multiValueLocationInsert(boolean isFoundRule,DBObject hs,Integer providerId,Object  locationType,Object locationValue,String separator)
{   boolean isFoundRuleLocal=isFoundRule;
    boolean isFoundAtleastOne=false;
    if (!isFoundRuleLocal && getString(locationValue).contains(separator))
    {
        isFoundAtleastOne=false;
        for(String locationVal : getString(locationValue).split(separator))
        {
            isFoundRuleLocal=insertSaturnWithLocation(hs,providerId,locationType,locationVal.trim());
            if (isFoundRuleLocal)isFoundAtleastOne=true;
            if(!isFoundRuleLocal)
            {
                hs.put(LocationType.getLocationType(getString(locationValue)).name,locationVal.trim());
                BasicDBObjectBuilder saturn=BasicDBObjectBuilder.start("SATURN_HS",hs);
                diag.insert(saturn.get());
            }
        }
        //if one of them is found then mark found
        if(isFoundAtleastOne) isFoundRuleLocal=true;
    }
    return  isFoundRuleLocal;
}

private String getString(Object val)
{
    if (val instanceof String)
         return (String)val;
    else if (val instanceof Integer)
        return ((Integer)val).toString();
    else if (val instanceof Double)
        return ((Double)val).toString();
    else if (val instanceof Long)
        return ((Long)val).toString();
    else
        return val.toString();
}

private Integer getInteger(Object val)
{
    if (val instanceof Integer)
        return (Integer)val;
    else if (val instanceof Double)
        return new Integer(((Double) val).intValue());
    else if (val instanceof Long)
        return new Integer(((Long) val).intValue());
    else if (val instanceof Float)
        return new Integer(((Float) val).intValue());
    else if (val instanceof String)
        return new Integer(((String) val).trim());
    else
        return null;
}

}
