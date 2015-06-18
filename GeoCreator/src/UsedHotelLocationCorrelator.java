import com.ipass.firefly.hotel.regex.GrokMatcher;
import com.ipass.firefly.hotel.regex.Matcher;
import com.ipass.firefly.hotel.util.Util;
import com.mongodb.*;


import java.text.DateFormat;
import java.text.DecimalFormat;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by rjain on 5/21/2015.
 */
public class UsedHotelLocationCorrelator {

    public static double Nearest = 100000;
    static String hoteldatabase = "locationhub";
    static String strcollection = "str";
    static String hotelcollection = "hotel_location_data";
    //static String hotelcollection = "UsedHotelLocation6Month";
    static String ssidHotelParentCompany = "ssidHotelParentCompany";
    static String strlatAttrName = "Latitude";
    static String strlngAttrName = "Longitude";
    static String hotellatAttrName = "LAT";
    static String hotellngAttrName = "LNG";
    static Double hotellat, hotellng, strlat, strlng;
    static int sample = 0;
    static int geomismatches = 0;
    static ParentHotelCompany parentHotelCompany=null;
    String hotelLocation = "Locations";
    DBCollection hotelColl;
    DBCollection strColl;
    DBCollection parenHotelColl;
    DBCollection ssidHotelParentCompanyColl;
    DBCollection parenHotelPosibleColl;
    DBCollection strParenHotelColl;
    char SEPARATOR = '_';
    DB popdb;

    private String hotelParentCompanyCode;
    private String hotelParentCompany;
    private GrokMatcher grokMatcher;
    private Double maxRadius=0.200;


    public UsedHotelLocationCorrelator(String hotelParentCompany) {
        this.hotelParentCompany = hotelParentCompany;
        parentHotelCompany=ParentHotelCompany.get(hotelParentCompany);
        this.hotelParentCompanyCode = parentHotelCompany.getCode();
        initializeMongoCollection();
    }

    public static void main(String[] args) {
        UsedHotelLocationCorrelator obj = new UsedHotelLocationCorrelator("Marriott");
        //obj.prepareParentHotelCollection();
        //obj.prepareStrByParentCompanyName();
        //obj.associateSTR(0.200);
       // obj.associateSTR(0.200, true);
        //obj.associateSTR(2.000);
        obj.associateSTR(2.000, true);
    }

    /**
     * initialize Mongodb client and Collection object
     */
    private void initializeMongoCollection() {
        try {
            MongoClient client = new MongoClient(new ServerAddress("localhost", 27017));
            popdb = client.getDB(hoteldatabase);
            hotelColl = popdb.getCollection(hotelcollection);
            strColl = popdb.getCollection(strcollection);
            ssidHotelParentCompanyColl = popdb.getCollection(this.ssidHotelParentCompany);

            String hotelParentLocationInfo=Util.join(SEPARATOR,this.hotelParentCompanyCode,hotelLocation).toLowerCase();
            String posibleMatch=Util.join(SEPARATOR, "p", hotelParentLocationInfo).toLowerCase();
            String hotelStr=Util.join(SEPARATOR, hotelParentCompanyCode,"str").toLowerCase();
            parenHotelColl = popdb.getCollection(hotelParentLocationInfo);
            parenHotelPosibleColl = popdb.getCollection(posibleMatch);
            strParenHotelColl = popdb.getCollection(hotelStr);


        } catch (Exception e) {
            System.out.println(e.toString());
            System.out.println("****** initialize Mongo Collection Failed at " + DateFormat.getDateTimeInstance(DateFormat.FULL, DateFormat.FULL).format(new Date()) + "******");
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public void associateSTR(Double maxRadius) {
        associateSTR(maxRadius,false);
    }


    public void associateSTR(Double maxRadius,Boolean includeAllStrMatches) {
        this.maxRadius=(maxRadius!=null)?maxRadius:this.maxRadius;
        try {
            String radiusAppend=  new DecimalFormat("0000").format(this.maxRadius * 1000);

            String hotelSTRParentLocationInfo=Util.join(this.SEPARATOR ,this.hotelParentCompanyCode,hotelLocation,radiusAppend,((includeAllStrMatches)?"extended":null)).toLowerCase();
            String unMatchedLocationInfo=Util.join(this.SEPARATOR ,"unmatched",this.hotelParentCompanyCode,hotelLocation,radiusAppend,((includeAllStrMatches)?"extended":null)).toLowerCase();
            DBCollection  strLocationMatch = popdb.getCollection(hotelSTRParentLocationInfo);
            DBCollection  unMatchedLocation = popdb.getCollection(unMatchedLocationInfo);
            try {
                strLocationMatch.drop();
            } catch (Exception e) {
                System.out.println(e.toString());
            }

            try {
                unMatchedLocation.drop();
            } catch (Exception e) {
                System.out.println(e.toString());
            }

            // Select everything
            // find hotel recs with GPS
            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("STR", new BasicDBObject("$exists", "false"));
            //searchQuery.put("REGION", "Europe");
            DBCursor cursor = parenHotelColl.find();
            System.out.println("****** Associate STR - Job Started at " + DateFormat.getDateTimeInstance(DateFormat.FULL, DateFormat.FULL).format(new Date()) + "******");

            // Process every item and create a "where" geospatial element [longitude,latitude]
            DBObject item = null;
            int updates = 0;
            int candidates = cursor.count();
            cursor.addOption(Bytes.QUERYOPTION_NOTIMEOUT);
            System.out.println("****** Processing " + candidates + " hotels******");
            while (cursor.hasNext()) {
                sample++;
                hotellat = null;
                hotellng = null;
                item = cursor.next();
                if (item.containsField("APMAC")&& Util.getField(item, "APMAC").equals("2CE6CC2A8428") )
                    System.out.println();
                String Location_id = Util.getField(item, "LOCATION_ID");
                BasicDBObject thishotel = new BasicDBObject("_id", item.get("_id"));
                // Capture the hotel lat/long
                String lat1 = Util.getField(item, hotellatAttrName);
                String lng1 = Util.getField(item, hotellngAttrName);
                if (lat1 != null && lat1.length() > 0)
                    hotellat = Double.valueOf(Util.getField(item, hotellatAttrName));
                if (lng1 != null && lng1.length() > 0)
                    hotellng = Double.valueOf(Util.getField(item, hotellngAttrName));
                if (hotellat == null || hotellng == null)
                {
                    item.put("STATUS","NO_GEO");
                    unMatchedLocation.insert(item);
                    continue;
                }
                // Save geopoint for hotel
                Object where = item.get("where");


                // Formulate a geoquery to find STR items near this hotel
                boolean isFound = false;
                boolean isIncreaseOnce = false;
                boolean isDecreaseOnce = false;
                double radius = maxRadius; // match within radius in kilometers
                if(item.containsField("STRID") && item.get("STRID")!=null )
                {
                    BasicDBObject target = new BasicDBObject("STRID", new Integer(Util.getField(item,"STRID")));
                    DBCursor str_cursor= strParenHotelColl.find(target);
                    if (str_cursor.count() >= 1) {
                        DBObject str = str_cursor.next();
                        strlat = Double.valueOf(Util.getField(str, strlatAttrName));
                        strlng = Double.valueOf(Util.getField(str, strlngAttrName));
                        Double distance = Util.gps2m(hotellat, hotellng, strlat, strlng);
                        str.put("distance", distance);
                        strLocationMatch.insert(simplifyObject(item,str,Boolean.TRUE));
                        updates++;
                        str_cursor.close();
                        isFound = true;
                    }
                }
                else {
                while (radius <= maxRadius && radius > 0) {
                    DBCursor str_cursor = getDBCursor(where,radius );;
                    boolean foundOne=false;
                    if (!(isDecreaseOnce && isIncreaseOnce)) {
                        if (!foundOne && str_cursor.count() > 1) {
                            int matchCounter=0;
                            DBObject data=null;
                            while (str_cursor.hasNext())
                            {
                                data=simplifyObject(item,str_cursor.next());
                                if(Boolean.valueOf(Util.getField(data, "isMatched")))
                                {
                                    matchCounter++;
                                }
                            }
                            if (matchCounter>1) {
                                radius = radius - 0.020;
                                str_cursor.close();
                                isDecreaseOnce = true;
                                continue;
                            }else  if (matchCounter==1)
                            {
                                strLocationMatch.insert(data);
                                updates++;
                                isFound = true;
                                foundOne=true;
                                str_cursor.close();
                                break;
                            }
                            else{
                                radius = radius + 0.020;
                                str_cursor.close();
                                isIncreaseOnce = true;
                                continue;
                            }
                        }
                        if (str_cursor.count() < 1) {
                            radius = radius + 0.020;
                            str_cursor.close();
                            isIncreaseOnce = true;
                            continue;
                        }
                    } else {

                    if (str_cursor.count() < 1) {
                            str_cursor.close();
                            radius = radius + 0.020;
                            str_cursor = getDBCursor(where,radius );
                        }
                     if (str_cursor.count() > 1) {

                            Double previousDistnace = 9999999.00;
                            DBObject closestStr = null;
                            while (str_cursor.hasNext()) {
                                DBObject str = str_cursor.next();
                                strlat = Double.valueOf(Util.getField(str, strlatAttrName));
                                strlng = Double.valueOf(Util.getField(str, strlngAttrName));
                                Double distance = Util.gps2m(hotellat, hotellng, strlat, strlng);
                                str.put("distance", distance);
                                if(includeAllStrMatches) //add all matched str for that hotel group
                                {

                                    strLocationMatch.insert(simplifyObject(item,str));
                                    updates++;
                                    isFound = true;
                                }else if (previousDistnace >= distance) {  // look for Closest Hotel from the geo point
                                    closestStr = str;
                                    previousDistnace = distance;
                                }
                            }

                           if(!includeAllStrMatches)  // Add Closest Hotel from the geo point
                           {
                            strLocationMatch.insert(simplifyObject(item,closestStr));
                            updates++;
                            str_cursor.close();
                            isFound = true;
                           }else
                           {
                             str_cursor.close();
                           }
                            break;
                      }else if(str_cursor.count() < 1)
                       { break;
                       }
                    }

                    if (str_cursor.count() == 1) {
                        DBObject str = str_cursor.next();
                        strlat = Double.valueOf(Util.getField(str, strlatAttrName));
                        strlng = Double.valueOf(Util.getField(str, strlngAttrName));
                        Double distance = Util.gps2m(hotellat, hotellng, strlat, strlng);
                        str.put("distance", distance);
                        strLocationMatch.insert(simplifyObject(item,str,Boolean.TRUE));
                        updates++;
                        str_cursor.close();
                        isFound = true;
                        break;
                    }

                }
                }
                if (!isFound)
                {
                    item.put("STATUS","NO_MATCH");
                    unMatchedLocation.insert(item);
                   // System.out.println("[" + sample + "] L:" + Location_id + " [lat,lng]:[" + hotellat + ", " + hotellng + "] ");
                }

            }
            System.out.println("****** Associate STR - Job Completed with " + updates + " updates to " + candidates + " with " + geomismatches + " geo-mismatches at " + DateFormat.getDateTimeInstance(DateFormat.FULL, DateFormat.FULL).format(new Date()) + "******");

        } catch (Exception e) {
            System.out.println(e.toString());
            System.out.println("****** Associate STR - Job Failed at " + DateFormat.getDateTimeInstance(DateFormat.FULL, DateFormat.FULL).format(new Date()) + "******");
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private DBCursor getDBCursor(Object where,Double radius)
    {
        BasicDBList sphere = new BasicDBList();
        sphere.put("0", where);
        sphere.put("1", radius / 3959.0);
        BasicDBObject within = new BasicDBObject("$centerSphere", sphere);
        BasicDBObject geo = new BasicDBObject("$geoWithin", within);
        //BasicDBObject parentCompany=new BasicDBObject("Parent Company",this.hotelParentCompany);
        //BasicDBObject whereGeo = new BasicDBObject("where",geo);
        //BasicDBList target=new BasicDBList();
        //target.add(parentCompany);
        //target.add(whereGeo);
        BasicDBObject target = new BasicDBObject("where", geo);
        return strParenHotelColl.find(target);
    }


    /*
       Get Parent Hotel Company by ssid
       @param ssid
     */
    private List<String> getParentHotelCompany(String ssid) {
        List<String> parentCompanies = new ArrayList<String>();
        DBCursor cursor = ssidHotelParentCompanyColl.find(new BasicDBObject("SSID", ssid));
        while (cursor.hasNext())
            parentCompanies.add(ParentHotelCompany.generateCode((String) cursor.next().get("PARENTCOMPANY")));
        return parentCompanies;
    }


     /*
      prepare Parent Hotel Location data Collection.
      By appling Below rules
         1) Check if SSID belows to Parent Hotel Company
            IF yes and return  Parent Hotel Companies list size is 1 then
                  insert the location info to Parent Hotel Location data Collection
            IF Yes and return Parent Hotel Companies list size is >1 then
                 if GIS_LOC_ID ,GIS_LOC_DESC and WISPR_NAME  matches to   Parent Hotel Company regex  then
                         insert the location info to Parent Hotel Location data Collection
                 else into  insert the location info to Parent Hotel Location Posible data Collection
     */
    private void prepareParentHotelCollection() {
        System.out.println("****** prepare Parent Hotel Collection - Job Started with for Parent Hotel " + this.hotelParentCompany + " " + DateFormat.getDateTimeInstance(DateFormat.FULL, DateFormat.FULL).format(new Date()) + "******");
        DBCursor cursor = hotelColl.find().snapshot();
        int success = 0;
        int possible = 0;
        try {
            parenHotelColl.drop();
        } catch (Exception e) {
            System.out.println(e.toString());
        }
        try {
            parenHotelPosibleColl.drop();
        } catch (Exception e) {
            System.out.println(e.toString());
        }

        while (cursor.hasNext()) {
            DBObject hotelObj = cursor.next();
            hotelObj.removeField("STR");
            String ssid = (String) hotelObj.get("SSID");
            List<String> HotelCompanies = getParentHotelCompany(ssid);
            if (HotelCompanies == null || HotelCompanies.isEmpty()) {
                continue;
            } else if (HotelCompanies.contains(this.hotelParentCompanyCode)) {
                if (HotelCompanies.size() > 1) {
                    String Search = Util.join('#', Util.getField(hotelObj, "WISPR_LOC_NAME"),
                            Util.getField(hotelObj, "GIS_LOC_DESC"),
                            Util.getField(hotelObj, "GIS_LOC_ID"),
                            Util.getField(hotelObj, "field9"),
                            Util.getField(hotelObj, "field10"),
                            Util.getField(hotelObj, "field11"),
                            Util.getField(hotelObj, "field12"),
                            Util.getField(hotelObj, "field13"),
                            Util.getField(hotelObj, "field14"),
                            Util.getField(hotelObj, "field15"));

                    if (parentHotelCompany.isMatchesWithParentCompanyRegex(Search)) {
                        parenHotelColl.insert(hotelObj);
                        success++;
                    } else {
                        parenHotelPosibleColl.insert(hotelObj);
                        possible++;
                    }
                } else {
                    parenHotelColl.insert(hotelObj);
                    success++;
                }


            }

        }
        System.out.println("****** prepare Parent Hotel Collection - Job Completed with for Parent Hotel " + this.hotelParentCompany + ",Matches:" + success + "  and  " + possible + " possibles " + DateFormat.getDateTimeInstance(DateFormat.FULL, DateFormat.FULL).format(new Date()) + "******");


    }




    private void prepareStrByParentCompanyName() {
        System.out.println("****** prepare Parent Hotel STR Collection - Job Started with for Parent Hotel " + this.hotelParentCompany + " " + DateFormat.getDateTimeInstance(DateFormat.FULL, DateFormat.FULL).format(new Date()) + "******");
        try {
            strParenHotelColl.drop();
        } catch (Exception e) {
            System.out.println(e.toString());
        }
        int inserted = 0;
        DBCursor cursor = strColl.find( parentHotelCompany.getQueryObject());
        while (cursor.hasNext()) {
            DBObject str = cursor.next();
//            String hotelName = com.ipass.firefly.hotel.util.Util.getField(str, "Hotel Name");
//            if (isMatchesWithParentCompanyRegex(hotelName)) {
                strParenHotelColl.insert(str);
                inserted++;
            //}
        }
        BasicDBObject geoindex = new BasicDBObject("where", "2dsphere");
        strParenHotelColl.createIndex(geoindex);
        System.out.println("****** prepare Parent Hotel STR Collection - Job Completed with for Parent Hotel " + this.hotelParentCompany + ", No Of hotels " +inserted + " at " + DateFormat.getDateTimeInstance(DateFormat.FULL, DateFormat.FULL).format(new Date()) + "******");
    }

    private void destroy()
    {
        //hotelColl.
    }

    private  DBObject simplifyObject(DBObject item,DBObject str)
    {
       return simplifyObject( item, str,Boolean.FALSE);
    }
    private  DBObject simplifyObject(DBObject item,DBObject str,Boolean checkForIsMatched)
    {
        //prepare joined GIS_LOC_ID
        DBObject location =item;
        String GIS_LOC_ID = Util.join(':', Util.getField(location, "GIS_LOC_ID"),
                Util.getField(location, "field9"),
                Util.getField(location, "field10"),
                Util.getField(location, "field11"),
                Util.getField(location, "field12"),
                Util.getField(location, "field13"),
                Util.getField(location, "field14"),
                Util.getField(location, "field15"));

        //remove below attributes it will be replaced by joined GIS_LOC_ID
        location.removeField("GIS_LOC_ID");
        location.removeField("field9");
        location.removeField("field10");
        location.removeField("field11");
        location.removeField("field12");
        location.removeField("field13");
        location.removeField("field14");
        location.removeField("field15");

        //remove below attributes
        location.removeField("_id");
        location.removeField("where");

        location.put("GIS_LOC_ID", GIS_LOC_ID);

        //str.removeField("_id");
        location.putAll(str.toMap());
        location.removeField("_id");
        String search = Util.join('#',GIS_LOC_ID,
                Util.getField(location, "GIS_LOC_DESC"),
                Util.getField(location, "WISPR_LOC_NAME"),
                Util.getField(location, "SSID"));
        if (checkForIsMatched)
          location.put("isMatched", parentHotelCompany.isEqualUsingParentCompanyRegex(search, Util.getField(str, "HotelName")));
        else
          location.put("isMatched", Boolean.TRUE);

        return location;

    }

}
