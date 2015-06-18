import java.text.DateFormat;
import java.util.Date;

import com.ipass.firefly.hotel.util.Util;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class HotelCorrelator {

	static double radius = 0.25; // match within radius in kilometers

	static String hoteldatabase = "locationhub";
	static String strcollection = "locationhub";
	static String hotelcollection = "locationhub";
	static String strlatAttrName = "Latitude";
	static String strlngAttrName = "Longitude";
	static String hotellatAttrName = "LATITUDE";
	static String hotellngAttrName = "LONGITUDE";
	static Double hotellat,hotellng,strlat,strlng;
	public static double Nearest=100000;
	static String nearestname;
	static String nearestaddr;
	static String nearestcity;
	static int sample=0;
	static int geomismatches=0;
	





	
	// Match an STR item with the hotel info, return STRID on a good match
	static String matchSTR(DBCursor str_cursor, Object where, String hotelname, String hoteladdr, String hotelcity){
	   DBObject nearest = null;
	   DBObject nearitem = null;
	   String strid = null;
	
	   // Get address line tokens
	   String [] hoteladdrtokens = Util.tokenizeAddress(hoteladdr);
	   String [] hotelnametokens = hotelname.split("\\s+-");
	   if(str_cursor.count() == 0){
//		   System.out.println("["+sample+"]no geo match:"+hotelname+" : "+hoteladdr);
		   geomismatches++;
		   return(strid);
	   }else{
//		   System.out.println("str candidates:"+str_cursor.count());
	   }
	   while(str_cursor.hasNext())
	   {
		   DBObject stritem = str_cursor.next();
		   // keep track of closest
		   String straddr = Util.getField(stritem, "Address 1");
		   String strname = Util.getField(stritem, "Hotel Name");
		   strlat = new Double(Util.getField(stritem, "Latitude"));
		   strlng = new Double(Util.getField(stritem, "Longitude"));
		   String strcity = Util.getField(stritem, "City");
		   double distance = Util.gps2m(strlat, strlng, hotellat, hotellng);
		   if (distance < Nearest){
			   Nearest=distance;
			   nearestname=strname;
			   nearestaddr=straddr;
			   nearestcity=strcity;		   
		   }
		   // First see if City matches
		   String scrubbedHotelCity = Util.scrubCity(hotelcity);
		   String scrubbedStrCity = Util.scrubCity(strcity);
		   if( scrubbedHotelCity.equalsIgnoreCase(scrubbedStrCity) || Util.spellingError(scrubbedHotelCity, scrubbedStrCity)){
			   // IF so, lets look for additional matches
			   String [] straddrtokens = Util.tokenizeAddress(straddr);
			   String [] strnametokens = strname.split("\\s+-");
			   if( hoteladdr.equalsIgnoreCase(straddr)){
				   System.out.println("["+sample+"]FULL match H:"+hotelname+" @ "+hoteladdr+" S:"+strname+" @ "+straddr);
				   System.out.println("["+sample+"] "+strlat+","+strlng);
				   return(Util.getField(stritem, "STR Number"));
			   }
			   
			   if(Util.substringMatchAddr(hoteladdrtokens, straddrtokens, scrubbedHotelCity))
			   {
				   System.out.println("["+sample+"]SUBS match H:"+hotelname+" @ "+hoteladdr+" S:"+strname+" @ "+straddr);
				   System.out.println("["+sample+"] "+strlat+","+strlng);
				   return(Util.getField(stritem, "STR Number"));
			   }
			   else{
//				   System.out.println("     --->["+sample+"]item mismatch H:"+hoteladdr+" S:"+straddr);
				   
			   }
			   
			   
			   
			   
			   // IF it's a match, return the matching STR item number	   
//			   strid = getField(stritem,"STR Number");
//			   break;
			   
		   }else{
			   System.out.println("["+sample+"]item city mismatch H:"+hotelcity+" S:"+strcity);
			   
		   }
	   }
//	   System.out.println("   --->["+sample+"]No match:"+hoteladdr);
	   return(strid);
	}


	/**
	 * Correlate hotel POPs with STR database
	 */
	
	public static void main(String[] args) {
		try{
		  MongoClient client = new MongoClient(new ServerAddress("localhost", 27017));
		  DB popdb = client.getDB(hoteldatabase);
		  
		  DBCollection hotelcoll = popdb.getCollection(hotelcollection);
		  DBCollection strcoll = popdb.getCollection(strcollection);
		  
		  // Select everything
		  // find hotel recs with GPS
		  BasicDBObject searchQuery = new BasicDBObject();
//		  searchQuery.put("where", new BasicDBObject("$exists","true"));
		  searchQuery.put("REGION", "Europe");
		  DBCursor cursor = hotelcoll.find(searchQuery).snapshot();
		  System.out.println("****** Job Started at "+ DateFormat.getDateTimeInstance(DateFormat.FULL,DateFormat.FULL).format(new Date())+"******");
		  
		  // Process every item and create a "where" geospatial element [longitude,latitude]
		    DBObject item = null;
		    int updates = 0;
		    int candidates=cursor.count();
		    cursor.addOption(Bytes.QUERYOPTION_NOTIMEOUT);
			System.out.println("****** Processing "+candidates+" hotels******");
		  	while(cursor.hasNext() ) 
		    {
		  		sample++;
		  		Nearest=420932093;
		  		nearestname=nearestaddr=nearestcity="";
		  		item = cursor.next();
				BasicDBObject thishotel = new BasicDBObject("_id",item.get("_id"));
		  		// Capture the hotel lat/long
		  		hotellat = Double.valueOf(Util.getField(item, hotellatAttrName));
		  		hotellng = Double.valueOf(Util.getField(item, hotellngAttrName));
		  		String hotelcity = Util.getField(item, "CITY");
		  		String hoteladdr = Util.getField(item, "SITE_ADDRESS");
		  		String hotelname = Util.getField(item, "SITE_NAME");
		  		// Save geopoint for hotel
		  		Object where = item.get("where");
		  		
		  		// Formulate a geoquery to find STR items near this hotel
		       BasicDBList sphere = new BasicDBList();
		       sphere.put( "0", where );
		       sphere.put( "1", (double)(radius/3959.0));
		       BasicDBObject within = new BasicDBObject("$centerSphere",sphere);
		       BasicDBObject geo = new BasicDBObject("$geoWithin",within);
			   BasicDBObject target = new BasicDBObject("where",geo);
			   DBCursor str_cursor = strcoll.find(target);
			   if( str_cursor.count() == 0)
			   {
				   BasicDBObject citysearchQuery = new BasicDBObject();				   
				   citysearchQuery.put("City", hotelcity);
				   str_cursor = strcoll.find(citysearchQuery).snapshot();
			   }
			   String strid = matchSTR(str_cursor, where, hotelname, hoteladdr, hotelcity);
			   if( strid != null){		  		
			  		// Construct update to add the "strid" element
			  		// update the ipass hotel document
//				   BasicDBObject update;
			       // update item with the new where
//			       update = new BasicDBObject("$set",new BasicDBObject("STRID", strid));
//				   hotelColl.update(target, update);
				   updates++;
			   }
			   else{
				   System.out.println("["+sample+"]nearest("+(long)Nearest+") H:"+hotelname+" @ "+hoteladdr+" S:"+nearestname+" @ "+nearestaddr+" in "+nearestcity);				   
			   }
		    }
			System.out.println("****** Job Completed with "+updates+" updates to "+candidates+" with "+geomismatches+" geo-mismatches at "+ DateFormat.getDateTimeInstance(DateFormat.FULL,DateFormat.FULL).format(new Date())+"******");
		
	  }
		  	catch(Exception e)
	  {
		  System.out.println(e.toString());
		  System.out.println("****** Job Failed at "+ DateFormat.getDateTimeInstance(DateFormat.FULL,DateFormat.FULL).format(new Date())+"******");
		  System.exit(-1);
	  }

	}

}
