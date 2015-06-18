import com.ipass.firefly.hotel.util.Util;
import com.mongodb.*;
import com.mongodb.MongoClient;
import org.bson.types.*;

import java.text.DateFormat;
import java.util.*;

public class GeoCreator {

	static String database = "locationhub";
//	static String collection = "str";
//	static String latAttrName = "Latitude";
//	static String lngAttrName = "Longitude";
	static String collection = "hotel_location_data";
	static String latAttrName = "LAT";
	static String lngAttrName = "LNG";

	private static String getField(DBObject item, String fieldName)
	{
		return getField( item,  fieldName,false);
	}
	private static String getField(DBObject item, String fieldName,boolean isSub)
	{
		if (isSub) {
			DBObject map = item;
			String[] keys=fieldName.split("\\.");
			for (int i=0 ;i<keys.length-1;i++) {
			  if (map instanceof BasicBSONList)
				  map=(DBObject)((BasicBSONList)map).get(0);
				map =(DBObject)map.get(keys[i]);
			}
			fieldName=keys[keys.length-1];
			item=map;
		}
		Object o = item.get(fieldName);
		   if( o instanceof String){
			   return (String)o;
		   }
		   else if(o instanceof Double)
		   {
			   return ((Double)o).toString();
		   }
		   else if(o instanceof Integer)
		   {
			   return ((Integer)o).toString();
		   }
		   else if(o instanceof Long)
		   {
			   return ((Long)o).toString();
		   }
		   else
		   {
			   if( o != null)
				   System.out.println("Unrecognized Item Class:"+o.getClass().getCanonicalName());
			   return "";
		   }
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try{
		  MongoClient client = new MongoClient(new ServerAddress("localhost", 27017));
		  DB popdb = client.getDB(database);
		  DBCollection coll = popdb.getCollection(collection);
		  
		  // Select everything
		  // find sqm recs with GPS
		  BasicDBObject searchQuery = new BasicDBObject();
		  searchQuery.put(latAttrName, new BasicDBObject("$exists","true"));
		  searchQuery.put(lngAttrName, new BasicDBObject("$exists","true"));
		  DBCursor cursor = coll.find(searchQuery).snapshot();
		  System.out.println("****** Job Started at "+ DateFormat.getDateTimeInstance(DateFormat.FULL,DateFormat.FULL).format(new Date())+"******");
		  
		  // Process every item and create a "where" geospatial element [longitude,latitude]
		    DBObject item = null;
		    int updates = 0;
			System.out.println("****** Processing "+cursor.count()+" items******");
		  	while(cursor.hasNext() ) 
		    {
		  		item = cursor.next();
				Double lat =null;
				Double lng =null;

				String lat1=Util.getField(item, latAttrName);
				String lng1= Util.getField(item, lngAttrName);
				if (lat1!=null && lat1.length()>0)
					lat = Double.valueOf(lat1);
				if (lng1!=null && lng1.length()>0)
					lng = Double.valueOf(lng1);
				if (lng == null  || lat==null )
					continue;
		  		// Construct update to add the "where" element
		  		// get document id for update
			   BasicDBObject target = new BasicDBObject("_id",item.get("_id"));
			   BasicDBObject update;
		       // construct the "where" array - [longitude, latitude]
		        DBObject coordinates = new BasicDBList();
				coordinates.put( "0", lng );
				coordinates.put( "1", lat );
				DBObject where= BasicDBObjectBuilder.start().append("type", "Point").append("coordinates",coordinates).get();

		       // update item with the new where
		       update = new BasicDBObject("$set",new BasicDBObject("where", coordinates));
			   coll.update(target, update);
			   updates++;
		    }
			cursor.close();
			BasicDBObject geoindex = new BasicDBObject("where", "2dsphere");
			coll.createIndex(geoindex);
			System.out.println("****** Job Completed with "+updates+" updates at "+ DateFormat.getDateTimeInstance(DateFormat.FULL,DateFormat.FULL).format(new Date())+"******");
		
	  }
		  	catch(Exception e)
	  {
		  e.printStackTrace();
		  System.out.println(e.toString());
		  System.out.println("****** Job Failed at "+ DateFormat.getDateTimeInstance(DateFormat.FULL,DateFormat.FULL).format(new Date())+"******");
		  System.exit(-1);
	  }

	}

}
