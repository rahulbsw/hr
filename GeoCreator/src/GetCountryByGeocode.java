import com.ipass.firefly.geotool.GeoQuery;
import com.vividsolutions.jts.io.ParseException;

import org.geotools.filter.text.cql2.CQLException;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rjain on 5/5/2015.
 */
public class GetCountryByGeocode {
    GeoQuery geoQuery;
    Map<String,String> retrunAtributes=new HashMap<String, String>();
   public GetCountryByGeocode() throws IOException {
        //key =>mapping column value=> lookup column

        String countyName     ="NAME";
        String countryISOCode =  "ISO2";
       retrunAtributes.put("country", countyName);
       retrunAtributes.put("CountryCode", countryISOCode);
       URL url=GetCountryByGeocode.class.getClassLoader().getResource("countries.shp");
        geoQuery= GeoQuery.getInstance("CountryByGeocode", url);
        //System.out.println(geoQuery.getSchema());
    }

    protected Map<String, String> findFirstByLatLong(double lat,double lng) throws ParseException, IOException, CQLException {
        return   geoQuery.findFirstByLatLong(lat, lng, retrunAtributes);
    }

    protected Double getDouble(Object obj)
    {
        if  (obj == null)
            return null;
        if (obj instanceof Double)
            return  (Double)obj;
        if (obj instanceof String)
            return  new Double((String)obj);
        if (obj instanceof Float)
            return  new Double((Float) obj);
        if (obj instanceof Long)
            return  new Double((Long) obj);
        else
            return null;
    }

}
