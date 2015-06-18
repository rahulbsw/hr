package com.ipass.firefly.hotel.util;

import com.mongodb.DBObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by rjain on 5/21/2015.
 */
public class Util {
    public static final HashMap<String , String> cityNames = new HashMap<String , String>() {{
        put("st",    "Saint");
        put("ste",    "Saint");
        put("st.",    "Saint");
    }};

    public static String fixAddress(String address, String countryCode)
    {
		/* Germany
		 *  1. replace 'aAe' at end of words with 'asse'
			2. replace names ending in 'str.' with 'strasse'
			3. replace 'AP' within a word with 'o'
			4. replace 'A1/4' within a word with 'u'
			5. replace 'ra??[Ee]' and 'ra?[Ee' with 'rasse'
			6. replace 2 numeric strings at end of address separated by space by replacing the intervening space with '-'.
			7. replace '??N' with 'un'
			8. replace 'A$?' with 'a'
			9. remove parenthetic strings[done by split]
			10 truncate addresses after the street number(s)
			11. Replace <number>/<number> with <number>-<number>
			12. Remove city name from address when it appears at end of address string
			13. Remove <number2> from <number1> <number2> if number2 < number1 or if number2-number1 > 100.
			14. Replace <number><space>-<space><number. with <number>-<number>
			15. Replace 'D??S' with 'Dus'
		 */
        return " ";

    }
    // find and replace city name prefixes with fully expanded versions
    public static void expandCity(String [] tokens){
        int tokenCount = tokens.length;
        for(int i=0; i< tokenCount; i++){
            String rep=cityNames.get(tokens[i].toLowerCase());
            if(rep != null){
                tokens[i]=rep;
            }
        }
    }
    public static String getField(DBObject item, String fieldName)
    {
       return getField( item,  fieldName, null);
    }

    public static String getField(DBObject item, String fieldName,String defaultValue)
    {
        Object o = item.get(fieldName);
        if (o == null)
            return defaultValue;
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
        else if(o instanceof List)
        {
            return ((List)o).toString();
        }
        else if(o instanceof Map)
        {
            return ((Map)o).toString();
        }
        else if(o instanceof Boolean)
        {
            return ((Boolean)o).toString();
        }
        else
        {
            if( o != null)
                System.out.println("Unrecognized Item Class:"+o.getClass().getCanonicalName());
            return defaultValue;
        }
    }
    // Calculate distance in meters between two geopoints
    public static double gps2m(double lat_a, double lng_a, double lat_b, double lng_b) {

        float pk = (float) (180/3.14169);

        double a1 = lat_a / pk;
        double a2 = lng_a / pk;
        double b1 = lat_b / pk;
        double b2 = lng_b / pk;

        double t1 = Math.cos(a1)*Math.cos(a2)*Math.cos(b1)*Math.cos(b2);
        double t2 = Math.cos(a1)*Math.sin(a2)*Math.cos(b1)*Math.sin(b2);
        double t3 = Math.sin(a1)*Math.sin(b1);
        double tt = Math.acos(t1 + t2 + t3);

        return 6378137*tt;
    }
    public static String blankIfNull(String s){
        if(s == null)
        {
            return "";

        }
        else{
            return( s.replace("\"", "\"\""));
        }
    }

    public static String restring(String [] tokens){
        StringBuffer rep = new StringBuffer();
        for( int i = 0; i < tokens.length; i++){
            if(tokens[i].length() !=0 ){
                rep.append(tokens[i]);
                rep.append(" ");
            }
        }
        if( rep.length() > 0) rep.deleteCharAt(rep.length()-1);
        return rep.toString();
    }
    public static String scrubCity(String city){
        String [] tokens = city.split("(\\s-\\s*.+)|(\\s)|(\\(.*\\)\\s*-*\\s*.+)|(-\\s)|(-(Manhatt[ea]n|JFK).*)");
        expandCity(tokens);
        return restring(tokens);

    }

    public static String [] tokenizeAddress(String address){
        String [] addrtokens = address.split("(\\s+-\\s*)|(\\s)|(\\(.*\\)\\s*)|(-\\s)");
        expandStreetEndings(addrtokens);
        return addrtokens;
    }

    public static boolean isNumeric(String str)
    {
        return str.matches("\\d+");  //match a number with optional '-' and decimal.
    }

    public static final HashMap<String , String> streetEndings = new HashMap<String , String>() {{
        put("st",    "Street");
        put("st.",    "Street");
        put("str",    "Strasse");
        put("str.",    "Strasse");
        put("dr", "Drive");
        put("dr.", "Drive");
        put("blvd",   "Boulevard");
        put("blv",   "Boulevard");
        put("blvd.","Boulevard");
        put("av",   "Avenue");
        put("av.",   "Avenue");
        put("ave",   "Avenue");
        put("ave.",   "Avenue");
        put("wy",   "Way");
        put("hwy",   "Highway");
        put("hwy.",   "Highway");
        put("hihgway", "Highway");
        put("cir",   "Circle");
        put("ct",   "Court");
        put("cv",   "Cove");
        put("expy",   "Expressway");
        put("expr", "Expressway");
        put("fwy", "Freeway");
        put("fwy.", "Freeway");
        put("ln", "Lane");
        put("ln.", "Lane");
        put("pl", "Place");
        put("pky", "Parkway");
        put("pkwy", "Parkway");
        put("pkwy.", "Parkway");
        put("rd", "Road");
        put("rd.", "Road");
        put("vly", "Valley");
        put("trl", "Trail");
        put("ter", "Terrace");
        put("e", "East");
        put("n","North");
        put("s","South");
        put("w","West");
        put("e.", "East");
        put("n.","North");
        put("s.","South");
        put("w.","West");
        put("nw","Northwest");
        put("sw","Southwest");
        put("ne","Northeast");
        put("se","Southwest");
        put("n.w.","Northwest");
        put("s.w.","Southwest");
        put("n.e.","Northeast");
        put("s.e.","Southwest");
        put("meml","Memorial");
        put("plz","Plaza");
    }};

    // find and replace street ending abbreviations with full spelling
    public static void expandStreetEndings(String [] tokens){
        int tokenCount = tokens.length;
        for(int i=0; i< tokenCount; i++){
            String rep=streetEndings.get(tokens[i].toLowerCase());
            if(rep != null){
                tokens[i]=rep;
            }
        }
    }


    public static void retokenizeAddr(String [] tokens){
        int tokenCount = tokens.length;
        for(int i=0; i< tokenCount; i++){
            if(isNumeric(tokens[i])){
                if( i+1 < tokenCount ){
                    if(isNumeric(tokens[i+1])){
                        tokens[i]=tokens[i]+"-"+tokens[i+1];
                        tokens[i+1]="";
                    }
                }
            }
        }
    }

    public static boolean spellingError(String tokena,String tokenb){
        boolean matches=true;
        boolean miss=false;
        int alength=tokena.length();
        int blength=tokenb.length();
        int diff = java.lang.Math.abs(alength-blength);
        // can't be more than 1 dropped character
        if(diff > 1  ) return false;
        if(diff != 0 && (isNumeric(tokena) && isNumeric(tokenb)))return false;
        for(int i=0,j=0; i < alength && j < blength; i++,j++)
        {
            if( tokena.charAt(i) == tokenb.charAt(j))continue;
            // check for transposition but not numeric transposition
            if(i+1 < alength && !isNumeric(tokena) && !isNumeric(tokenb) ){
                if(tokena.charAt(i+1) == tokenb.charAt(j)){
                    // Might be, check other way
                    if(j+1 < blength){
                        if(tokena.charAt(i) == tokenb.charAt(j+1)){
                            i++;j++;
                            continue;
                        }
                    }
                }
            }
            // if not transposition, check for dropped character but don't allow dropped numerics
            if(miss == false && alength != blength && !isNumeric(tokena) && diff != 0){
                miss=true;
                if(alength < blength){
                    i--;
                }else{
                    j--;
                }
            }else{
                matches=false;
                break;
            }
        }
        return matches;
    }
   public static boolean substringMatchAddr(String [] hoteladdrtokens,String [] straddrtokens,String hotelcity){
        // See if the elements of the STR address (which we assume is well formed) are present in the pop hotel addr
        int matchcount=0;
        retokenizeAddr(hoteladdrtokens);
        for(int i=0; i < straddrtokens.length; i++){
            if( straddrtokens[i].length() != 0){
                boolean matches=false;
                for( int j=0; j< hoteladdrtokens.length; j++){
                    if( hoteladdrtokens[j].length() != 0){
                        if(hoteladdrtokens[j].equalsIgnoreCase(straddrtokens[i])){
                            matches=true;
                            break;
                        }else if(spellingError(hoteladdrtokens[j],straddrtokens[i])){
                            matches=true;
                            System.out.println("******Spelling error*******:"+hoteladdrtokens[j]+" : "+	straddrtokens[i]);
                            break;
                        }
                    }
                }
                if( !matches) break;
                matchcount++;
            }
        }
        return(matchcount == straddrtokens.length);
    }

    public static String join(char separator, String ... values)
    {
        StringBuffer sp=new StringBuffer();

        for (int i = 0; i < values.length; i++)
            if(values[i]!=null && values[i].length()>0 && i+1!= values.length)
                sp.append(values[i]).append(separator);
        return (values[values.length-1]!=null && values[values.length-1].length()>0)?sp.append(values[values.length-1]).toString():sp.append("").toString();
    }

}
