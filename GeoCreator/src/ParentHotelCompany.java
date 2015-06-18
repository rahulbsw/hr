import com.ipass.firefly.hotel.regex.GrokMatcher;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by rjain on 6/11/2015.
 */
public enum ParentHotelCompany {
       /*   Add All regex to GrokMatcher
          Hotel Name Regex :-
          ===================
           1.Accor "(\bAdagio\b)|(\ball seasons)|(\bCaesar Business)|(\bCaesar Park\b)|(\bCitigate\b)|(\bCoralia club\b)|(\bEtap\b)|(Fairmont Resort)|(Formule 1\b)|(\bGrand Mercure)|(\bMercure\b)|(\bHotel F1\b)|(\bIbis\b)|(\bMGallery)|(\bNovotel\b)|(Parthenon Residence)|(\bPullman\b)|(\bQuay\b)|(\bSofitel\b)|(\bThe Sebel\b)"

           2.Best Western "(Best Western)|(\bBW Premier)|(City Sheridan)|(^BW\b)|(\bBW.+Inn)"

           3.Carlson "(\bArt['` ]*otel\b)|(Country Inn (& )*Suites)|(^Park Inn)|(^Park Plaza)|(\bRadisson\b)"

           4.Choice "(\bAscend\b)|(\bCambria\b)|(\bComfort Inn)|(\bComfort Suites)|(\bSleep Inn)|(Quality Hotel)|(Quality Inn)|(\bMainstay\b)|(Suburban Extended)|(Econo Lodge)|(Rodeway Inn)|(\bClarion\b)|(Comfort Hotel)|(Comfort Resort)|(Quality Resort)|(Quality Suites)"

           5.Hilton  "(\bHilton\b)|(\bConrad\b)|(\bDoubletree\b)|(Double tree\b)|(\bEmbassy Suites)|(\bHampton by\b)|(\bHampton Inn\b)|(Homewood Suites)|(\bEmily Morgan)|(Waldorf Astoria)"

           6.Hyatt "(\bHyatt\b)|(\bAndaz\b)"

           7.IHG  "(Crowne Plaza)|(Hotel Indigo)|(Holiday Inn(?! Motel))|(Staybridge)|(Candlewood)|(^Even )|( Even )|(CrownePlaza)|(Hualuxe)|(Intercontinental)|(Sunspree)"  "ANA Hotel"

           8.Marriott  "(Marriot)|(TownePlace)|(Towne Place)|(Fairfield.?Inn)|(Ritz.+carlton)|(Autograph)|(Moxy)|(Protea)|(Springhill)|(Gaylord)|(Delta Hotel)|(Edition )|(Bulgari )|(^Renaissance)|(Samara Renaissance)|(^Courtyard(?! by))|(Residence inn)" "^AC "

           9.Starwood "(Sheraton)|(four points)|(aloft)|(le meridien)|(luxury collection)|(element (?!on ))|(Westin )|( Westin$)|(^(The )*St Regis )|(^St. Regis)|(^itc\b)|(^w hotel)|( w hotel)"

           10.Wyndham  "(\bWyndham\b)|(\bBaymont\b)|(\bDays Hotel\b)|(\bDays Inn\b)|(Howard Johnson)|(\bKnights Inn\b)|(Microtel\b)|(\bNight Hotel\b)|(\bRamada\b)|(\bSuper\s*8\b)|(Travelodge\b)|(\bTryp\b)|(\bWingate\b)|(\bHawthorne Suites)"

           11.Scandic  "(\bScandic\b)"

    */

      /*
    * Prepare STR sub set collection for Parent Company by Regex
    * Parent hotel Group :-
         ====================
           Parent Hotel Company         Parent Hotel Company Full Name                   Mongo Regex($option :i)
           ---------------------------------------------------------------------------------------------------------------
           Marriott                     Marriott International                            marriott
           Starwood                     Starwood Hotels & Resorts                         starwood
           Choice                       Choice Hotels International                       choice
           Hyatt                        Hyatt                                             hyatt
           Wyndham                      Wyndham Extra Holidays                            wyndham
           Wyndham                      Wyndham Hotels & Resorts                          wyndham
           Wyndham                      Wyndham Vacation Resort                           wyndham
           Wyndham                      Wyndham Worldwide                                 wyndham
           Hilton                       Hilton Hotels                                     hilton
           Hilton                       Hilton Worldwide                                  hilton
           Accor                        Accor Company                                     accor
           Best Western                 Best Western Company                              best Western
           Carlson                      Carlson Hospitality Company                       carlson
           IHG                          Intercontinental Hotels Group                     ^Intercontinental
           SCANDIC                      Scandic                                           Scandic

    */

    HYATT("Hyatt","hyatt",
          "(\\bHyatt\\b)|(\\bAndaz\\b)",
          "(\\bhyatt\\b)|(\\bandaz\\b)"),
    //MARRIOTT("Marriott", "marriott", "(Marriot)|(TownePlace)|(Towne Place)|(Fairfield.?Inn)|(Ritz.+carlton)|(Autograph)|(Moxy)|(Protea)|(Springhill)|(Gaylord)|(Delta Hotel)|(Edition )|(Bulgari )|(^Renaissance)|(Samara Renaissance)|(^Courtyard(?! by))|(Residence inn)|(^AC )"),
    MARRIOTT("Marriott", "marriott",
            "(Marriot)|(TownePlace)|(Towne Place)|(Fairfield.?Inn)|(Ritz.+carlton)|(Autograph)|(Moxy)|(Protea)|(Springhill)|(Gaylord)|(Delta Hotel)|(Edition )|(Bulgari )|(^Renaissance)|(Samara Renaissance)|(^Courtyard(?! by))|(Residence inn)|(^AC )",
            "(Marriot)|(TownePlace)|(Towne Place)|(Fairfield)|(Ritz.+carlton)|(Autograph)|(Moxy)|(Protea)|(Springhill)|(Gaylord)|(Delta Hotel)|(Edition )|(Bulgari )|(^Renaissance)|(Samara Renaissance)|(Courtyard)|(Residence)|(^AC )"),
    HILTON("Hilton", "hilton", "(\\bHilton\\b)|(\\bConrad\\b)|(\\bDoubletree\\b)|(Double tree\\b)|(\\bEmbassy Suites)|(\\bHampton by\\b)|(\\bHampton Inn\\b)|(Homewood Suites)|(\\bEmily Morgan)|(Waldorf Astoria)",null),
    CHOICE("Choice", "", "(\\bAscend\\b)|(\\bCambria\\b)|(\\bComfort Inn)|(\\bComfort Suites)|(\\bSleep Inn)|(Quality Hotel)|(Quality Inn)|(\\bMainstay\\b)|(Suburban Extended)|(Econo Lodge)|(Rodeway Inn)|(\\bClarion\\b)|(Comfort Hotel)|(Comfort Resort)|(Quality Resort)|(Quality Suites)",null),
    STARWOOD("Starwood", "starwood", "(Sheraton)|(four points)|(aloft)|(le meridien)|(luxury collection)|(element (?!on ))|(Westin )|( Westin$)|(^(The )*St Regis )|(^St. Regis)|(^itc\\b)|(^w hotel)|( w hotel)",null),
    CARLSON("Carlson", "carlson", "(\\bArt['` ]*otel\\b)|(Country Inn (& )*Suites)|(^Park Inn)|(^Park Plaza)|(\\bRadisson\\b)",null),
    ACCOR("Accor", "accor", "(\\bAdagio\\b)|(\\ball seasons)|(\\bCaesar Business)|(\\bCaesar Park\\b)|(\\bCitigate\\b)|(\\bCoralia club\\b)|(\\bEtap\\b)|(Fairmont Resort)|(Formule 1\\b)|(\\bGrand Mercure)|(\\bMercure\\b)|(\\bHotel F1\\b)|(\\bIbis\\b)|(\\bMGallery)|(\\bNovotel\\b)|(Parthenon Residence)|(\\bPullman\\b)|(\\bQuay\\b)|(\\bSofitel\\b)|(\\bThe Sebel\\b)",null),
    BESTWESTER("Best Western", "best western", "(Best Western)|(\\bBW Premier)|(City Sheridan)|(^BW\\b)|(\\bBW.+Inn)",null),
    IHG("IHG", "^intercontinental", "(Crowne Plaza)|(Hotel Indigo)|(Holiday Inn(?! Motel))|(Staybridge)|(Candlewood)|(^Even )|( Even )|(CrownePlaza)|(Hualuxe)|(Intercontinental)|(Sunspree)|(ANA Hotel)",null),
    WYNDHAM("Wyndham", "wyndham", "(\\bWyndham\\b)|(\\bBaymont\\b)|(\\bDays Hotel\\b)|(\\bDays Inn\\b)|(Howard Johnson)|(\\bKnights Inn\\b)|(Microtel\\b)|(\\bNight Hotel\\b)|(\\bRamada\\b)|(\\bSuper\\s*8\\b)|(Travelodge\\b)|(\\bTryp\\b)|(\\bWingate\\b)|(\\bHawthorne Suites)",null),
    SCANDIC("Scandic","Scandic","(\\bScandic\\b)",null),
    NULL("null", "null", "null",null);

    private String name;
    private String mongoRegex;
    private String regex;
    private String ssidRegex;

    private GrokMatcher grokMatcher;
    private String code;

    public void initializeHotelDirectory() {
        Map<String, String> dict = new HashMap<String, String>();
        //dict.put(dictSourceType,dictionaries);
        grokMatcher = new GrokMatcher(dict);
        Map<String, GrokMatcher.Expression> hotelRegexDict = new HashMap<String, GrokMatcher.Expression>();
        // Hotel Name regex
        hotelRegexDict.put(this.code, grokMatcher.createExpression(this.regex, Pattern.CASE_INSENSITIVE));

        int i = 1;
        for (String s : regex.split("\\|")) {

            hotelRegexDict.put(this.code + "-" + i, grokMatcher.createExpression(s, Pattern.CASE_INSENSITIVE));
            i++;
        }
        grokMatcher.setNumRequiredMatches(GrokMatcher.NumRequiredMatches.AT_LEAST_ONCE);
        grokMatcher.prepareExpressionMap(hotelRegexDict);
        grokMatcher.setFindSubstrings(true);
    }

    public static String generateCode(String name) {
        if (name == null)
            return "";
        return name.replace(" ", "").toUpperCase();
    }

    ParentHotelCompany(String name, String mongoRegex, String regex,String ssidRegex) {
        this.name = name;
        this.regex = regex;
        this.mongoRegex = mongoRegex;
        this.ssidRegex=(ssidRegex!=null)?ssidRegex:regex;
        this.code = generateCode(name);
        initializeHotelDirectory();
    }

    public DBObject getQueryObject() {
        // Parent Hotel Company
        // { "ParentCompany": { $regex: "starwood" ,$options: 'i'} }
        DBObject object = new BasicDBObject("ParentCompany", new BasicDBObjectBuilder().start().add("$regex", this.mongoRegex).add("$options", "i").get());
        System.out.println(object.toString());
        return object;
    }

    public String getCode() {
        return code;
    }

    @Override
    public String toString() {
        return code;
    }

    public static ParentHotelCompany get(String name) {
        String code = generateCode(name);
        for (ParentHotelCompany p : ParentHotelCompany.values())
            if (p.code.equalsIgnoreCase(code))
                return p;
        return ParentHotelCompany.NULL;
    }


    /*
     Does Input String Matches with Parent Company Regex
     @param str
   */
    public boolean isMatchesWithParentCompanyRegex(String str) {
        return grokMatcher.doMatch(this.code, str, false);
    }

    public boolean isEqualUsingParentCompanyRegex(String str1, String str2) {
        int i = 1;
        for (String s : ssidRegex.split("\\|")) {

            if (grokMatcher.doMatch(this.code + "-" + i, str1, false))
                if (grokMatcher.doMatch(this.code + "-" + i, str2, false))
                    return true;
            i++;
        }
        return false;
    }


}
