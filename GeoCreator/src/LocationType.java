/**
 * Created by rjain on 5/11/2015.
 */
public enum LocationType {
    NAS_IP("NasIp","NAS_IP_ADDRESS"),
    ISP("ISP","PROVIDER_ID"),
    CALLED_STATION_ID("CalledStationId","CALLED_STATION_ID"),
    HUNT_GROUP("HuntGroup","HUNT_GROUP"),
    PREFIX("Prefix","PREFIX"),
    OTHER(null,"OTHER");;

    String type;
    String name;

    LocationType(String type, String name) {
        this.type = type;
        this.name = name;
    }

    public static LocationType getLocationType(String type)
    {
        if (NAS_IP.type.equalsIgnoreCase(type))
            return NAS_IP;
        if (CALLED_STATION_ID.type.equalsIgnoreCase(type))
            return CALLED_STATION_ID;
        if (HUNT_GROUP.type.equalsIgnoreCase(type))
            return HUNT_GROUP;
        if (PREFIX.type.equalsIgnoreCase(type))
            return PREFIX;
        if (ISP.type.equalsIgnoreCase(type))
            return ISP;
        else
            return OTHER;
    }

    public boolean equals(LocationType in)
    {
        return (in.name.equals(this.name));
    }

}
