package com.yizhao.apps;

/**
 * @author YI ZHAO
 */
public class FastrackFileDao {
    private String event_id;
    private String kvPair;
    private String cookie_id;
    private String dp_id;
    private String location_id;
    private String modification_ts;

    public FastrackFileDao(String event_id, String kvPair, String cookie_id, String dp_id, String location_id, String modification_ts){
        this.event_id = event_id;
        this.kvPair = kvPair;
        this.cookie_id = cookie_id;
        this.dp_id = dp_id;
        this.location_id = location_id;
        this.modification_ts = modification_ts;
    }

    public String getEvent_id() {
        return event_id;
    }

    public void setEvent_id(String event_id) {
        this.event_id = event_id;
    }

    public String getKvPair() {
        return kvPair;
    }

    public void setKvPair(String kvPair) {
        this.kvPair = kvPair;
    }

    public String getCookie_id() {
        return cookie_id;
    }

    public void setCookie_id(String cookie_id) {
        this.cookie_id = cookie_id;
    }

    public String getDp_id() {
        return dp_id;
    }

    public void setDp_id(String dp_id) {
        this.dp_id = dp_id;
    }

    public String getLocation_id() {
        return location_id;
    }

    public void setLocation_id(String location_id) {
        this.location_id = location_id;
    }

    public String getModification_ts() {
        return modification_ts;
    }

    public void setModification_ts(String modification_ts) {
        this.modification_ts = modification_ts;
    }


    @Override
    public String toString() {
        return "FastrackFileDao [event_id=" + event_id + ", kvPair=" + kvPair + ", cookie_id=" + cookie_id
                + ", dp_id=" + dp_id + ", location_id=" + location_id + ", modification_ts=" + modification_ts + "]";
    }
}
