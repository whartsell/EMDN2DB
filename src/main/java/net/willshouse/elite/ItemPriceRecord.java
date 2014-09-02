package net.willshouse.elite;

import org.apache.log4j.Logger;

import java.sql.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by whartsell on 9/1/14.
 */
public class ItemPriceRecord {

    private static final String qStationID = "SELECT ID FROM STATIONS WHERE upper(STATION) = upper(?) AND upper(SYSTEM) = upper(?)";
    private static final String qItemID = "SELECT ID FROM ITEMS WHERE upper(PSUDONYM) = upper(?)";
    private static final String qUpdateItem = "UPDATE PRICES SET BUY_CR = ?, SELL_CR = ?,LASTUPDATE= ?  WHERE STATION_ID = ? AND ITEM_ID=?";

    private int buyPrice,sellPrice,demand,demandLevel,stationStock,stationStockLevel,itemID,stationID;
    private String categoryName;
    private String itemName;
    private String stationName;
    private String systemName;
    private String[] message;
    private Timestamp time;
    private Logger log = Logger.getLogger(ItemPriceRecord.class);

    public ItemPriceRecord(String data) {
        message = data.split(",",-1);
        setupItemPrice();

    }

    public int getBuyPrice() {
        return buyPrice;
    }

    public int getSellPrice() {
        return sellPrice;
    }

    public int getDemand() {
        return demand;
    }

    public int getDemandLevel() {
        return demandLevel;
    }

    public int getStationStock() {
        return stationStock;
    }

    public int getStationStockLevel() {
        return stationStockLevel;
    }

    public String getCategoryName() {
        return categoryName;
    }

    public String getItemName() {
        return itemName;
    }

    public String getStationName() {
        return stationName;
    }

    public String getSystemName() {
        return systemName;
    }

    public String[] getMessage() {
        return message;
    }

    public Timestamp getTime() {
        return time;
    }






    private void setupItemPrice() {

        buyPrice = Integer.parseInt(message[0]);
        sellPrice = Integer.parseInt(message[1]);
        demand = Integer.parseInt(message[2]);
        demandLevel = Integer.parseInt(message[3]);
        stationStock = Integer.parseInt(message[4]);
        stationStockLevel = Integer.parseInt(message[5]);
        categoryName = message[6];
        itemName = message[7];
        Pattern p = Pattern.compile("\\((.*?)\\)",Pattern.DOTALL);
        Matcher matcher = p.matcher(message[8]);
        if (matcher.find()) {
            stationName = matcher.group(1);
        }
        else stationName = "NOT FOUND";
        int index = message[8].indexOf("(");
        systemName = message[8].substring(0,index).trim();
        time = Timestamp.valueOf(message[9].replaceAll("T"," "));
        stationID = 0;
        itemID = 0;

    }

    public void updateDB(Connection conn) throws SQLException {

        PreparedStatement psStationID = conn.prepareStatement(qStationID);
        psStationID.setString(1,stationName);
        psStationID.setString(2,systemName);
        ResultSet rs = psStationID.executeQuery();
        if (rs.first()) {
            stationID = rs.getInt("ID");
            log.debug("StationID:" + stationID);
            PreparedStatement psItemID = conn.prepareStatement(qItemID);
            psItemID.setString(1,itemName);
            rs = psStationID.executeQuery();
            if(rs.first()) {
                itemID = rs.getInt(rs.findColumn("ID"));
                log.debug("ItemID:" + itemID);
                PreparedStatement psUpdateItem = conn.prepareCall(qUpdateItem);
                psUpdateItem.setInt(1,buyPrice);
                psUpdateItem.setInt(2,sellPrice);
                psUpdateItem.setTimestamp(3, time);
                psUpdateItem.setInt(4, stationID);
                psUpdateItem.setInt(5,itemID);
                psUpdateItem.executeUpdate();

                log.info(psUpdateItem.toString());
                log.info("UPDATED");
            }
            else {
                log.error("Item not found:" + psItemID.toString());
            }
        }
        else {
            log.error("Station not found:" + psStationID.toString());
        }


        }
}
