DECLARE DS, DE STRING;
SET (DS, DE) = ('<period0>', '<period1>');

SELECT
    Attributed_Touch_Type, Attributed_Touch_Time, Install_Time, Event_Time, 
    Event_Name, Cost_Model, Cost_Value, Cost_Currency, Event_Source, Partner, 
    Media_Source, Channel, Campaign, Campaign_ID, Adset, Adset_ID, Ad, Ad_ID, 
    Ad_Type, Site_ID, Country_Code, DMA, Carrier, AppsFlyer_ID, IMEI, 
    App_ID, Attribution_Lookback, Match_Type

FROM `warm-actor-290215.segmentstream_202203.appsflyerInAppEvents_<platform>_*`

WHERE ARRAY_REVERSE(SPLIT(_TABLE_SUFFIX, '_'))[OFFSET(0)] BETWEEN REPLACE(DS, '-', '') AND REPLACE(DE, '-', '')
AND Event_Name IN ('ftd1', 'conversionStep_[registration]_success', 'std1', 'dep300', 'ftd2')
