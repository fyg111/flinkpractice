package com.flinkpractice.flinkdemo.source.customsource.parallel;

import com.flinkpractice.flinkdemo.pojo.EventLog;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class RIchSourceFunctionParallelDemo extends RichParallelSourceFunction<EventLog> {
    private volatile boolean isGoOn = true;
    private String[] eventIds = {"applaunch", "pageload" ,"adshow" , "adclick" , "itemshare" , "itemcollect" ,"putback" , "wakeup", "appclose"};
    @Override
    public void run(SourceContext<EventLog> ctx) throws Exception {
        EventLog eventLog = new EventLog();
        HashMap<String, String> map = new HashMap<>();
        while (isGoOn) {
            eventLog.setGuid(RandomUtils.nextLong(1,1000L));
            eventLog.setSessionId(RandomStringUtils.randomAlphabetic(12).toUpperCase());
            eventLog.setEventId(eventIds[RandomUtils.nextInt(0 , eventIds.length )]);
            eventLog.setTimestamp(System.currentTimeMillis());

            map.put(RandomStringUtils.randomAlphabetic(1), RandomStringUtils.randomAlphabetic(2));
            eventLog.setEventInfo(map);

            ctx.collect(eventLog);
            map.clear();
            TimeUnit.MILLISECONDS.sleep(RandomUtils.nextInt(500 , 1500));
        }
    }

    @Override
    public void cancel() {
        isGoOn = false;
    }
}
