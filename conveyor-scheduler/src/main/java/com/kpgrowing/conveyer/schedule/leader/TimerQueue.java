package com.kpgrowing.conveyor.schedule.leader;

import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.common.Time;
import org.springframework.scheduling.support.CronSequenceGenerator;

/**
 * ExpiryQueue tracks elements in time sorted fixed duration buckets.
 * It's used by SessionTrackerImpl to expire sessions and NIOServerCnxnFactory
 * to expire connections.
 * 分桶策略：不同特征隔离处理，相同特征统一处理。定时器桶的特征是cron一样，假设最小时间单位为分钟，则每分钟检测一次，当前时间命中
 * 时间Long的处理poll出来，再设置进去。
 * <p>
 * 设计步骤：
 * 1.需要哪些操作？
 * - add or update E
 * - remove E
 * - poll, return Set<E>. remove the bucket and update every E in bucket
 * 2.设计数据模型来支持。
 * 索引： E - cron
 * 桶队列： L - E
 * 如何通过索引查找bucket set, 根据cron生成Long
 * 3.编写算法
 * <p>
 * 定时器队列，时间最小单位为分钟，涉及的参数：cron，生成对应时间的Long值
 * 维护一组定时元素
 * 业务场景：
 * 定时获取一桶元素，然后再更新它们
 * update(E e, String cron) 添加元素、更新元素，外部需要维护这组元素集合
 * remove(E e) 删除元素
 */
@Slf4j
public class TimerQueue<E> {

    private final ConcurrentHashMap<E, String> elemMap = new ConcurrentHashMap<E, String>();
    private final ConcurrentHashMap<Long, Set<E>> timedMap = new ConcurrentHashMap<Long, Set<E>>();

    private final AtomicLong scanTime = new AtomicLong();
    private final int timeInterval;

    public TimerQueue(int timeInterval) {
        this.timeInterval = timeInterval;
        scanTime.set(roundToNextInterval(new Date().getTime()));
    }

    private long roundToNextInterval(long time) {
        return (time / timeInterval + 1) * timeInterval;
    }

    private long cronToElapsedTime(String cron) {
        if (!CronSequenceGenerator.isValidExpression(cron)) {
            throw new IllegalStateException("Cron expression is invalid.");
        }
        CronSequenceGenerator cronSequenceGenerator = new CronSequenceGenerator(cron);
        Date next = cronSequenceGenerator.next(new Date());
        return next.getTime();
    }

    public String remove(E elem) {
        String cron = elemMap.remove(elem);
        if (cron != null) {
            Set<E> set = timedMap.get(cronToElapsedTime(cron));
            if (set != null) {
                set.remove(elem);
            }
        }
        return cron;
    }

    public Long update(E elem, String newCron) {
        String prevCron = elemMap.putIfAbsent(elem, newCron);

        if (newCron.equals(prevCron)) {
            return null;
        }

        Long prevTimedTime = null;
        if (StringUtils.isNotBlank(prevCron)) {
            prevTimedTime = cronToElapsedTime(prevCron);
        }
        Long newTimedTime = cronToElapsedTime(newCron);

        if (newTimedTime.equals(prevTimedTime)) {
            return null;
        }

        Set<E> set = timedMap.get(newTimedTime);
        if (set == null) {
            set = Collections.newSetFromMap(new ConcurrentHashMap<E, Boolean>());
            Set<E> existingSet = timedMap.putIfAbsent(newTimedTime, set);
            if (existingSet != null) {
                set = existingSet;
            }
        }
        set.add(elem);

        if (prevTimedTime != null && !newTimedTime.equals(prevTimedTime)) {
            Set<E> prevSet = timedMap.get(prevTimedTime);
            if (prevSet != null) {
                prevSet.remove(elem);
            }
        }

        return newTimedTime;
    }

    public long getWaitTime() {
        long now = new Date().getTime();
        long nextTriggerTime = this.scanTime.get();
        return now < nextTriggerTime ? (nextTriggerTime - now) : 0L;
    }

    public Set<E> poll() {
        long now = new Date().getTime();
        long scanTime = this.scanTime.get();
        if (now < scanTime) {
            return Collections.emptySet();
        }

        Set<E> set = null;
        long nextScanTime = scanTime + timeInterval;
        if (this.scanTime.compareAndSet(scanTime, nextScanTime)) {
            set = timedMap.remove(scanTime);
        }
        if (set == null) {
            return Collections.emptySet();
        }

        // set is not null and had removed, then update every element in this bucket.
        for (E e : set) {
            String cron = elemMap.get(e);
            if (cron != null) {
                long newTimedTime = cronToElapsedTime(cron);

                Set<E> newTimedSet = timedMap.get(newTimedTime);
                if (newTimedSet == null) {
                    newTimedSet = Collections.newSetFromMap(new ConcurrentHashMap<E, Boolean>());
                    Set<E> existingSet = timedMap.putIfAbsent(newTimedTime, newTimedSet);
                    if (existingSet != null) {
                        newTimedSet = existingSet;
                    }
                }
                newTimedSet.add(e);
            }
        }

        return set;
    }

    public void dump(PrintWriter pwriter) {
        pwriter.print("Sets (");
        pwriter.print(timedMap.size());
        pwriter.print(")/(");
        pwriter.print(elemMap.size());
        pwriter.println("):");
        ArrayList<Long> keys = new ArrayList<Long>(timedMap.keySet());
        Collections.sort(keys);
        for (long time : keys) {
            Set<E> set = timedMap.get(time);
            if (set != null) {
                pwriter.print(set.size());
                pwriter.print(" expire at ");
                pwriter.print(Time.elapsedTimeToDate(time));
                pwriter.println(":");
                for (E elem : set) {
                    pwriter.print("\t");
                    pwriter.println(elem.toString());
                }
            }
        }
    }

    public Map<Long, Set<E>> getTimedMap() {
        return Collections.unmodifiableMap(timedMap);
    }

}

