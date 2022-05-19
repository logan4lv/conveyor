package com.kpgrowing.conveyer.common.queue.zookeeper;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.apache.curator.utils.ZKPaths.extractSequentialSuffix;
import static org.apache.curator.utils.ZKPaths.makePath;

public abstract class AbstractZKQueue implements ZKQueueConstants {

    protected List<String> sortGroupNodes(List<String> groups) {
        List<String> sortedList = Lists.newArrayList(groups);
        Collections.sort(sortedList,
                new Comparator<String>() {
                    @Override
                    public int compare(String lhs, String rhs) {
                        return fixForSorting(lhs, GROUP_SEQUENTIAL_SEPARATOR)
                                .compareTo(fixForSorting(rhs, GROUP_SEQUENTIAL_SEPARATOR));
                    }
                });
        return sortedList;
    }

    private String fixForSorting(String str, String lockName) {
        return standardFixForSorting(str, lockName);
    }

    private String standardFixForSorting(String str, String lockName) {
        int index = str.lastIndexOf(lockName);
        if (index >= 0) {
            index += lockName.length();
            return index <= str.length() ? str.substring(index) : "";
        }
        return str;
    }

    protected String getGroupKeyByNode(String node) {
        return node.replace(GROUP_SEQUENTIAL_SEPARATOR + extractSequentialSuffix(node), "");
    }

}
