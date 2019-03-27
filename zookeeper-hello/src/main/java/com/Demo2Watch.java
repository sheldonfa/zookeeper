package com;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class Demo2Watch {
    public static void main(String[] args) throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient("hadoop01:2181", retryPolicy);
        client.start();
        // 创建TreeCache
        TreeCache treeCache = new TreeCache(client, "/per1/per2");
        // 设置监听过程
        treeCache.getListenable().addListener((curatorFramework, event) -> {
            ChildData data = event.getData();
            if (data != null) {
                switch (event.getType()) {
                    case NODE_ADDED:
                        System.out.println("NODE_ADDED : " + data.getPath() + "  数据:" + new String(data.getData()));
                        break;
                    case NODE_REMOVED:
                        System.out.println("NODE_REMOVED : " + data.getPath() + "  数据:" + new String(data.getData()));
                        break;
                    case NODE_UPDATED:
                        System.out.println("NODE_UPDATED : " + data.getPath() + "  数据:" + new String(data.getData()));
                        break;

                    default:
                        break;
                }
            } else {
                System.out.println("data is null : " + event.getType());
            }
        });
        //开始监听
        treeCache.start();
        Thread.sleep(Integer.MAX_VALUE);
    }
}
