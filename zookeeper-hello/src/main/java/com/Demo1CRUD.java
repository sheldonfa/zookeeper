package com;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

public class Demo1CRUD {

    /**
     * 创建永久节点
     */
    @Test
    public void createNode() throws Exception {
        // 1 创建客户端
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient("hadoop01:2181", retryPolicy);
        // 2 开启客户端连接
        client.start();
        // 3 通过create创建节点，并且指定节点类型
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/per1/per2");
        // 4 关闭连接
        client.close();
    }

    /**
     * 创建临时节点
     * @throws Exception
     */
    @Test
    public void createNode2() throws Exception {
        // 1 创建客户端
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient("hadoop01:2181", retryPolicy);
        // 2 开启客户端连接
        client.start();
        // 3 创建临时节点
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/e1/e2");
        // 临时节点断开连接就消失，可以快速用命令行观察结果
        Thread.sleep(5000);
        // 4 关闭连接
        client.close();
    }


    /**
     * 修改节点
     * @throws Exception
     */
    @Test
    public void nodeData() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient("hadoop01:2181", retryPolicy);
        client.start();
        client.setData().forPath("/per1/per2", "hello".getBytes());
        client.close();
    }

    /**
     * 查询节点
     */
    @Test
    public void search() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient("hadoop01:2181", retryPolicy);
        client.start();
        byte[] bytes = client.getData().forPath("/per1/per2");
        System.out.println(new String(bytes));
        client.close();
    }

}
