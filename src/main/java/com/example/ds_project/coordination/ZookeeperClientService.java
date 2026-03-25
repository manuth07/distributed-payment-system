package com.example.ds_project.coordination;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class ZookeeperClientService {

    @Value("${zookeeper.url}")
    private String zookeeperUrl;

    private CuratorFramework client;

    @PostConstruct
    public void init() {
        client = CuratorFrameworkFactory.builder()
                .connectString(zookeeperUrl)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .build();
        client.start();
    }

    public CuratorFramework getClient() {
        return client;
    }

    @PreDestroy
    public void close() {
        if (client != null) {
            client.close();
        }
    }
}
