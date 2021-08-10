## 简介
由于业务需求，在MySQL生产环境进行DDL操作是比较常见的，DDL操作是比较谨慎的，一般需要做记录，目前市面上有一些审计插件，启用审计一般对性能会有损耗，同时安装审计也是一个比较重的操作，对数据库有侵入性。因此开发了一款工具用于记录DDL操作。

当前的mysqlddl支持mysql 5.6/5.7/8.0、mariadb。



## 工作原理

mysqlddl工作原理

+ mysqlddl 模拟 MySQL slave 的交互协议，伪装自己为 MySQL slave，向 MySQL master 发送dump 协议
+ MySQL master 收到 dump 请求，开始推送 binary log 给 slave (即 mysqlddl)
+ mysqlddl 解析 binary log 对象(原始为 byte 流)，解析DDL binlog events



## 配置文件说明

配置config.ini

```
[server]
;如果数据库类型为mysql则输入mysql,如果为mariadb则输入mariadb
flavor=mysql
;从库的server_id,和被监控的库的server_id不能相同
serverID=888
host=192.168.230.221
port=3306
;账号需要有replication slave和replication client权限
username=fxkt
password=123456
```



## 配置开机自启动(systemd方式)

+ 创建启动脚本

  vi run_mysqlddl.sh

  ```shell
  #!/bin/bash
  set -e
  
  DEPLOY_DIR=/opt/soft/mysqlddl-v1.3-Linux
  
  cd "${DEPLOY_DIR}" || exit 1
  exec ./mysqlddl
  ```

+ 创建systemd文件

  vi /etc/systemd/system/mysqlddl.service

  ```shell
  [Unit]
  Description=mysqlddl monitor service
  After=syslog.target network.target remote-fs.target nss-lookup.target
  
  [Service]
  LimitNOFILE=1000000
  LimitSTACK=10485760
  
  User=root
  ExecStart=/opt/soft/mysqlddl-v1.3-Linux/run_mysqlddl.sh
  Restart=always
  
  RestartSec=15s
  
  [Install]
  WantedBy=multi-user.target
  ```

+ 执行如下命令:

  ```shell
  systemctl daemon-reload
  systemctl start mysqlddl
  systemctl status mysqlddl
  systemctl enable mysqlddl
  ```

  