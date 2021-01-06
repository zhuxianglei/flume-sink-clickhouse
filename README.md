# flume-sink-clickhouse
最近研究flume,发现网络上的资料多少都有些问题，针对clickhouse引用最多的flume sink是：https://reviews.apache.org/r/50692/diff/1#2 ,这个源码的pom.xml存在问题，无法编译；<br>
其次clickhouse官网的jdbc驱动效率不高，鉴于此，本文介绍利用clickhouse官网推荐的，更高效的第三方jdbc写的flume sink，环境基于RHEL6，相关过程见下文。<br>
https://blog.csdn.net/dustzhu/article/details/106887458
