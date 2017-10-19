
library(sparklyr)
#yarn模式集群操作
sc <- spark_connect(master = "yarn-client",spark_home ="/opt/cloudera/parcels/SPARK2-2.2.0.cloudera1/lib/spark2",version = "2.1.0",hadoop_version="2.6")

#读取hive表
web_logs_tbl <- spark_read_table(sc,"web_logs")
#过滤操作
select(web_logs_tbl,c("app","city")) %>% filter(city == "Singapore")

#使用query方式查询
library(DBI)
web_logs_tbl2 <- dbGetQuery(sc, "SELECT city,app,bytes,os_family FROM web_logs where city is not null and length(city)>0  ")
web_logs_tbl2

#分组统计，过滤
web_logs_stat <- web_logs_tbl2 %>% 
  group_by(city) %>%
  summarise(count = n(),cnt_distinct=n_distinct(app), flux = mean(bytes)  ) %>%
  filter(count > 20, flux > 200, !is.na(city),length(city)>4) %>%
  #tally()  %>%
  collect

web_logs_stat


#查看spark运行界面
spark_web(sc)
#查看spark运行日志
spark_log(sc)
#退出回话
spark_disconnect(sc)