# logstash/Dockerfile
FROM logstash:7.12.1

# 安装 WebHDFS 输出插件
RUN logstash-plugin install logstash-output-webhdfs

# pipeline 配置通过 docker-compose 的 volume 挂进去
