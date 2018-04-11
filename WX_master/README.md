一.python 版本2.7

二 依赖(linux)
    redis (2.10.6)
    psycopg2 (2.7.4)
    Flask (0.12.2)

三.master程序启动
    程序的调度后端为redis,确保redis_server内的redis-server已经用redis.conf启动，
    再用python start.py启动master调度系统。--nohup后台挂起，gunicorn + gevent(四进程+异步)


四.结构：
    基于redis的调度系统,redis_server内为redis程序，redis.conf已经配置好，绑定127.0.0.1,监听7001,
    开启rdb持久化，日志为redis.log。默认是单机的redis，若要开启哨兵高可用模式，请在config.py里配置
    sentinel的地址和myster的名字 。slave断线后，取的没跑掉的任务并不会丢失，master会把这部分任
    务（默认一天）再推入todo队列。由于内存的限制，注意推入redis中任务的量，config.py中做了限制
    任务量(50万个任务)。master断线后，salve会把要返回的task_id累计起来，等连接正常一并返回。
    程序由两部分构成 :
      1.服务master.py
          a.主服务，接受外部请求（对master推任务），接受从机的请求（从机拿任务),群控的转
        发服务，实时爬取广州入库服务，广州字段加密服务。
          b.任务由task_id和任务内容构成，任务的真实存储是redis里的hash。redis对task_id维护1个 todo 
        队列，3个： doing, finish, failed 集合。
          c.推入任务，task_id进入todo队列，任务内容存储为hash,并返回推任务的状态码。默认的任务是往todo
        队列的右边推，若是优先级高的任务往左边推。
          d.取任务，从todo的左边pop，slave从master取定量任务(500),被取任务的task_id从todo队列移动
        到doing集合，任务retry+1，并记录当天的日期。
          e.从机返回task_id，把从机返回的task_id按照是否完成，分别加入到finish，failed两个集合中。

      2.redis监控clear_redis.py
        监控doing，finish，failed三个集合，定时清理。doing集合中的任务状态超时（当天未完成的任务）
        使其返回todo队列，failed集合超过重试次数丢弃它未超过使其返回todo队列，finish完成队列超过阈值就进行清理


五.日志
    log为日志文件夹，
        1.clear_redis.log为清理redis的日志
        2.gunicorn.err.log master.py的标准报错
        3.gunicorn.log master.py 的标准输出
        4.pgsql_db.log  连接pg库的日志
        5.redis_db.log  本地redis相关日志
        6.web_access.log 服务访客日志


六.dbs
    pg_dg连接广州的pg插入数据
    redis_db本地调度redis
    redis_db2群控的redis调度


七.wanning.txt 重要的报错码
    

# redis 默认密码 mujin258   
#报错码
    000:清理redis队列出错 clear_redis.py > clear_redis()
    001:清理redis TASK_FINISH出错 clear_redis.py > trim_finish_set(self)
    002:清理redis TASK_FAILED出错 clear_redis.py > trim_failed_set(self)
    003:清理redis TASK_DOING出错 clear_redis.py > handle_bad_doing(self)
    004:清理redis 检查任务是否可用出错 clear_redis.py > task_is_available(self,task_id)
    005:清理redis 把任务推回todo队列出错 clear_redis.py > back_task_to_todo(self,task_id)
    101:连接到redis出错，确保master本地的redis已经启动 dbs/redis_db.py > connection_test(self)
    102:向master推单个任务出错 dbs/redis_db.py > save_task_to_redis(self,task)
    103:向master推多任务出错 dbs/redis_db.py > save_tasks_to_redis(self,tasks)
    104:向master推优先任务出错 dbs/redis_db.py > save_task_to_redis_promote(self,task)
    105:slave取任务出错 dbs/redis_db.py >get_task(self,count)
    106:slave完成的任务id推入TASK_FINISH出错 dbs/redis_db.py > task_finished(self,task_ids)
    107:slave失败的任务id推入TASK_FIALED出错 dbs/redis_db.py > task_failed(self,task_ids)
    200:与pg库建立连接失败,确定pg的配置正确 dbs/pg_db.py > get_conn(self) get_cursor(self)
    201:插入pg表1失败 dbs/pg_db.py > channel_1(self,data_list)
    202:插入pg表2失败 dbs/pg_db.py > channel_2(self,data_list)
    203:插入pg表3失败 dbs/pg_db.py > channel_3(self,data_list)
