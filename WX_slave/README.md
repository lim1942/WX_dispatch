一.python 版本2.7

二.依赖(windows)
    gevent (1.2.2)

三.slave程序启动
    python start.py

四.结构：
slave断线后，取的没跑掉的任务不会丢失，master会把这部分僵尸任
务（doing状态超过一天）再推入todo队列。master断线后，salve会把要返回的
task_id累计起来，等连接正常一并返回。如从机性能过剩，可复制slave文件夹
再运行一份，不要在同文件夹重复运行，未找到windows文件锁的方案
文件写入会混乱。用script_test.py测试内部脚本是否可用，返回时间
是否正常，一个任务中会跑若干个脚本，如果单个脚本超时失败就会导致
该任务失败的，需常用script_test.py测试内部脚本是否正常，

    1.任务循环系统:slave任务池(1000)任务数量少于一定数量(500)
    就去master取(500)，任务完成一定数量向master并反馈任务id。
    对应start.py中的 get_tasks()，back_taskids()函数。

    2.文件备份系统：backup.py,start.py在loop_system()导入了它。
    每到了0点，对今天前完成写入的文件移动备份到 D:/data1中，并对文件
    名添加全局id，脚本会自动同步文件到88服务器。

    3.任务执行系统：根据设置的并发量(50),并发执行任务。通过
    Q_FINISH,Q_FAILED,Q_TASK和任务系统进行通信。任务循环系统
    取到任务放入Q_TASK中，执行需要任务就从Q_TASK中取。完成的
    任务，将task_id放入Q_FINISH,失败放入Q_FAILED。任务循环系统
    达到阀值将task_id返回给master。一个任务，会跑多个网站，在
    限定的超时时间(25s)任务还没跑完就记为失败。内部网站跑出了结果
    一部分会调用 tool.py 里的save_file(filename,file,head)存储本地文件，
    另一份会调用tools.py中响应的通道入广州的pg库。内部网站的字段处理，加密
    也会用到tools中的工具。


五.日志
    log为日志文件夹
        1.backup.log 文件备份日志
        2.dispatch.log 调度系统的日志，记录失败的任务id
        3.guangzhou_api.log 入广州pg库的日志
        4.script.log内部脚本相关的日志
        5.tasks.log任务循系统的日志


六.目录结构
    data\为存储本地网站爬取数据的本地文件
    entrance\爬取内部网站的脚本
    log\日志目录
    utils\相关的工具
    backup.py 备份脚本
    cleaner.py 清理pyc的脚本
    config.py配置文件
    tools.py 相关的工具
    script_test.py 用于测试内部爬虫脚本的耗费时间

七.wanning.txt 重要的报错码



#脚本配置
 须在config.py中把内部爬虫脚本按照入库类别分别加入 ENTER_1，ENTER_2，ENTER_3中
 entance\__init__.py中__all__应添加新添加的脚本

#报错码
 100:任务循环系统报错 start.py >loop_system()
 101:任务id返回maser出错,slave与master间的连接问题 start.py >back_taskids()
 102:获取任务失败,slave与master间的连接问题 start.py >get_tasks()
 103:master没有任务获取 start.py >get_tasks()
 104:文件备份失败 start.py >loop_system()
 105:任务失败 start.py >doing()
 201:数据入广州pg库发生错误 tools.py >channel(data_list,headers,retry=5)
