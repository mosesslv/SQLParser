SQL 预测及建议服务

[LICENSE]

LICENSE=Z90LZEXN99U3WRK8IMSD0F1W5

## 异步任务
- 启动命令 `celery -A sqlreview worker -l info -Q default,review`

## API documentation
pallas-app.lujs.cn/pallas/docs

--loglevel=info -Ofair celery默认情况下执行任务会有个预取机制（prefetching）
celery  -A sqlreview  inspect active
查看每个节点注册的任务
celery  -A sqlreview  inspect registered
列出保留任务
celery -A sqlreview inspect reserved

- async task  `app/tasks.py`

整个任务
python manage.py run_git_review_by_task -t {task_id}
单条
python manage.py run_git_review_single -s {seq_id}

前端
python2 -m SimpleHTTPServer 8012

系统包

mysql-lib.x86-64
mysql-devel.x86-64


gunicorn sqlreview.wsgi:application -t 1800 -w 2 --bind 0.0.0.0:8015 --pid ./gunicorn.pid


根据AiSrProfileManage刷新AiSrTenantSchema AiSrSchemaTable数据的命令
python manage.py refresh_table_schema


安装部署:
- 系统需要安装mysql-lib,mysql-devel或mariadb-lib,mysql-devel
- 需要python3.6环境
- 制作license需要先拿到安装机子的机器码
- 安装requirements.txt
- 用supervisor启动


tar -zcvf ./sqlreview.compile.tar.gz  --exclude=.git --exclude=supervisord.conf ./sqlreview.compile/



license key

文档 https://github.com/usrbinsam/mini-key-server

server
172.23.25.202:5000


nohup /wls/virtualenv/license/bin/python keyserver.py &
页面
生成key token
admin
admin

http://lujs.cn/confluence/pages/viewpage.action?pageId=513475168

获取机器码 hwid
python api_service/pallas_api/libs/licensing/get_machine_code.py

api_service/pallas_api/libs/licensing/verify_licens.py
替换 hwid 执行py文件



http://172.23.25.202:8012/#/login

update user set password=password(‘123') where user='root' and host='localhost'; 
flush privileges;
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY '123456'  
flush privileges;      


30.94.4.8
QfiMzM6MDN6IDMgETMtITMtkTMwIjIgojIzR3X0JXY0N3XlNnblNWasJCIsIiMuIjLw4CMxICI6ICcp9lbvlGdhZXa0NWYfR3chxmIgwiI3AjO2QjOyADIxETLyETL5EDMyICI6Iyc09lbvlGdhZXa0NWYfR3chxmIgwSO5kDI6IyclJXawhXZfV2cuV2YpxmIgwiIXNVVVR1U3IVSNFFN3YjUKNVRGhjNLVFWRJCI6Iiblt2b0JCIsgTOgojIn5WaulWYtVmciACLiEWOiVGNkVTZhZzNlZTNkBTOjJ2MyYmY0cjMxIjZkNzYyEmM2MzYwY2NmVmZ3EjZjhzMjJTNxUWMmRGOkJWO3QjIgojIzRWa3hmIgwiIiAiOi8Wbl1mIgwSZ1JHdgojIkVGbiFmblJCIsISMwozM0ojMwASMx0iMx0SOxAjMiAiOiUGdhRGd1NmIgwSMgojIkl2XwBXYiACLyEDI6ICZpJye



创建sueprvisor文件
`python manage.py create_supervisor_conf -p /wls/miniconda3/envs/sqlreview/bin/ -u yangyun
`
复制support/default-config.ini文件到项目目录下替换相关的配置







