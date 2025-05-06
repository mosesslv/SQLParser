from django.core.management.base import BaseCommand
import os
from django.conf import settings

class Command(BaseCommand):
    help = 'It is a fake command, Import init data for test'

    def add_arguments(self, parser):
        parser.add_argument('-p', '--path', dest='path', default=False, help='python path')
        parser.add_argument('-u', '--user', dest='user', default=False, help='user')

    def handle(self, *args, **options):
        conf = """
        [unix_http_server]
        file={2}/supervisor.sock   ; (the path to the socket file)
        
        [inet_http_server]         ; inet (TCP) server disabled by default
        port=127.0.0.1:9011        ; (ip_address:port specifier, *:port for all iface)
        username=root              ; (default is no username (open server))
        password=Pa888888          ; (default is no password (open server))
        
        [supervisord]
        logfile={2}/supervisord.log ; (main log file;default $CWD/supervisord.log)
        logfile_maxbytes=150MB        ; (max main logfile bytes b4 rotation;default 50MB)
        logfile_backups=10           ; (num of main logfile rotation backups;default 10)
        loglevel=debug                ; (log level;default info; others: debug,warn,trace)
        pidfile={2}/supervisord.pid ; (supervisord pidfile;default supervisord.pid)
        nodaemon=false               ; (start in foreground if true;default false)
        minfds=1024                  ; (min. avail startup file descriptors;default 1024)
        minprocs=200                 ; (min. avail process descriptors;default 200)
        
        [rpcinterface:supervisor]
        supervisor.rpcinterface_factory = supervisor.rpcinterface:make_main_rpcinterface
        
        [supervisorctl]
        serverurl=unix://{2}/supervisor.sock ; use a unix:// URL  for a unix socket
        
        [program:pallas-web]
        command=python2.7 -m SimpleHTTPServer 8012
        process_name=%(program_name)s ; process_name expr (default %(program_name)s)
        numprocs=1                    ; number of processes copies to start (def 1)
        directory={1}/web/dist             ; directory to cwd to before exec (def no cwd)
        autostart=true                ; start at supervisord start (default: true)
        stopsignal=QUIT               ; signal used to kill process (default TERM)
        user={3}                   ; setuid to this UNIX account to run the program
        redirect_stderr=true          ; redirect proc stderr to stdout (default false)
        stdout_logfile={2}/pallas-web.log        ; stdout log path, NONE for none; default AUTO
        #environment=APP_ENV="Production"       ; process environment additions (def no adds)
        stopasgroup=true
        killasgroup=true
        
        [program:pallas-app]
        command={0}python3 {0}gunicorn sqlreview.wsgi:application -t 1800 -w 2 --bind 0.0.0.0:8011
        process_name=%(program_name)s ; process_name expr (default %(program_name)s)
        numprocs=1                    ; number of processes copies to start (def 1)
        directory={1}             ; directory to cwd to before exec (def no cwd)
        autostart=true                ; start at supervisord start (default: true)
        stopsignal=QUIT               ; signal used to kill process (default TERM)
        user={3}                   ; setuid to this UNIX account to run the program
        redirect_stderr=true          ; redirect proc stderr to stdout (default false)
        stdout_logfile={2}/pallas-app.log        ; stdout log path, NONE for none; default AUTO
        stopasgroup=true
        killasgroup=true
        
        [program:pallas-celery]
        command={0}python3 {0}celery -A sqlreview worker -l info -Ofair
        process_name=%(program_name)s ; process_name expr (default %(program_name)s)
        numprocs=1                    ; number of processes copies to start (def 1)
        directory={1}              ; directory to cwd to before exec (def no cwd)
        autostart=true                ; start at supervisord start (default: true)
        stopsignal=QUIT               ; signal used to kill process (default TERM)
        user={3}                   ; setuid to this UNIX account to run the program
        redirect_stderr=true          ; redirect proc stderr to stdout (default false)
        stdout_logfile={2}/pallas-celery.log        ; stdout log path, NONE for none; default AUTO
        stopasgroup=true
        killasgroup=true
        """
        if options.get('path') and options.get('user'):
            cwd = os.getcwd()
            py_path = options.get('path')
            log_path = settings.LOG_ROOT
            user = options.get('user')
            conf = conf.format(py_path, cwd, log_path, user)
            with open(os.path.join(cwd, 'support', 'supervisord.conf'), 'w') as f:
                for l in conf.split('\n'):
                    f.write(l.strip() + '\n')
