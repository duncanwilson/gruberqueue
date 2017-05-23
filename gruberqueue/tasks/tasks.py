from celery.task import task
from dockertask import docker_task
from subprocess import call,STDOUT
import requests
import os
import json as jsonx

#Default base directory 
basedir="/data/static/campgruber"


@task()
def add_usingR(x,y):
    task_id = str(add_usingR.request.id)
    resultDir = setup_result_directory(task_id)
    docker_opts = '-v /opt/osucybercom/data/static/campgruber:/script:z -w /script '	
    docker_cmd ="Rscript /script/add_usingR.R {0} {1}".format(x,y)
    print docker_cmd, docker_opts
    result = docker_task(docker_name="cybercom_r",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
    result_url ="http://{0}/campgruber/tasks/{1}".format("cybercom-app.hpc.okstate.edu",task_id)
    return result_url

    
@task()
def runRscript_file(args):
    """
        Generic task to batch submit to R
        args: run parameters saved as a text file in json format
              The run parameters will be read into R as part of the R script
              Users will need to know structure of the parms file
        kwargs: keyword arg is the R script filename
    """
    task_id = str(runRscript_file.request.id)
    resultDir = setup_result_directory(task_id)
    #host_data_resultDir = "{0}/static/someapp_tasks/{1}".format(host_data_dir,task_id)
    with open(resultDir + '/input/args.json', "w") as f:
        jsonx.dump(args,f)
    #Run R Script
    docker_opts = " -v /opt/osucybercom/data/static/campgruber:/script:z -w /script "
    docker_cmd =" Rscript /script/simple.R "
    result = docker_task(docker_name="cybercom_r",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
    reportDir = os.path.join('/opt/osucybercom/data/static/campgruber/tasks/', task_id, 'report')
    print(reportDir)
    tmp = '{0}/testing_R.txt'.format(reportDir)
    os.rename("/opt/osucybercom/data/static/campgruber/testing_R.txt", tmp)
    result_url ="http://{0}/campgruber/tasks/{1}".format("cybercom-app.hpc.okstate.edu",task_id)
    return result_url

	
def setup_result_directory(task_id):
    resultDir = os.path.join(basedir, 'tasks/', task_id)
    os.makedirs(resultDir)
    os.chmod(resultDir,0777)
    os.makedirs("{0}/input".format(resultDir))
    os.chmod("{0}/input".format(resultDir),0777)
    os.makedirs("{0}/output".format(resultDir))
    os.chmod("{0}/output".format(resultDir),0777)
    os.makedirs("{0}/report".format(resultDir))
    os.chmod("{0}/report".format(resultDir),0777)
    return resultDir 

