from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator

from etlTbFact.loadFactCddv import loadFactCddv
from etlTbFact.loadFactHoaDon import loadFactHoaDon
from etlTbFact.loadFactTtdt import loadFactTtdt
from etlTbFact.loadFactTtxn import loadFactTtxn
from etlTbDim.loadDimBranch1 import loadBranch1
from etlTbDim.loadDimBranch2 import loadBranch2
from etlTbDim.loadDimBranch3 import loadBranch3
from etlTbDim.loadDimBranch4 import loadBranch4
from etlTbDim.loadDimBranch5 import loadBranch5
from etlTbDb.sparkTb import Connect
# from airflow.providers.mongo.hooks.mongo import MongoHook

with DAG(
    dag_id="load_clh",
    start_date=datetime(2023, 4, 10, 8, 0, 0),
    catchup=False,
    tags=['etl', 'v4'],
    schedule_interval=timedelta(days=2000)
) as dag:
    with TaskGroup(group_id='group_insert_dim', tooltip="group_insert_dim") as group_insert_dim:
        start = DummyOperator(task_id="startDim")
        taskDim1 = loadBranch1(task_id='load_Dim_Branch_1')
        taskDim2 = loadBranch2(task_id='load_Dim_Branch_2')
        taskDim3 = loadBranch3(task_id='load_Dim_Branch_3')
        taskDim4 = loadBranch4(task_id='load_Dim_Branch_4')
        taskDim5 = loadBranch5(task_id='load_Dim_Branch_5')
    
        # start >> taskDim3 >> taskDim4 >> taskDim5 >> taskDim1 >> taskDim2
        start >> [taskDim5, taskDim1]
        taskDim1 >> taskDim4 >> taskDim3 >> taskDim2
        
    
    with TaskGroup(group_id='group_insert_fact', tooltip="group_insert_fact") as group_insert_fact:
        startFact = DummyOperator(task_id="startFact")
        taskFact1 = loadFactCddv(task_id='load_fact_chi_dinh_dich_vu')
        taskFact2 = loadFactHoaDon(task_id='load_fact_hoa_don')
        taskFact3 = loadFactTtdt(task_id='load_fact_thong_tin_dieu_tri')
        taskFact4 = loadFactTtxn(task_id='load_fact_thong_tin_xet_nghiem')
        startFact >> [taskFact1, taskFact2]
        taskFact2 >> taskFact4 >> taskFact3
        
    group_insert_dim >> group_insert_fact
    