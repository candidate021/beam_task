#!/bin/bash

MODE=$1

##Install dependencies (preferably within a venv)
function install_requirements() {
    echo "installing requirements from requirements.txt file."
    python3 -m pip install -r requirements.txt
    echo "successfully installed requirements"
}

#Run task_1 pipeline
function run_task_1_pipeline() {
    echo "running task_1 apache beam batch job."
    python3 task_1/pipeline/main.py
}

#Run task_2 pipeline
function run_task_2_pipeline() {
    echo "running task_2 apache beam batch job."
    python3 task_2/pipeline/main.py
}

#Run test pipeline
function run_test_pipeline() {
    echo "running tests pipeline."
    python3 -m unittest -v task_2/test/test_pipeline.py
}

if [[ $MODE == "install" ]];
    then
        install_requirements
elif [[ $MODE == "task_1" ]];
    then
        run_task_1_pipeline
elif [[ $MODE == "task_2" ]];
    then
        run_task_2_pipeline
elif [[ $MODE == "test" ]];
    then
        run_test_pipeline
else
    echo "Mode argument must be one of (install, task_1, task_2, test)"
fi