**Solution Overview**

This repo contains the solution for the tasks below.

## Task 1
Write an Apache Beam batch job in Python satisfying the following requirements
1. Read the input from `gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv`
1. Find all transactions have a `transaction_amount` greater than `20`
1. Exclude all transactions made before the year `2010`
1. Sum the total by `date`
1. Save the output into `output/results.jsonl.gz` and make sure all files in the `output/` directory is git ignored

If the output is in a CSV file, it would have the following format
```
date, total_amount
2011-01-01, 12345.00
...
```

## Task 2
Following up on the same Apache Beam batch job, also do the following 
1. Group all transform steps into a single `Composite Transform`
1. Add a unit test to the Composite Transform using tooling / libraries provided by Apache Beam

## Solution

**Dependencies**

Apache-beam python sdk is required to execute the pipelines. If not installed, the below command can be run to install the apache-beam package (version 2.45.0 - latest at time of writing).

```
./run.sh install
```

Alternatively, the below command will install the same package.

```
pip3 install -r requirements.txt
```

**Running the jobs**

The `run.sh` file can be used a single entrypoint for installing dependencies and starting each job.

**Running task_1 pipeline**

Task_1 pipeline is stored in the task_1/pipeline/ folder and can be run with the following command

```
./run.sh task_1
```

Alternatively, you can cd into the task_1/pipeline/ folder and execute the following to start the task_1 pipeline.

```
python3 main.py
```

**Running task_2 pipeline**

Task_2 pipeline is stored in the task_2/pipeline/ folder and can be run with the following command

```
./run.sh task_2
```

Alternatively, you can cd into the task_2/pipeline/ folder and execute the following to start the task_2 pipeline.

```
python3 main.py
```

**Running the unit test pipeline**

The unit test pipeline for the CompositeTransform is stored in the task_2/test/ folder and can be run with the following command.

```
./run.sh test
```

Alternatively, you can cd into the task_2/test/ folder and execute the following to start the unit tests pipeline.

```
python3 -m unittest -v test_pipeline.py
```