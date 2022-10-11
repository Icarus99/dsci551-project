# dsci551-project
# TO allow jupyter notebook to process large data input/output/transit, when open jupyter in terminal, 
# run: jupyter notebook --NotebookApp.iopub_data_rate_limit=1.0e10


# TEST put, getPartitionLocations, readPartition, run following command:
# edfs.put('Salary_Dataset.csv', '/user/jack', 3)
# edfs.getPartitionLocations('/user/jack/Salary_Dataset')
# edfs.readPartition('/user/jack/Salary_Dataset', 1)