# EMR Jupyter Notebooks


Setting up EMR Jupyter Notebooks for testing in AWS. Click Create notebook  
![alt text](./images/emr-jupyter-notebook/EMRNotebook1.png "EMR Jupyter Notebook")

You have two options here either to use existing cluster or create a new cluster. Sometimes do note that Notebooks might not be available in certain zones. When you come across those scenarios, you will have to create EC2 Keypair again for that zone and proceed creating the notebook  
![alt text](./images/emr-jupyter-notebook/EMRNotebook2.png "EMR Jupyter Notebook")

Once you click the Create Cluster, it will be in the pending state sometimes for verification purposes this will be about 5mins or so.  
![alt text](./images/emr-jupyter-notebook/EMRNotebook3-NBmaynotbeavailableinallzones.png "EMR Jupyter Notebook")

Once the state changes to 'Ready' you are good to open the jupyter notebook.  
![alt text](./images/emr-jupyter-notebook/EMRNotebook4-Ready.png "EMR Jupyter Notebook")

In EMR Notebooks, its best to change the kernel from Python to PySpark and use PySpark commands to install the packages.   
![alt text](./images/emr-jupyter-notebook/EMRNotebook5-Jupyter.png "EMR Jupyter Notebook")

After using the Notebook, come to the notebooks main screen and click Stop to stop the notebook  
![alt text](./images/emr-jupyter-notebook/EMRNotebook6-Stop.png "EMR Jupyter Notebook")

Once the status changes to Stopped you can proceed to Terminate the cluster  
![alt text](./images/emr-jupyter-notebook/EMRNotebook7-Stopped.png "EMR Jupyter Notebook")

Terminate the cluster created for the notebook  
![alt text](./images/emr-jupyter-notebook/EMRNotebook8-Terminate.png "EMR Jupyter Notebook")

EMR Notebook terminated  
![alt text](./images/emr-jupyter-notebook/EMRNotebook9-Terminated.png "EMR Jupyter Notebook")

## Thats all about EMR Jupyter Notebooks  