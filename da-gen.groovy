import java.text.SimpleDateFormat
def date = new Date()
sdf = new SimpleDateFormat("yyyyMMddHHmmss")
run_id = sdf.format(date)
sdf_for_email = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
run_id_for_email = sdf_for_email.format(date)
println "Run id: ${run_id}"

def checkpoint_job_url_params = ""
def etl_version_old = "${params.etl_version_old}"
if (!etl_version_old?.trim()) {
	etl_version_old = "${params.etl_version}"
}

def blob_path = ""
def storage_account = ""
def envir = "${params.env}"
def blob_shared = ""
def snapshot_id = ""

if (envir == 'DEV') {
    blob_path = "data/input/DaVinciProjectOneDrive"
    storage_account = "https://davincidev01.blob.core.windows.net"
    blob_account_name = "davincidev01"
    blob_shared = "dev_blob_shared_id"
    host_prod = "https://azl-davinci016.np-cloud-pg.com:8443/job/PROD-COPY-ENV"
    target_folder = "davinci-dry-run-datalake"
    sas_blob_shared = "sas_dev_blob_shared_id"
    mlflow_url = "local" // "http://azl-davinci016.np-cloud-pg.com:5000"
    api_token_shared_id = "api_token_shared_id_dev"
    api_token_shared_id_w_token = "api_token_davincinonprod"
    shared_id_user_w_psw = "shared-id2"
} else {
    blob_path = "data/input/DaVinciProjectOneDrive"
    storage_account = "https://davincistorage01.blob.core.windows.net"
    blob_account_name = "davincistorage01"
    blob_shared = "prod_blob_shared_id"
    host_prod = "https://azl-davincipr01.cloud-pg.com:8443"
    target_folder = "davinci-datalake"
    sas_blob_shared = "sas_prod_blob_shared_id"
    mlflow_url = "http://azl-davincipr01.cloud-pg.com:5000"
    api_token_shared_id = "api_token_shared_id_prod"
    api_token_shared_id_w_token = "api_token_davinci"
    shared_id_user_w_psw = "shared-id"
}

def get_URL_encoded_params(params) {
    // params is a list of the 2-element lists [param_name, param_value]
    def url_params = []
    def url_param = ""

    for (param in params) {
        url_param = java.net.URLEncoder.encode(param[1], "UTF-8")
        url_params.add("${param[0]}=${url_param}")
    }

    return url_params.join("&")
}

//Specify variable parameters for automatically launched pipeline RU-Checkpoint in post-success
//def branch_name = "master"
//def location = "UK"
//def etl_version = "v7"
//def ml_step_args = "-p checkpoint"

pipeline {
    agent {
        node {
            label 'slave'
        }
    } 
    // agent any
	
    parameters {
        string(name: 'env', defaultValue: 'DEV', description: '')			//PROD
        string(name: 'branch_name', defaultValue: 'cancelled_promos', description: '')	//master
        string(name: 'location', defaultValue: 'UK', description: '--c, --country')
        string(name: 'etl_version', defaultValue: 'v9', description: '')
        string(name: 'snapshot_id', defaultValue: '20200225', description: 'The date of snapshot production')
        string(name: 'da_gen_params', defaultValue: '', description: '')
        string(name: 'blob_source_container_name', defaultValue: 'davinci-dry-run', description: 'Container name where source dataset is uploaded.')
        string(name: 'datasets_folder', defaultValue: 'davinci-upload', description: 'Folder name where source dataset is uploaded.')
        string(name: 'snapshot_id_old', defaultValue: '20200218', description: '')
    }
	
    stages {
        stage('DA Generation') {
            steps {
                sh("sudo rm -rf ./* && rm -rf ./.git*")
                sh("git config --global http.sslVerify false")
                git(
                    url: 'https://github.com/procter-gamble/davinci-wip.git',
                    credentialsId: 'github-CiCdBot-png-PAT',
                    branch: "${params.branch_name}"
                )
                withCredentials([string(credentialsId: "${blob_shared}", variable: 'BLOB_KEY'), string(credentialsId: "${sas_blob_shared}", variable: 'SAS_KEY')]) {
                    // Download original dataset
                    // sh("/usr/bin/keyctl new_session")
                    // sh(". /opt/miniconda3/etc/profile.d/conda.sh && conda env list && conda create --name davinci python=3.6.6 && conda activate davinci && conda env list && conda env update -f environment.yml && conda activate davinci && conda env list")
                    sh("sudo mkdir -p  'data/input/DaVinciProjectOneDrive/' ")

                    sh("sudo chown --recursive $USER data/input/")
                
                    sh("sudo azcopy copy  '${storage_account}/${params.blob_source_container_name}/${params.datasets_folder}/${params.snapshot_id_old}${SAS_KEY}'  'data/input/DaVinciProjectOneDrive/' --recursive=true")
                    sh("sudo azcopy copy  '${storage_account}/${params.blob_source_container_name}/${params.datasets_folder}/${params.snapshot_id}${SAS_KEY}'  'data/input/DaVinciProjectOneDrive/' --recursive=true")
                    sh("sudo azcopy copy  '${storage_account}/${params.blob_source_container_name}/common/gtin_fpc_customer_mapping.xlsx${SAS_KEY}'  'data/input/DaVinciProjectOneDrive/${params.snapshot_id}/fpc_mapping/uk/gtin-fpc customer mapping.xlsx' ")
                    
                    sh("sudo chown --recursive $USER data/input/DaVinciProjectOneDrive/${params.snapshot_id}")
                    sh("sudo chown --recursive $USER data/input/DaVinciProjectOneDrive/${params.snapshot_id_old}")
                    sh("sudo find data/input/DaVinciProjectOneDrive/${params.snapshot_id} -type f -exec chmod 644 {} \\;")
                    // Download ETL results

                    sh("sudo azcopy copy  '${storage_account}/${target_folder}/snapshots/${params.location}/${params.etl_version}/etl_output__${params.etl_version}__${params.location}__${params.snapshot_id}.parquet${SAS_KEY}'   'data/lake/snapshots/${params.location}/${params.etl_version}/etl_output__${params.etl_version}__${params.location}__${params.snapshot_id}.parquet' --recursive=true")
                    // Download ML results
                
                    sh("sudo azcopy copy  '${storage_account}/${target_folder}/future_predictions/${params.location}/${params.snapshot_id}/all${SAS_KEY}'  'data/lake/ml_results/' --recursive=true")
                    // Download DA results.

                    sh("sudo azcopy copy  '${storage_account}/${target_folder}/da_results/${SAS_KEY}'  'data/lake/' --recursive=true")
                
                    sh("sudo chown --recursive $USER data/lake/")
                    sh("sudo find data/lake/ -type f -exec chmod 644 {} \\;")
                
                    // sh(". /opt/miniconda3/etc/profile.d/conda.sh && conda activate davinci && conda env list && python dav.py da-gen -c ${params.location} --dataset-id ${snapshot_id} --etl-version ${params.etl_version} --etl-local \"data/lake/snapshots/${params.location}/${params.etl_version}/etl_output__${params.etl_version}__${params.location}__${snapshot_id}.parquet\" --ml-results-path \"data/lake/ml_results/\" --da-local \"data/lake/da_results\" ${params.da_gen_params} ")
                    sh(". /opt/miniconda3/etc/profile.d/conda.sh && conda env list && conda create --name davinci python=3.6.6 && conda activate davinci && conda env list && conda env update -f environment.yml && conda activate davinci && conda env list && python dav.py da-gen -c ${params.location} --dataset-id ${params.snapshot_id} --etl-version ${params.etl_version} --etl-local \"data/lake/snapshots/${params.location}/${params.etl_version}/etl_output__${params.etl_version}__${params.location}__${params.snapshot_id}.parquet\" --ml-results-path \"data/lake/ml_results/\" --da-local \"data/lake/da_results\" ${params.da_gen_params} ")
                
                    sh("sudo azcopy copy 'data/lake/da_results/${params.snapshot_id}/${params.location}/'   '${storage_account}/${target_folder}/da_results/${params.snapshot_id}${SAS_KEY}'  --recursive=true")
                    // Upload to SharePoint
                

                    script {
                        if (envir == 'PROD') {
                            sh(". /opt/miniconda3/etc/profile.d/conda.sh && conda activate davinci && conda env list && python dav.py sharepoint-upload --folder_path data/lake/da_results ")
                        }
                    }
                }
            }
        }
    }
}
