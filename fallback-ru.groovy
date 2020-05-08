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
def blob_account_name = ""
def envir = "${params.env}"
def location = "${params.location}"
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

def issue_title = "[${envir}] ${run_id} ${env.JOB_NAME} FAILURE"
def issue_descr = "${envir}:\nPipeline ${env.JOB_NAME} execution result: FAILURE\nJOB_NAME: ${env.JOB_NAME}\nBUILD_NUMBER: ${env.BUILD_NUMBER}\nTime: ${run_id_for_email}\nMore info (BUILD_URL): ${env.BUILD_URL}\n\nWith parameters:\nbranch_name: ${params.branch_name}\nlocation: ${params.location}\nwaiting_timeout: ${params.waiting_timeout}\nwaiting_interval: ${params.waiting_interval}\nml_step_args: ${params.ml_step_args}\ndatasets_folder: ${params.datasets_folder}\netl_params: ${params.etl_params}\netl_version: ${params.etl_version}\nml_directory: ${params.ml_directory}\nblob_account_name: ${blob_account_name}\nblob_source_container_name: ${params.blob_source_container_name}"

pipeline {
    agent {
        node {
            label 'slave'
        }

    }
    // agent any

/*
    parameters {
        string(name: 'env', defaultValue: 'DEV', description: '')		//PROD
        string(name: 'branch_name', defaultValue: 'dev', description: '')	//master
        string(name: 'location', defaultValue: 'RU', description: '')
        string(name: 'waiting_timeout', defaultValue: '4320', description: 'Timeout (in minutes) for waiting dataset uploading on blob storage.')
        string(name: 'waiting_interval', defaultValue: '5', description: 'Interval (in minutes) between checking datasets in blob storage.')
        string(name: 'etl_params', defaultValue: '--mild-deoverlap', description: 'Parameters for ETL step.')
        string(name: 'etl_version', defaultValue: 'v9', description: '')
        string(name: 'ml_directory', defaultValue: 'prod_pipeline', description: '')
        string(name: 'ml_step_args', defaultValue: '-p future -x complex', description: 'Parameter for ML step.')
        string(name: 'blob_source_container_name', defaultValue: 'davinci-dry-run', description: 'Container name where source dataset is uploaded.')
        string(name: 'datasets_folder', defaultValue: 'davinci-upload', description: 'Folder name where source dataset is uploaded.')
        string(name: 'da_gen_params', defaultValue: '', description: '')
        string(name: 'snapshot_id_old', defaultValue: '20200317', description: 'Previous snapshot used for the validation step. Comparing ETLs.')
        string(name: 'etl_version_old', defaultValue: '', description: 'Leave it empty if you are confident that previous Weekly Run was with the same etl_version as current one, otherwise specify value of etl_version param of previous weekly run. It is used as reference for "snapshot_id_old" for comparison. Sometimes etl_version is changed. And thus we need a way to identify location of this old snapshot.')
        string(name: 'logs_path', defaultValue: 'data/lake/validation_logs/', description: '')
        string(name: 'fpi_blob_source_container_name', defaultValue: 'davinci-fallback', description: 'Container name for FPI where source dataset is uploaded.')
        string(name: 'fpi_datasets_folder', defaultValue: 'davinci-upload-ru', description: 'Folder name where source dataset for fallback run is uploaded.')
        string(name: 'FALLBACK_TIMEOUT_IN_SECONDS', defaultValue: '1800', description: '')
    }
*/
    stages {
        
        stage ('Prepare DataSet'){
            steps {
                sh("sudo rm -rf ./* && rm -rf ./.git*")
                git(
                    url: 'https://github.com/procter-gamble/davinci-wip.git',
                    credentialsId: 'github-CiCdBot-png-PAT',
                    branch: "${params.branch_name}"
                )
                withCredentials([string(credentialsId: "${blob_shared}", variable: 'BLOB_KEY'), string(credentialsId: "${sas_blob_shared}", variable: 'SAS_KEY')]) {

                    sh("sudo mkdir -p \"data/lake/validation_logs\"")

                    sh("sudo chown --recursive $USER data/lake/")
                    sh("sudo find data/lake/ -type f -exec chmod 644 {} \\;")
                    sh("sudo chown --recursive $USER data/output/")
                    sh("sudo find data/output/ -type f -exec chmod 644 {} \\;")

                    sh(". /opt/miniconda3/etc/profile.d/conda.sh && conda env list && conda create --name davinci python=3.6.6 && conda activate davinci && conda env list && conda env update -f environment.yml && conda activate davinci && conda env list")
                    script {
                        try{
                            sh(". /opt/miniconda3/etc/profile.d/conda.sh && ./env_update.sh && conda activate davinci && conda env list && python ./dav.py fallback --source-key \"${BLOB_KEY}\" --account ${blob_account_name} --container ${params.blob_source_container_name} -f ${params.datasets_folder} -c ${params.location} -l ${params.logs_path} -d \"data/output/\"")
                            script { snapshot_id = sh ( script: 'cat ./current_snapshot', returnStdout: true ).trim() }

                            sh("sudo azcopy copy  'data/output/${snapshot_id}/*' '${storage_account}/${params.fpi_blob_source_container_name}/${params.fpi_datasets_folder}/${snapshot_id}${SAS_KEY}' --recursive=true")
                            sh("sudo azcopy copy  '${params.logs_path}' '${storage_account}/${params.fpi_blob_source_container_name}/validation_logs/${snapshot_id}/${params.location}/fallback${SAS_KEY}' --recursive=true")
                        } finally {
                            // sh("sudo azcopy --quiet  --dest-key \"${BLOB_KEY}\" --destination \"${storage_account}/${params.fpi_blob_source_container_name}/validation_logs/${snapshot_id}/${params.location}/fallback\" --source \"${params.logs_path}\" --recursive")
                        }
                    }
                }
            }
        }
		
        stage ('Wait for files'){
            steps {
                sh("sudo rm -rf ./* && rm -rf ./.git*")
                git(
                    url: 'https://github.com/procter-gamble/davinci-wip.git',
                    credentialsId: 'github-CiCdBot-png-PAT',
                    branch: "${params.branch_name}"
                )
                withCredentials([string(credentialsId: "${blob_shared}", variable: 'BLOB_KEY'), string(credentialsId: "${sas_blob_shared}", variable: 'SAS_KEY')]) {
                    sh(". /opt/miniconda3/etc/profile.d/conda.sh && conda env list && conda create --name davinci python=3.6.6 && conda activate davinci && conda env list && conda env update -f environment.yml && conda activate davinci && conda env list")
                    sh(". /opt/miniconda3/etc/profile.d/conda.sh && ./env_update.sh && conda activate davinci && conda env list && python ./dav.py validation prod-pre-etl --source-key \"${BLOB_KEY}\" --account ${blob_account_name} --container ${params.fpi_blob_source_container_name} -f ${params.fpi_datasets_folder} -i ${params.waiting_interval} -t ${params.waiting_timeout} -c ${params.location}")
                }
                script { snapshot_id = sh ( script: 'cat ./current_snapshot', returnStdout: true ).trim() }
                script { echo "snapshot_id: ${snapshot_id}" }
            }
        }

        stage ('Pre-ETL') {
            steps {
                sh ( "rm -rf \$(ls -I \"current_snapshot\")")
                script { snapshot_id = sh ( script: 'cat ./current_snapshot', returnStdout: true ).trim() }
                script { echo "Pre-ETL snapshot_id: ${snapshot_id}" }
                git(
                    url: 'https://github.com/procter-gamble/davinci-wip.git',
                    credentialsId: 'github-CiCdBot-png-PAT',
                    branch: "${params.branch_name}"
                )
                // script {println snapshot_id}
                // python ./dav.py validation pre-etl -s ${params.snapshot} --source-key ${params.sourcekey}" -t 600 -i 10 -cm
                withCredentials([string(credentialsId: "${blob_shared}", variable: 'BLOB_KEY'), string(credentialsId: "${sas_blob_shared}", variable: 'SAS_KEY')]) {
                    sh("sudo mkdir -p \"data/lake/validation_logs\"")

                    sh("mkdir -p 'data/input/DaVinciProjectOneDrive/'")

                    sh("sudo azcopy copy  '${storage_account}/${params.fpi_blob_source_container_name}/${params.fpi_datasets_folder}/${snapshot_id}/${SAS_KEY}' 'data/input/DaVinciProjectOneDrive/' --recursive=true")
                    sh("sudo azcopy copy  '${storage_account}/${params.blob_source_container_name}/${params.datasets_folder}/${params.snapshot_id_old}/${SAS_KEY}'  'data/input/DaVinciProjectOneDrive/' --recursive=true")

                    sh("sudo chown --recursive $USER data/lake/")
                    sh("sudo chown --recursive $USER data/input/")
                    sh("sudo find data/lake/ -type f -exec chmod 644 {} \\;")
                    sh("sudo find data/input/ -type f -exec chmod 644 {} \\;")
                    sh("sudo ls -hal data/lake")
                    sh("sudo ls -hal data/input")
                    script {
                        try{
                            sh(". /opt/miniconda3/etc/profile.d/conda.sh && ./env_update.sh && conda activate davinci && conda env list && python ./dav.py validation pre-etl -s ${snapshot_id} -d ${snapshot_id} -sp ${params.snapshot_id_old} --source-key '${BLOB_KEY}' --account ${blob_account_name} --container ${params.blob_source_container_name} --fpi-container ${params.fpi_blob_source_container_name}  -f ${params.datasets_folder} -ffpi ${params.fpi_datasets_folder} -c ${params.location} -l ${params.logs_path} -p \"data/input/DaVinciProjectOneDrive/\"")
                        } finally {
                            sh("sudo azcopy copy  '${params.logs_path}' '${storage_account}/${params.fpi_blob_source_container_name}/validation_logs/${snapshot_id}/${params.location}/pre-etl${SAS_KEY}' --recursive=true")
                        }
                    }
                }
            }
        }

        stage ('ETL') {
            steps {
                // sh("fusermount -u ./data/lake/ -q")
                sh ( "rm -rf \$(ls -I \"current_snapshot\")")
                script { snapshot_id = sh ( script: 'cat ./current_snapshot', returnStdout: true ).trim() }
                script { echo "ETL snapshot_id: ${snapshot_id}" }
                sh("git config --global http.sslVerify false")
                git(
                    url: 'https://github.com/procter-gamble/davinci-wip.git',
                    credentialsId: 'github-CiCdBot-png-PAT',
                    branch: "${params.branch_name}"
                )
                withCredentials([string(credentialsId: "${blob_shared}", variable: 'BLOB_KEY'), string(credentialsId: "${sas_blob_shared}", variable: 'SAS_KEY')]) {

                    // sh("PATH=$PATH:~/azcopy_linux_amd64_10.2.1/")
                    sh("echo $PATH")
                    // sh("alias azcopy='~/azcopy_linux_amd64_10.2.1/azcopy'")
                    sh("sudo which azcopy")
                    // script { etl_version = sh (  script: 'git describe --abbrev=0',  returnStdout: true ).trim().tokenize( '_' )[1] }
                    // echo "etl_version: ${etl_version}"

                    sh("mkdir -p 'data/input/DaVinciProjectOneDrive/'")
                    sh("mkdir -p 'data/input/DaVinciProjectOneDrive/Supplemental inputs/'")

                    sh("sudo azcopy copy '${storage_account}/${params.blob_source_container_name}/common/SELECT_DISTINCT_MARKET_DAY_NAME_TYPE_STORES_CLOSED_STORES_CLOSED_201904121041.csv${SAS_KEY}'   'data/input/DaVinciProjectOneDrive/Supplemental inputs/SELECT_DISTINCT_MARKET_DAY_NAME_TYPE_STORES_CLOSED_STORES_CLOSED_201904121041.csv' ")
                    sh("sudo azcopy copy '${storage_account}/${params.fpi_blob_source_container_name}/${params.fpi_datasets_folder}/${snapshot_id}/${SAS_KEY}'  'data/input/DaVinciProjectOneDrive/' --recursive=true")

                    sh("sudo chown --recursive $USER data/input/")
                    sh("sudo find data/input/ -type f -exec chmod 644 {} \\;")
                    sh("sudo ls -hal data/input")

                    // sh("sudo chown --recursive $USER data/input/DaVinciProjectOneDrive/${snapshot_id}")
                    // sh("sudo chown --recursive $USER \"data/input/DaVinciProjectOneDrive/Supplemental inputs\"")
                    // sh("sudo find data/input/DaVinciProjectOneDrive/${snapshot_id} -type f -exec chmod 644 {} \\;")

                    sh(". /opt/miniconda3/etc/profile.d/conda.sh && ./env_update.sh && conda activate davinci && conda env list && python ./dav.py etl ${params.location} -ms ${snapshot_id} --etl-version ${params.etl_version} ${params.etl_params}")

                    sh("sudo azcopy copy   'data/output/etl_output__${params.etl_version}__${params.location}__${snapshot_id}.parquet' '${storage_account}/${params.fpi_blob_source_container_name}/etl_output/etl_output__${params.etl_version}__${params.location}__${snapshot_id}.parquet${SAS_KEY}'  --recursive=true")
                }
            }
        }

        stage ('Post-ETL') {
            steps {
                sh ( "rm -rf \$(ls -I \"current_snapshot\")")
                script { snapshot_id = sh ( script: 'cat ./current_snapshot', returnStdout: true ).trim() }
                script { echo "Post-ETL snapshot_id: ${snapshot_id}" }
                git(
                    url: 'https://github.com/procter-gamble/davinci-wip.git',
                    credentialsId: 'github-CiCdBot-png-PAT',
                    branch: "${params.branch_name}"
                )

                withCredentials([string(credentialsId: "${blob_shared}", variable: 'BLOB_KEY'), string(credentialsId: "${sas_blob_shared}", variable: 'SAS_KEY')]) {
                    sh(". /opt/miniconda3/etc/profile.d/conda.sh && ./env_update.sh && conda activate davinci && conda env list && python ./dav.py validation prod-pre-etl --source-key '${BLOB_KEY}' --account ${blob_account_name} --container ${params.fpi_blob_source_container_name} -f ${params.fpi_datasets_folder} -c ${params.location} --update-marker-file ${snapshot_id}")

                    sh("sudo mkdir -p \"data/lake/validation_logs\"")
                    sh("sudo chown --recursive $USER data/lake/")
                    sh("sudo find data/lake/ -type f -exec chmod 644 {} \\;")
                    sh("sudo ls -hal data/lake")

                    sh("sudo azcopy copy  '${storage_account}/${params.fpi_blob_source_container_name}/etl_output/etl_output__${params.etl_version}__${params.location}__${snapshot_id}.parquet${SAS_KEY}'  'data/output/' --recursive=true")
                    sh("sudo azcopy copy  '${storage_account}/${target_folder}/snapshots/${params.location}/${etl_version_old}/etl_output__${etl_version_old}__${params.location}__${snapshot_id_old}.parquet${SAS_KEY}'  'data/output/' --recursive=true")

                    sh("sudo chown --recursive $USER data/output/")
                    sh("sudo find data/output/ -type f -exec chmod 644 {} \\;")

                    script {
                        try {
                            sh(". /opt/miniconda3/etc/profile.d/conda.sh && ./env_update.sh && conda activate davinci && conda env list && python ./dav.py validation post-etl -c ${params.location} -v ${params.etl_version} -s \"data/output/etl_output__${params.etl_version}__${params.location}__${snapshot_id}.parquet\" -p \"data/output/etl_output__${etl_version_old}__${params.location}__${params.snapshot_id_old}.parquet\" --source-key '${BLOB_KEY}' --account ${blob_account_name} --container ${params.fpi_blob_source_container_name} -l ${params.logs_path}")

                        } finally {
                            sh("sudo azcopy copy '${params.logs_path}'  '${storage_account}/${params.fpi_blob_source_container_name}/validation_logs/${snapshot_id}/${params.location}/post-etl${SAS_KEY}'   --recursive=true")
                            sh("sudo azcopy copy  '${storage_account}/${params.fpi_blob_source_container_name}/etl_output/etl_output__${params.etl_version}__${params.location}__${snapshot_id}.parquet${SAS_KEY}'  'data/output/' --recursive=true")
                            sh("sudo azcopy copy  'data/output/etl_output__${params.etl_version}__${params.location}__${snapshot_id}.parquet' '${storage_account}/${params.fpi_blob_source_container_name}/snapshots/${params.location}/${params.etl_version}/etl_output__${params.etl_version}__${params.location}__${snapshot_id}.parquet${SAS_KEY}'   --recursive=true ")
                        }
                    }
                }
            }
        }
		
        stage ('ML') {
            steps {
                sh ( "rm -rf \$(ls -I \"current_snapshot\")")
                script { snapshot_id = sh ( script: 'cat ./current_snapshot', returnStdout: true ).trim() }
                script { echo "ML snapshot_id: ${snapshot_id}" }
	        sh("git config --global http.sslVerify false")
                git(
                    url: 'https://github.com/procter-gamble/davinci-wip.git',
                    credentialsId: 'github-CiCdBot-png-PAT',
                    branch: "${params.branch_name}"
                )
                withCredentials([string(credentialsId: "${blob_shared}", variable: 'BLOB_KEY'), string(credentialsId: "${sas_blob_shared}", variable: 'SAS_KEY')]) {
		    sh("sudo azcopy copy  '${storage_account}/${params.fpi_blob_source_container_name}/snapshots/${params.location}/${params.etl_version}/etl_output__${params.etl_version}__${params.location}__${snapshot_id}.parquet${SAS_KEY}'  'data/lake/snapshots/${params.location}/${params.etl_version}/etl_output__${params.etl_version}__${params.location}__${snapshot_id}.parquet' --recursive=true")
                    // UK (RU) mapping tables
                    sh("sudo azcopy copy '${storage_account}/${params.blob_source_container_name}/common/remapping_tables/*${SAS_KEY}' 'data/input' --recursive=true")
                    sh("sudo chown --recursive $USER data/")
                    sh("sudo find data/ -type f -exec chmod 644 {} \\;")

                    // sh("sudo chown --recursive $USER data/lake/")
                    // sh("sudo find data/lake/ -type f -exec chmod 644 {} \\;")
                    sh(". /opt/miniconda3/etc/profile.d/conda.sh && ./env_update.sh && conda activate davinci && conda env list && python dav.py run-experiments ${params.ml_directory} -c ${params.location} --mlflow ${mlflow_url} --etl-version ${params.etl_version} ${params.ml_step_args}")

                    sh("sudo azcopy copy 'data/experiments/${params.ml_directory}/*' '${storage_account}/${params.fpi_blob_source_container_name}/future_predictions/${params.location}/${snapshot_id}${SAS_KEY}'  --recursive=true")
                }
            }
        }

        stage ('ML extra artifacts') {
            steps {
                sh ( "rm -rf \$(ls -I \"current_snapshot\")")
                script { snapshot_id = sh ( script: 'cat ./current_snapshot', returnStdout: true ).trim() }
                script { echo "ML extra artifacts snapshot_id: ${snapshot_id}" }

                sh("git config --global http.sslVerify false")
                git(
                    url: 'https://github.com/procter-gamble/davinci-wip.git',
                    credentialsId: 'github-CiCdBot-png-PAT',
                    branch: "${params.branch_name}"
                )
                withCredentials([string(credentialsId: "${blob_shared}", variable: 'BLOB_KEY'), string(credentialsId: "${sas_blob_shared}", variable: 'SAS_KEY')]) {
                    sh("sudo azcopy copy \"${storage_account}/${params.fpi_blob_source_container_name}/future_predictions/${params.location}/${snapshot_id}/all/all/predictions/*${SAS_KEY}\" \"data/post_ml/predictions\" --recursive=true")
                    sh("sudo azcopy copy \"${storage_account}/${params.fpi_blob_source_container_name}/snapshots/${params.location}/${params.etl_version}/etl_output__${params.etl_version}__${params.location}__${snapshot_id}.parquet${SAS_KEY}\" \"data/post_ml/etl/etl_output__${params.etl_version}__${params.location}__${snapshot_id}.parquet\" --recursive=true")
                    sh("sudo azcopy copy \"${storage_account}/${params.fpi_blob_source_container_name}/${params.fpi_datasets_folder}/${snapshot_id}/pea_event_files/${params.location.toLowerCase()}/*${SAS_KEY}\" \"data/post_ml/pea\" --recursive=true")

                    sh("sudo mkdir -p \"data/post_ml/output\" ")
                    sh("sudo chown --recursive $USER \"data/post_ml\" ")
                    sh("sudo find \"data/post_ml\" -type f -exec chmod 644 {} \\;")

                    script {
                        try {
                            if (params.location == "UK") {
                                sh(". /opt/miniconda3/etc/profile.d/conda.sh && ./env_update.sh && conda activate davinci && conda env list && python dav.py post-ml extra-excel --pred \"data/post_ml/predictions\" --output \"data/post_ml/output\" && python dav.py post-ml anaplan-vs-davinci-excel --pred \"data/post_ml/predictions\" --etl \"data/post_ml/etl/etl_output__${params.etl_version}__${params.location}__${snapshot_id}.parquet\" --pea \"data/post_ml/pea\" --output \"data/post_ml/output\" ")
                            } else {
                                sh(". /opt/miniconda3/etc/profile.d/conda.sh && ./env_update.sh && conda activate davinci && conda env list && python dav.py post-ml extra-excel --pred \"data/post_ml/predictions\" --output \"data/post_ml/output\" ")
                            }
                        } finally {
                            sh("sudo azcopy copy \"data/post_ml/output/*\" \"${storage_account}/${params.fpi_blob_source_container_name}/future_predictions/${params.location}/${snapshot_id}/all/all/predictions${SAS_KEY}\" --recursive=true")
                        }
                    }
                }
            }
        }
		
        stage ('Post-ML') {
            steps {
                sh ( "rm -rf \$(ls -I \"current_snapshot\")")
                script { snapshot_id = sh ( script: 'cat ./current_snapshot', returnStdout: true ).trim() }
                script { echo "Post-ML: ${snapshot_id}" }
                sh("git config --global http.sslVerify false")
                git(
                    url: 'https://github.com/procter-gamble/davinci-wip.git',
                    credentialsId: 'github-CiCdBot-png-PAT',
                    branch: "${params.branch_name}"
                )
                withCredentials([string(credentialsId: "${blob_shared}", variable: 'BLOB_KEY'), string(credentialsId: "${sas_blob_shared}", variable: 'SAS_KEY')]) {
                    sh("sudo azcopy copy  '${storage_account}/${params.fpi_blob_source_container_name}/future_predictions/${params.location}/${snapshot_id}/all${SAS_KEY}'   'data/lake/predictions/' --recursive=true")

                    sh("sudo mkdir -p \"data/lake/validation_logs/\"")
                    sh("sudo chown --recursive $USER data/lake/")
                    sh("sudo find data/lake/ -type f -exec chmod 644 {} \\;")
                    sh("sudo ls -hal data/lake")
                    sh("sudo ls -hal data/lake/predictions")

                    script {
                        try {
                            sh(". /opt/miniconda3/etc/profile.d/conda.sh && ./env_update.sh && conda activate davinci && conda env list && python ./dav.py validation post-ml -p \"data/lake/predictions/\" -l ${params.logs_path} ")
                        } finally {
                            sh("sudo azcopy copy  '${params.logs_path}' '${storage_account}/${params.fpi_blob_source_container_name}/validation_logs/${snapshot_id}/${params.location}/post-ml${SAS_KEY}'   --recursive=true")
                        }
                    }
                }
            }
        }

        stage ('DA Generation') {
            steps {
                sh ( "rm -rf \$(ls -I \"current_snapshot\")")
                script { snapshot_id = sh ( script: 'cat ./current_snapshot', returnStdout: true ).trim() }
                script { echo "DA Generation: ${snapshot_id}" }
                sh("git config --global http.sslVerify false")
                git(
                    url: 'https://github.com/procter-gamble/davinci-wip.git',
                    credentialsId: 'github-CiCdBot-png-PAT',
                    branch: "${params.branch_name}"
                )
                withCredentials([string(credentialsId: "${blob_shared}", variable: 'BLOB_KEY'), string(credentialsId: "${sas_blob_shared}", variable: 'SAS_KEY')]) {
                    // Download original dataset
                    sh("mkdir -p 'data/input/DaVinciProjectOneDrive/'")
                    sh("sudo chown --recursive $USER data/input/")

                    sh("sudo azcopy copy  '${storage_account}/${params.fpi_blob_source_container_name}/${params.fpi_datasets_folder}/${snapshot_id}${SAS_KEY}'  'data/input/DaVinciProjectOneDrive/' --recursive=true")
                    sh("sudo azcopy copy  '${storage_account}/${params.blob_source_container_name}/common/gtin_fpc_customer_mapping.xlsx${SAS_KEY}'  'data/input/DaVinciProjectOneDrive/${snapshot_id}/fpc_mapping/ru/gtin-fpc customer mapping.xlsx' ")

                    sh("sudo chown --recursive $USER data/input/DaVinciProjectOneDrive/${snapshot_id}")
                    sh("sudo find data/input/DaVinciProjectOneDrive/${snapshot_id} -type f -exec chmod 644 {} \\;")

                    // Download ETL results
                    sh("sudo azcopy copy  '${storage_account}/${params.fpi_blob_source_container_name}/snapshots/${params.location}/${params.etl_version}/etl_output__${params.etl_version}__${params.location}__${snapshot_id}.parquet${SAS_KEY}'   'data/lake/snapshots/${params.location}/${params.etl_version}/etl_output__${params.etl_version}__${params.location}__${snapshot_id}.parquet' --recursive=true")

                    // Download ML results
                    sh("sudo azcopy copy  '${storage_account}/${params.fpi_blob_source_container_name}/future_predictions/${params.location}/${snapshot_id}/all${SAS_KEY}'  'data/lake/ml_results/' --recursive=true")

                    // Download DA results.
                    sh("sudo azcopy copy  '${storage_account}/${target_folder}/da_results/${SAS_KEY}'  'data/lake/' --recursive=true")

                    sh("sudo chown --recursive $USER data/lake/")
                    sh("sudo find data/lake/ -type f -exec chmod 644 {} \\;")

                    sh(". /opt/miniconda3/etc/profile.d/conda.sh && conda activate davinci && conda env list && python dav.py da-gen -c ${params.location} --dataset-id ${snapshot_id} --etl-version ${params.etl_version} --etl-local \"data/lake/snapshots/${params.location}/${params.etl_version}/etl_output__${params.etl_version}__${params.location}__${snapshot_id}.parquet\" --ml-results-path \"data/lake/ml_results/\" --da-local \"data/lake/da_results\" ${params.da_gen_params} ")
                    sh("sudo azcopy copy 'data/lake/da_results/${snapshot_id}/${params.location}/*'   '${storage_account}/${params.fpi_blob_source_container_name}/da_results/${snapshot_id}/${params.location}${SAS_KEY}'  --recursive=true")
                }
            }
        }
    }
	
    post {

        failure {
            echo 'Pipeline run has a failed'
            script {
                if (envir == 'PROD') {

                    withCredentials([
                        string(credentialsId: 'SNOW_USERNAME', variable: 'SNOW_USERNAME'),
                        string(credentialsId: 'SNOW_PASSWORD', variable: 'SNOW_PASSWORD')
                    ]) {
                        script {
                            sh("python3 /home/davinci.im.1/snow.py -t '${issue_title}' -d '${issue_descr}' > out.txt && cat out.txt")
                        }
                    }
                }
            }
        }

        always {
            echo 'One way or another, pipeline finished'
            emailext attachLog: true, compressLog: true, body: "ENV: Test Fallback\nPipeline ${env.JOB_NAME} execution result: ${currentBuild.currentResult}\nJOB_NAME: ${env.JOB_NAME}\nBUILD_NUMBER: ${env.BUILD_NUMBER}\nTime: ${run_id_for_email}\nMore info (BUILD_URL): ${env.BUILD_URL}\n\nWith parameters:\nbranch_name: ${params.branch_name}\nlocation: ${params.location}\netl_version: ${params.etl_version}\nml_step_args: ${params.ml_step_args}\nsnapshot_id: ${snapshot_id}", subject: "[Fallback] ${run_id} ${env.JOB_NAME} ${currentBuild.currentResult}", to: 'Vladimir_Bolotin@epam.com, cc:SharedPGUS-DAVNotifications@epam.com'
        }
    }
}
