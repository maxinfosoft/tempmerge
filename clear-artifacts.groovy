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
def snapshot_id = "${params.snapshot_id}"

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

pipeline {
    agent {
        node {
            label 'slave'
        }
    }
    // agent any
/*
    triggers {
        cron('TZ=Europe/Minsk\nH 11 * * *')
    }

    parameters {
        string(name: 'env', defaultValue: 'DEV', description: '')
        string(name: 'branch_name', defaultValue: 'dev', description: '')
        string(name: 'location', defaultValue: 'UK', description: '')
        string(name: 'waiting_timeout', defaultValue: '4320', description: 'Timeout (in minutes) for waiting dataset uploading on blob storage.')
        string(name: 'waiting_interval', defaultValue: '5', description: 'Interval (in minutes) between checking datasets in blob storage.')
        string(name: 'etl_params', defaultValue: '--mild-deoverlap', description: 'Parameters for ETL step.')
        string(name: 'etl_version', defaultValue: 'v9', description: '')
        string(name: 'ml_directory', defaultValue: 'prod_pipeline', description: '')
        string(name: 'blob_source_container_name', defaultValue: 'davinci-dry-run', description: 'Container name where source dataset is uploaded.')
        string(name: 'datasets_folder', defaultValue: 'davinci-upload', description: 'Folder name where source dataset is uploaded.')
        string(name: 'snapshot_id', defaultValue: '20200331', description: '')
    }
*/
    stages {

        stage('Clear Artifacts') {
            steps {
                sh("sudo rm -rf ./* && rm -rf ./.git*")

                withCredentials([string(credentialsId: "${blob_shared}", variable: 'BLOB_KEY'), string(credentialsId: "${sas_blob_shared}", variable: 'SAS_KEY')]) {

                    script {
                        try {
                            // Remove .davinci_dataset_metadata.json
                            sh("sudo azcopy rm  '${storage_account}/${params.blob_source_container_name}/${params.datasets_folder}/${snapshot_id}/.davinci_dataset_metadata.json${SAS_KEY}' ")
                        }
                        catch (error) {
                            echo "json file not found (previously deleted?)"
                        }
                        finally {
                            sh("sudo azcopy rm  '${storage_account}/${target_folder}/da_results/${snapshot_id}${SAS_KEY}' --recursive=true")
                        }
                    }
                }
            }
        }
    }


    post {

        failure {
            echo 'Pipeline run has a failed'
        }

        success{
            script {
                checkpoint_job_url_params = get_URL_encoded_params([
                        ['branch_name', params.branch_name],
                        ['etl_version', params.etl_version],
                        ['snapshot_id', snapshot_id]
                ])
            }
        }

        always {
            echo 'One way or another, pipeline finished'
            emailext attachLog: true, compressLog: true, body: "ENV: ${envir}\nPipeline ${env.JOB_NAME} execution result: ${currentBuild.currentResult}\nJOB_NAME: ${env.JOB_NAME}\nBUILD_NUMBER: ${env.BUILD_NUMBER}\nTime: ${run_id_for_email}\nMore info (BUILD_URL): ${env.BUILD_URL}\n\nWith parameters:\nbranch_name: ${params.branch_name}\nlocation: ${params.location}\nwaiting_timeout: ${params.waiting_timeout}\nwaiting_interval: ${params.waiting_interval}\nml_step_args: ${params.ml_step_args}\ndatasets_folder: ${params.datasets_folder}\netl_params: ${params.etl_params}\netl_version: ${params.etl_version}\nml_directory: ${params.ml_directory}\nblob_account_name: ${blob_account_name}\nblob_source_container_name: ${params.blob_source_container_name}", subject: "[${envir}] ${run_id_for_email} ${env.JOB_NAME} ${currentBuild.currentResult}\nsnapshot_id: ${snapshot_id}", to: 'SpecialPGUS-DAVCoreTeam@epam.com, cc:SharedPGUS-DAVNotifications@epam.com'
        }
    }
}
