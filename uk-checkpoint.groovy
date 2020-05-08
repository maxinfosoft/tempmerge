import java.text.SimpleDateFormat
def date = new Date()
sdf = new SimpleDateFormat("yyyyMMddHHmmss")
run_id = sdf.format(date)
sdf_for_email = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
run_id_for_email = sdf_for_email.format(date)
println "Run id: ${run_id}"

def etl_version = "${params.etl_version}"
def run_description = "${params.location}_checkpoint_${params.snapshot_id}"
def summary_job_url_params = ""

def blob_shared = ""
def envir = "${params.env}"

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

//Specify variable parameters for automatically launched pipeline Summary in {post-success}
//def branch_name = "master"
//def location = "UK"
//def checkpoint_root = "davinci-datalake/checkpoints"
//def da_root = "davinci-upload/davinci-upload"
//def output_root = "davinci-datalake/summary"
//def summary_params = "--export"
//def snapshot_id = "20191119"

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
        string(name: 'location', defaultValue: 'UK', description: '')
        string(name: 'etl_version', defaultValue: 'v9', description: '')
        string(name: 'ml_step_args', defaultValue: '-p checkpoint', description: 'Parameter for ML step.')
        string(name: 'snapshot_id', defaultValue: '20200310', description: 'The date of snapshot production')
    }
*/	
    stages {

        stage('ML') {
            steps {
                sh("sudo rm -rf ./* && rm -rf ./.git*")
                sh("git config --global http.sslVerify false")
                git(
                    url: 'https://github.com/procter-gamble/davinci-wip.git',
                    credentialsId: 'github-CiCdBot-png-PAT',
                    branch: "${params.branch_name}"
                    )
                withCredentials([string(credentialsId: "${blob_shared}", variable: 'BLOB_KEY'), string(credentialsId: "${sas_blob_shared}", variable: 'SAS_KEY')]) {
                    sh("sudo azcopy copy '${storage_account}/davinci-datalake/snapshots/${params.location}/${params.etl_version}${SAS_KEY}' 'data/lake/snapshots/${params.location}' --recursive=true")
                    // UK (RU) mapping tables
                    sh("sudo azcopy copy '${storage_account}/davinci-upload/common/remapping_tables/*${SAS_KEY}' 'data/input' --recursive=true")
                    sh("sudo chown --recursive $USER data/")
                    sh("sudo find data/ -type f -exec chmod 644 {} \\;")
                    
                    // sh("sudo chown --recursive $USER data/lake/")
                    // sh("sudo find data/lake/ -type f -exec chmod 644 {} \\;")
                    sh(". /opt/miniconda3//etc/profile.d/conda.sh && ./env_update.sh && conda activate davinci && conda env list && python dav.py run-experiments ${run_description} -c ${params.location} --mlflow ${mlflow_url} --etl-version ${params.etl_version} ${params.ml_step_args}")
                    sh("sudo azcopy copy 'data/experiments/${run_description}/all/all/predictions/*' '${storage_account}/davinci-datalake/checkpoints/${params.location}/${params.snapshot_id}${SAS_KEY}' --recursive=true")
                }
            }
        }
    }

    post {
    
        success{
            script {
                summary_job_url_params = get_URL_encoded_params([
                    ['branch_name', params.branch_name],
                    ['location', params.location],
                    ['snapshot_id', params.snapshot_id]
                ])
            }

            echo "Summary job URL params: ${summary_job_url_params}"

            echo "snapshot_id = ${params.snapshot_id}"
            echo "branch_name = ${params.branch_name}"
            echo "location = ${params.location}"
            echo "etl_version = ${params.etl_version}"
            echo "ml_step_args = ${params.ml_step_args}"
            
            withCredentials([usernamePassword(credentialsId: "${api_token_shared_id_w_token}", usernameVariable: 'API_USER', passwordVariable: 'API_TOKEN')]) {
                script {
                    sh("curl --insecure -X POST \"${host_prod}/job/Summary/buildWithParameters?${summary_job_url_params}\" --user ${API_USER}:${API_TOKEN}")
                }
            }
        }

        always {
            echo 'One way or another, pipeline finished'
            emailext attachLog: true, compressLog: true, body: "ENV: PROD\nPipeline ${env.JOB_NAME} execution result: ${currentBuild.currentResult}\nJOB_NAME: ${env.JOB_NAME}\nBUILD_NUMBER: ${env.BUILD_NUMBER}\nTime: ${run_id_for_email}\nMore info (BUILD_URL): ${env.BUILD_URL}\n\nWith parameters:\nbranch_name: ${params.branch_name}\nlocation: ${params.location}\netl_version: ${params.etl_version}\nml_step_args: ${params.ml_step_args}\nsnapshot_id: ${snapshot_id}", subject: "[PROD] ${run_id} ${env.JOB_NAME} ${currentBuild.currentResult}", to: 'SpecialPGUS-DAVCoreTeam@epam.com, cc:SharedPGUS-DAVNotifications@epam.com'
        }
    }
}
