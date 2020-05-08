import java.text.SimpleDateFormat
def date = new Date()
sdf = new SimpleDateFormat("yyyyMMddHHmmss")
run_id = sdf.format(date)
sdf_for_email = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
run_id_for_email = sdf_for_email.format(date)
println "Run id: ${run_id}"

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
        string(name: 'snapshot_id', defaultValue: '', description: 'The date of snapshot production')
        string(name: 'da_gen_params', defaultValue: '', description: '')
        string(name: 'blob_source_container_name', defaultValue: 'davinci-upload', description: 'Container name where source dataset is uploaded.')
        string(name: 'datasets_folder', defaultValue: 'davinci-upload', description: 'Folder name where source dataset is uploaded.')
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
                withCredentials([string(credentialsId: 'prod_blob_shared_id', variable: 'BLOB_KEY'),  string(credentialsId: "${sas_blob_shared}", variable: 'SAS_KEY')]) {
                    // Download DA results.
                    sh("sudo azcopy  copy \"${storage_account}/${target_folder}/da_results/*${SAS_KEY}\"  \"data/lake/da_results/\" --recursive=true")
                    sh(". /opt/miniconda3/etc/profile.d/conda.sh && conda env list && conda create --name davinci python=3.6.6 && conda activate davinci && conda env list && conda env update -f environment.yml && conda activate davinci && conda env list")
                    sh("sudo chown --recursive $USER data/lake/")
                    sh("sudo find data/lake/ -type f -exec chmod 644 {} \\;")
                    withCredentials([usernamePassword(credentialsId: "${shared_id_user_w_psw}", usernameVariable: 'API_USER', passwordVariable: 'API_PASS')]) {
                                sh(". /opt/miniconda3/etc/profile.d/conda.sh && conda activate davinci && conda env list && python dav.py  sharepoint-upload --folder_path data/lake/da_results --pea_forecast_folder_path 'data/lake/predictions' --snapshot_id ${snapshot_id}  --pea_forecast_file_name 'PEA_vs_DaVinci_Forecast_per_retailer_promo_barcode.xlsx' --location ${params.location} --user ${API_USER} --password ${API_PASS}")
                    }
                }
            }
        }
    }
	
    post {

        always {
            echo 'One way or another, pipeline finished'
            emailext attachLog: true, compressLog: true, body: "ENV: SharePoint\nPipeline ${env.JOB_NAME} execution result: ${currentBuild.currentResult}\nJOB_NAME: ${env.JOB_NAME}\nBUILD_NUMBER: ${env.BUILD_NUMBER}\nTime: ${run_id_for_email}\nMore info (BUILD_URL): ${env.BUILD_URL}\n\nWith parameters:\nbranch_name: ${params.branch_name}\nlocation: ${params.location}\nwaiting_timeout: ${params.waiting_timeout}\nwaiting_interval: ${params.waiting_interval}\nml_step_args: ${params.ml_step_args}\ndatasets_folder: ${params.datasets_folder}\netl_params: ${params.etl_params}\netl_version: ${params.etl_version}\nml_directory: ${params.ml_directory}\nblob_account_name: ${blob_account_name}\nblob_source_container_name: ${params.blob_source_container_name}", subject: "[PROD] ${run_id_for_email} ${env.JOB_NAME} ${currentBuild.currentResult}\nsnapshot_id: ${snapshot_id}", to: 'SpecialPGUS-DAVCoreTeam@epam.com, cc:SharedPGUS-DAVNotifications@epam.com'
        }
    }
}
