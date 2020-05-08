import java.text.SimpleDateFormat
def date = new Date()
sdf = new SimpleDateFormat("yyyyMMddHHmmss")
run_id = sdf.format(date)
sdf_for_email = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
run_id_for_email = sdf_for_email.format(date)
println "Run id: ${run_id}"

def local_summary_root = "data/summary_data"

def cpt_file = ""
def cpt_path = ""
def da_file = "${params.location}_${params.snapshot_id}_da_archive.parquet"
def da_path = "${local_summary_root}/da/${da_file}"
def output_root_path = "${local_summary_root}/output"
def output_path = "${output_root_path}/${params.location}_${params.snapshot_id}_summary.ipynb"

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
        string(name: 'location', defaultValue: 'RU', description: '--c, --country')
        string(name: 'checkpoint_root', defaultValue: 'davinci-datalake/checkpoints', description: 'Root container for checkpoints in blob')
        string(name: 'da_root', defaultValue: 'davinci-upload/davinci-upload', description: 'Root container for DA archives in blob')
        string(name: 'output_root', defaultValue: 'davinci-datalake/summary', description: 'Root container for summary output in blob')
        string(name: 'summary_params', defaultValue: '--export', description: 'Additional parameters for summary command')
        string(name: 'snapshot_id', defaultValue: '20200310', description: 'The date of snapshot production')
    }
*/	
    stages {
        
        stage('Summary') {
            steps {
                script {
                    params.each {param ->
                        println "${param.key} = ${param.value} "
                    }
                }

                sh("sudo rm -rf ./* && rm -rf ./.git*")
                sh("git config --global http.sslVerify false")
                git(
                    url: 'https://github.com/procter-gamble/davinci-wip.git',
                    credentialsId: 'github-CiCdBot-png-PAT',
                    branch: "${params.branch_name}"
                    // branch: "master"
                    )
                withCredentials([string(credentialsId: "${blob_shared}", variable: 'BLOB_KEY'), string(credentialsId: "${sas_blob_shared}", variable: 'SAS_KEY')]) {
                    sh("sudo azcopy copy '${storage_account}/${params.checkpoint_root}/${params.location}/${params.snapshot_id}/*${SAS_KEY}' '${local_summary_root}/cpt' --recursive=true")
                    sh("sudo azcopy copy '${storage_account}/${params.da_root}/${params.snapshot_id}/manual_da/${params.location.toLowerCase()}/${da_file}${SAS_KEY}' '${da_path}\' --recursive=true")
                    sh("sudo chown --recursive $USER ${local_summary_root}")
                    sh("sudo find ${local_summary_root} -type f -exec chmod 644 {} \\;")
                    script { 
                        //cpt_file = sh ( script: "ls -1 ./${local_summary_root}/cpt/*.pkl | head -n 1", returnStdout: true ).trim()
                        cpt_file = sh ( script: "find ./${local_summary_root}/cpt/*.pkl | xargs -n 1 basename", returnStdout: true ).trim()
                        cpt_path = "${local_summary_root}/cpt/${cpt_file}"
                    }
                    sh(". /opt/miniconda3/etc/profile.d/conda.sh && ./env_update.sh && conda activate davinci && conda env list && python dav.py summary -c ${params.location} -cpt ${cpt_path} -da ${da_path} -o ${output_path} ${params.summary_params}")
                    sh("sudo azcopy copy '${output_root_path}\' '${storage_account}/${params.output_root}/${params.location}/${params.snapshot_id}${SAS_KEY}' --recursive=true")
                }
            }
        }
    }

    post {
        always {
            echo 'One way or another, pipeline finished'
            emailext attachLog: true, compressLog: true, body: "ENV: PROD\nPipeline ${env.JOB_NAME} execution result: ${currentBuild.currentResult}\nJOB_NAME: ${env.JOB_NAME}\nBUILD_NUMBER: ${env.BUILD_NUMBER}\nTime: ${run_id_for_email}\nMore info (BUILD_URL): ${env.BUILD_URL}\n\nWith parameters:\nbranch_name: ${params.branch_name}\nlocation: ${params.location}\ncheckpoint_root: ${params.checkpoint_root}\nda_root: ${params.da_root}\noutput_root: ${params.output_root}\nsummary_params: ${params.summary_params}\nsnapshot_id: ${params.snapshot_id}", subject: "[PROD] ${run_id_for_email} ${env.JOB_NAME} ${currentBuild.currentResult}", to: 'SpecialPGUS-DAVCoreTeam@epam.com, cc:SharedPGUS-DAVNotifications@epam.com'
        }
    }
}
