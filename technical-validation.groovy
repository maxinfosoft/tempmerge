/*
DEV
    master (azl-davinci016.np-cloud-pg.com, 137.182.194.215)
    slave1 (azl-davinci15-1.np-cloud-pg.com, 137.182.193.5)
    slave2 (azl-davinci04.np-cloud-pg.com, 137.182.194.42)
    slave3 (azl-davinci14.np-cloud-pg.com, 137.182.194.200)
    slave4 (azl-davinci02.np-cloud-pg.com, 137.182.193.29)
    slave5 (azl-davinci11.np-cloud-pg.com, 137.182.194.38)
    slave6 (azl-davinci05.np-cloud-pg.com, 137.182.195.59)

PROD
    master (azl-davincipr01.cloud-pg.com, 137.182.163.4)
    slave1 (azl-davincipr02.cloud-pg.com, 137.182.162.255)
    slave2 (azl-davincipr03.cloud-pg.com, 137.182.163.5)
    slave3 (azl-davincipr04.cloud-pg.com, 137.182.162.161)
*/
               
import java.text.SimpleDateFormat
def date = new Date()
sdf = new SimpleDateFormat("yyyyMMddHHmmss")
run_id = sdf.format(date)
sdf_for_email = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
run_id_for_email = sdf_for_email.format(date)
println "Run id: ${run_id}"

pipeline {
    
    agent { node { label "${params.node_run}" } } 
    //agent { node { label "$node" } } 

    stages {
        
        stage ('Github verification'){
            steps {
                
                echo "${params.node_run}"
                
                sh("git --version") 
		//echo "hello world!"
                //sh("git config --global core.compression 9")
                
                sh("sudo rm -rf ./* && rm -rf ./.git*")
                
                git(
                    //url: 'git@github.com:procter-gamble/davinci-wip.git',
                    url: 'https://github.com/procter-gamble/davinci-wip.git',
                    credentialsId: 'github-CiCdBot-png-PAT',
                    //credentialsId: 'maxinfosoft_github_pat',
                    //credentialsId: 'github_maxinfosoft',
                    branch: "${params.branch_name}"
                )
            }
        }    
       
        stage ('Azcopy & storage verification'){
            steps {

                sh("sudo rm test.txt") 
                sh("echo \"Davinci P&G - temporary file for technical-validation pipeline\" > test.txt") 
                
                withCredentials([
                    string(credentialsId: "sas_dev_blob_shared_id", variable: 'SAS_DEV_KEY'),
                    string(credentialsId: "sas_prod_blob_shared_id", variable: 'SAS_PROD_KEY')
                ]) {
                    //check storage performance
                    sh("sudo azcopy bench 'https://davincidev01.blob.core.windows.net/davinci-upload${SAS_DEV_KEY}' ")
                    sh("sudo azcopy bench 'https://davincistorage01.blob.core.windows.net/davinci-upload${SAS_PROD_KEY}' ")
                    
                    //copy to dev.env: https://davincidev01.blob.core.windows.net/davinci-upload/test/test.txt
                    sh("sudo azcopy copy 'test.txt'   'https://davincidev01.blob.core.windows.net/davinci-upload/test/${SAS_DEV_KEY}' ")
                    //copy to prod.env: https://davincistorage01.blob.core.windows.net/davinci-upload/test/test.txt
                    sh("sudo azcopy copy 'test.txt'   'https://davincistorage01.blob.core.windows.net/davinci-upload/test/${SAS_PROD_KEY}' ")
                }
            }
        }
        
        stage ('Conda verification'){
            steps {
                sh(". /opt/miniconda3/etc/profile.d/conda.sh && conda env list && conda create --name davinci python=3.6.6 && conda activate davinci && conda env list && conda env update -f environment.yml && conda activate davinci && conda env list")
            }      
        }
       
        stage('Verification of services') { 
            steps {
                sh("hostname")
                sh("whoami")
				
                sh("java -version")
                sh("python -V")
                sh("sudo /opt/miniconda3/bin/conda --version")
                echo sh(script: 'env|sort', returnStdout: true)
               
                script {
                    //check azcopy
                    try {
                       sh("sudo -n /usr/bin/azcopy --version")
                    }
                    catch (error) {
                       echo "Incorrect status old version azcopy 7.3 with sudo - see real status and azcopy version above."
                    }
                    finally {
                       //echo "There is other method of calling or azcopy not found on this instance."
                       //sh("azcopy --version")
                    }
                    
                    //check mlflow
                    script { 
                        status_mlflow_dev = sh ( script: "curl -LI http://azl-davinci016.np-cloud-pg.com:5000 -o /dev/null -w '%{http_code}\n' -s", returnStdout: true ).trim()
                        status_mlflow_prod = sh ( script: "curl -LI http://azl-davincipr01.cloud-pg.com:5000 -o /dev/null -w '%{http_code}\n' -s", returnStdout: true ).trim()
                    
                        echo "MLFlow azl-davinci016 (DEV) status: ${status_mlflow_dev}"
                        echo "MLFlow azl-davincipr01 (PROD) status: ${status_mlflow_prod}"
                    
                        if ( status_mlflow_dev != "200") {
                            echo "!!! ERROR !!! http://azl-davinci016.np-cloud-pg.com:5000 NOT WORK"
                            sh("curl -Is http://azl-davinci016.np-cloud-pg.com:5000 | head -n 1")
                        }

                        if ( status_mlflow_prod != "200") {
                            echo "!!! ERROR !!! http://azl-davincipr01.cloud-pg.com:5000 NOT WORK"
                            sh("curl -Is http://azl-davincipr01.cloud-pg.com:5000 | head -n 1")
                        }
                    }
			
                    try {
                       sh("sudo -n mlflow --version")
                    }
                    catch (error) {
                       echo "Most likely mlflow is not on this instance - see real status and mlflow version above."
                    }
                    finally {
                       //echo "There is other method of calling mlflow or not found on this instance."
                       //sh("mlflow --version")
                    }
                }
            }
        }
    }
     
    post {
        always {
            echo 'One way or another, pipeline finished'
            emailext attachLog: true, compressLog: true, body: "Pipeline ${env.JOB_NAME} execution result: ${currentBuild.currentResult}\nJOB_NAME: ${env.JOB_NAME}\nBUILD_NUMBER: ${env.BUILD_NUMBER}\nTime: ${run_id_for_email}\nMore info (BUILD_URL): ${env.BUILD_URL}\n\nWith parameters:\nbranch_name: ${params.branch_name}", subject: "[technical-validation] ${run_id} ${env.JOB_NAME} ${currentBuild.currentResult}", to: 'SharedPGUS-DAVNotifications@epam.com'
        }
    }
}
