@Library('flexy') _

// rename build
def userId = currentBuild.rawBuild.getCause(hudson.model.Cause$UserIdCause)?.userId
if (userId) {
  currentBuild.displayName = userId
}

def RETURNSTATUS = "default"

pipeline {
  agent none

  parameters {
        string(name: 'BUILD_NUMBER', defaultValue: '', description: 'Build number of job that has installed the cluster.')
        string(name: 'SCALE_UP', defaultValue: '0', description: 'If value is set to anything greater than 0, cluster will be scaled up before executing the workload.')
        string(name: 'SCALE_DOWN', defaultValue: '0', description:
        '''If value is set to anything greater than 0, cluster will be scaled down after the execution of the workload is complete,<br>
        if the build fails, scale down may not happen, user should review and decide if cluster is ready for scale down or re-run the job on same cluster.'''
        )
        string(name:'JENKINS_AGENT_LABEL',defaultValue:'oc45',description:
        '''
        scale-ci-static: for static agent that is specific to scale-ci, useful when the jenkins dynamic agent isn't stable<br>
        4.y: oc4y || mac-installer || rhel8-installer-4y <br/>
            e.g, for 4.8, use oc48 || mac-installer || rhel8-installer-48 <br/>
        3.11: ansible-2.6 <br/>
        3.9~3.10: ansible-2.4 <br/>
        3.4~3.7: ansible-2.4-extra || ansible-2.3 <br/>
        '''
        )
        string(name: 'JOB_ITERATIONS', defaultValue: '1000', description: 'This variable configures the number of cluster-density jobs iterations to perform (1 namespace per iteration). By default 1000.')
        booleanParam(name: 'WRITE_TO_FILE', defaultValue: false, description: 'Value to write to google sheet (will run https://mastern-jenkins-csb-openshift-qe.apps.ocp-c1.prod.psi.redhat.com/job/scale-ci/job/e2e-benchmarking-multibranch-pipeline/job/write-scale-ci-results)')
        text(name: 'ENV_VARS', defaultValue: '', description:'''<p>
               Enter list of additional (optional) Env Vars you'd want to pass to the script, one pair on each line. <br>
               e.g.<br>
               SOMEVAR1='env-test'<br>
               SOMEVAR2='env2-test'<br>
               ...<br>
               SOMEVARn='envn-test'<br>
               </p>'''
            )
        string(name: 'E2E_BENCHMARKING_REPO', defaultValue:'https://github.com/cloud-bulldozer/e2e-benchmarking', description:'You can change this to point to your fork if needed.')
        string(name: 'E2E_BENCHMARKING_REPO_BRANCH', defaultValue:'master', description:'You can change this to point to a branch on your fork if needed.')
    }

  stages {
    stage('Run Cluster-Density'){
      agent { label params['JENKINS_AGENT_LABEL'] }
      steps{
        script{
          if(params.SCALE_UP.toInteger() > 0) {
            build job: 'scale-ci/e2e-benchmarking-multibranch-pipeline/cluster-workers-scaling/', parameters: [string(name: 'BUILD_NUMBER', value: BUILD_NUMBER), text(name: "ENV_VARS", value: ENV_VARS), string(name: 'WORKER_COUNT', value: SCALE_UP), string(name: 'JENKINS_AGENT_LABEL', value: JENKINS_AGENT_LABEL)]
          }
        }
        deleteDir()
        checkout([
          $class: 'GitSCM',
          branches: [[name: params.E2E_BENCHMARKING_REPO_BRANCH ]],
          doGenerateSubmoduleConfigurations: false,
          userRemoteConfigs: [[url: params.E2E_BENCHMARKING_REPO ]
          ]])

        copyArtifacts(
            filter: '',
            fingerprintArtifacts: true,
            projectName: 'ocp-common/Flexy-install',
            selector: specific(params.BUILD_NUMBER),
            target: 'flexy-artifacts'
        )
        script {
          buildinfo = readYaml file: "flexy-artifacts/BUILDINFO.yml"
          currentBuild.displayName = "${currentBuild.displayName}-${params.BUILD_NUMBER}"
          currentBuild.description = "Copying Artifact from Flexy-install build <a href=\"${buildinfo.buildUrl}\">Flexy-install#${params.BUILD_NUMBER}</a>"
          buildinfo.params.each { env.setProperty(it.key, it.value) }
        }
        script {
          RETURNSTATUS = sh(returnStatus: true, script: '''
          # Get ENV VARS Supplied by the user to this job and store in .env_override
          echo "$ENV_VARS" > .env_override
          # Export those env vars so they could be used by CI Job
          set -a && source .env_override && set +a
          mkdir -p ~/.kube
          cp $WORKSPACE/flexy-artifacts/workdir/install-dir/auth/kubeconfig ~/.kube/config
          oc config view
          oc projects
          ls -ls ~/.kube/
          env
          echo "$OSTYPE"

          cd workloads/kube-burner
          wget https://www.python.org/ftp/python/3.8.12/Python-3.8.12.tgz
          tar -zxvf Python-3.8.12.tgz
          cd Python-3.8.12
          newdirname=~/.localpython
          if [ -d "$newdirname" ]; then
            echo "Directory already exists"
          else
            mkdir -p $newdirname
            ./configure --prefix=$HOME/.localpython
            make
            make install
          fi
          python_folder="bin/python3"
          if [ -d "$newdirname/bin/python3" ]; then
            echo "Directory python3 "
          elif [ -d "$newdirname/bin/python3.8" ]; then
            echo "Directory python3.8 "
            python_folder="bin/python3.8"
          fi
          $HOME/.localpython/$python_folder --version
          python3 -m pip install virtualenv --user
          python3 -m virtualenv venv3 -p $HOME/.localpython/$python_folder
          source venv3/bin/activate
          python --version
          cd ..
          ./run_clusterdensity_test_fromgit.sh
          ''')
        }

        script{
            def status = "FAIL"
            if( RETURNSTATUS.toString() == "0") {
                status = "PASS"
            }else {
                currentBuild.result = "FAILURE"
            }
           if(params.WRITE_TO_FILE == true) {
            build job: 'scale-ci/e2e-benchmarking-multibranch-pipeline/write-scale-ci-results', parameters: [string(name: 'BUILD_NUMBER', value: BUILD_NUMBER), string(name: 'CI_JOB_ID', value: BUILD_ID), string(name: 'CI_JOB_URL', value: BUILD_URL), text(name: "ENV_VARS", value: ENV_VARS), string(name: 'JENKINS_AGENT_LABEL', value: JENKINS_AGENT_LABEL), string(name: "CI_STATUS", value: "${status}"), string(name: "JOB", value: "cluster-density")]
           }
        }

        script{
          // if the build fails, scale down will not happen, letting user review and decide if cluster is ready for scale down or re-run the job on same cluster
          if(params.SCALE_DOWN.toInteger() > 0) {
            build job: 'scale-ci/e2e-benchmarking-multibranch-pipeline/cluster-workers-scaling', parameters: [string(name: 'BUILD_NUMBER', value: BUILD_NUMBER), string(name: 'WORKER_COUNT', value: SCALE_DOWN), text(name: "ENV_VARS", value: ENV_VARS), string(name: 'JENKINS_AGENT_LABEL', value: JENKINS_AGENT_LABEL)]
          }

        }
       }
     }
   }
}