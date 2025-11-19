properties([disableConcurrentBuilds(), buildDiscarder(logRotator(artifactDaysToKeepStr: '5', artifactNumToKeepStr: '5', daysToKeepStr: '5', numToKeepStr: '5'))])

@Library('pipeline-library')
import dk.stiil.pipeline.Constants

def template = '''
    apiVersion: v1
    kind: Pod
    spec:
      containers:
      - name: buildkit
        image: moby/buildkit:v0.26.1-rootless # renovate
        command:
        - /bin/sh
        tty: true
        volumeMounts:
        - name: docker-secret
          mountPath: /home/user/.docker
        - name: certs
          mountPath: /certs/client
      restartPolicy: Never
      volumes:
      - name: docker-secret
        secret:
          secretName: github-dockercred
          items:
          - key: .dockerconfigjson
            path: config.json
      - name: certs
        secret:
          secretName: buildkit-client-certs
'''

podTemplate(yaml: template) {
  node(POD_LABEL) {
    TreeMap scmData
    String gitCommitMessage
    Map properties
    stage('checkout SCM') {  
      scmData = checkout scm
      gitCommitMessage = sh(returnStdout: true, script: "git log --format=%B -n 1 ${scmData.GIT_COMMIT}").trim()
//      gitMap = scmGetOrgRepo scmData.GIT_URL
//      githubWebhookManager gitMap: gitMap, webhookTokenId: 'jenkins-webhook-repo-cleanup'
      properties = readProperties file: 'package.env'
    }
    String name
    if (isMainBranch()){
      pushName = "\"name=${properties.PACKAGE_DESTINATION}/${properties.PACKAGE_NAME}:$BRANCH_NAME,${properties.PACKAGE_DESTINATION}/${properties.PACKAGE_NAME}:latest\""
    } else {
      pushName = "name=${properties.PACKAGE_DESTINATION}/${properties.PACKAGE_NAME}:$BRANCH_NAME"
    }
    if ( !gitCommitMessage.startsWith("renovate/") || ! gitCommitMessage.startsWith("WIP") ) {
      container('buildkit') {
        stage('Build Docker Image') {
          withEnv(["GIT_COMMIT=${scmData.GIT_COMMIT}", "PUSH_NAME=${pushName}", "PACKAGE_NAME=${properties.PACKAGE_NAME}", "PACKAGE_CONTAINER_PLATFORMS=${properties.PACKAGE_CONTAINER_PLATFORMS}", "PACKAGE_DESTINATION=${properties.PACKAGE_DESTINATION}", "PACKAGE_CONTAINER_SOURCE=${properties.PACKAGE_CONTAINER_SOURCE}", "GIT_BRANCH=${BRANCH_NAME}"]) {
            sh '''
              buildctl --addr 'tcp://buildkitd:1234'\
              --tlscacert /certs/client/ca.crt \
              --tlscert /certs/client/tls.crt \
              --tlskey /certs/client/tls.key \
              build \
              --frontend dockerfile.v0 \
              --opt filename=Dockerfile --opt platform=$PACKAGE_CONTAINER_PLATFORMS \
              --local context=$(pwd) --local dockerfile=$(pwd) \
              --import-cache $PACKAGE_DESTINATION/$PACKAGE_NAME:buildcache \
              --export-cache $PACKAGE_DESTINATION/$PACKAGE_NAME:buildcache \
              --output type=image,$PUSH_NAME,push=true,annotation.org.opencontainers.image.description="Build based on $PACKAGE_CONTAINER_SOURCE/commit/$GIT_COMMIT",annotation.org.opencontainers.image.revision=$GIT_COMMIT,annotation.org.opencontainers.image.version=$GIT_BRANCH,annotation.org.opencontainers.image.source=$PACKAGE_CONTAINER_SOURCE,annotation.org.opencontainers.image.licenses=GPL-2.0-only
              '''
          }
        }
      }
      if (env.CHANGE_ID) {
        if (pullRequest.createdBy.equals("renovate[bot]")){
          if (pullRequest.mergeable) {
            stage('Approve and Merge PR') {
              pullRequest.merge(commitTitle: pullRequest.title, commitMessage: pullRequest.body, mergeMethod: 'squash')
            }
          }
        } else {
          echo "'PR Created by \""+ pullRequest.createdBy + "\""
        }
      }
    }
  }
}
