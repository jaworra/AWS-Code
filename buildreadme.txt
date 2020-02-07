The process is a lot simpler than it looks, I have included all options for clarity. To summarise:
1.	Create a CodeCommit git repository and upload the CloudFormation template that creates the S3 Bucket (supplied).
2.	Create a CodeBuild project that deploys the CloudFormation template.
3.	Create a CodePipeline which orchestrates the deployment.

The steps are as follows:
1.	Download the attached CloudFormation (CFN) template, cfn_template.yaml
2.	Log into the AWS Console
3.	In the AWS Console, go to the CodeCommit service and create a new repository in the Sydney region. 
a.	Create repository:
i.	Repository name: my_pipeline_repo
ii.	Click create.
b.	my_pipeline_repo page:
i.	Select Add file / upload file
ii.	select cfn_template.yaml
iii.	enter author name, email address and commit message.
iv.	Click: Commit changes
4.	In the AWS Console, go to the CodeBuild service and under “Build projects” click “Create build project” 
a.	Create build project:
i.	Project name: my_pipeline_build
ii.	Source: Provider: AWS Code Commit
iii.	Repository: my_pipeline_repo
iv.	Environment:
1.	Managed Image
2.	Ubuntu
3.	Python (this isn’t important for our demo)
4.	Runtime 3.6.5
5.	Version: latest
6.	new service role
v.	Buildspec:
1.	Insert build commands
2.	Switch to editor
3.	Paste in the contents of buildspec.txt
b.	Leave all other settings as their default, and click Create build project
5.	In the AWS Console, go to the CodePipeline service and click “Create pipeline” 
a.	Pipeline settings:
i.	Pipeline name: my_pipeline
ii.	new service role


iii.	allow AWS CodePipeline to create service role
iv.	default artifact store location
v.	Click next.
b.	Source settings:
i.	Source provider: CodeCommit,
ii.	Repository: my_pipeline_repo
iii.	branch: master


iv.	Detection options: Amazon CloudWatch Events (this means the pipeline will auto-run after every commit).
c.	Build:
i.	Build Provider: AWS CodeBuild
ii.	Region: Sydney
iii.	Project Name: my_pipeline_build
d.	Deploy Provider: Cloud Formation
i.	Region: Sydney
ii.	Action: Create or Update Stack
iii.	Stack Name: my-pipeline-stack (underscores are not allowed)
iv.	Template: BuildArtifact::cfn_template.yaml
v.	Template Configuration: <none>
vi.	Capabilities: <none>
vii.	Role name: AWSCloudFormationStackSetAdministratorRole
viii.	Click Create pipeline
