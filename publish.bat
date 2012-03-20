cd ""C:\Documents and Settings\ChambeJX.RISK\My Documents\NetBeansProjects\Pentaho"
@echo off

set varDir=ECLDatasetJob
set varJar=ECLDatasetJob

copy /Y "Jobs\%varDir%\dist\%varJar%.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\%varJar%.jar"
copy /Y "ECLDirect\dist\ECLDirect.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLDirect.jar"
copy /Y "ECLGUIFeatures\dist\ECLGUIFeatures.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLGUIFeatures.jar"


set varDir=ECLDedupJob
set varJar=ECLDedupJob

copy /Y "Jobs\%varDir%\dist\%varJar%.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\%varJar%.jar"
copy /Y "ECLDirect\dist\ECLDirect.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLDirect.jar"
copy /Y "ECLGUIFeatures\dist\ECLGUIFeatures.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLGUIFeatures.jar"

set varDir=ECLDistributeJob
set varJar=ECLDistributeJob

copy /Y "Jobs\%varDir%\dist\%varJar%.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\%varJar%.jar"
copy /Y "ECLDirect\dist\ECLDirect.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLDirect.jar"
copy /Y "ECLGUIFeatures\dist\ECLGUIFeatures.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLGUIFeatures.jar"


set varDir=ECLGlobalVariablesJob
set varJar=ECLGlobalVariables

copy /Y "Jobs\%varDir%\dist\%varJar%.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\%varJar%.jar"
copy /Y "ECLDirect\dist\ECLDirect.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLDirect.jar"
copy /Y "ECLGUIFeatures\dist\ECLGUIFeatures.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLGUIFeatures.jar"

set varDir=ECLIterateJob
set varJar=%varDir%

copy /Y "Jobs\%varDir%\dist\%varJar%.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\%varJar%.jar"
copy /Y "ECLDirect\dist\ECLDirect.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLDirect.jar"
copy /Y "ECLGUIFeatures\dist\ECLGUIFeatures.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLGUIFeatures.jar"

set varDir=ECLJoinJob
set varJar=%varDir%

copy /Y "Jobs\%varDir%\dist\%varJar%.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\%varJar%.jar"
copy /Y "ECLDirect\dist\ECLDirect.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLDirect.jar"
copy /Y "ECLGUIFeatures\dist\ECLGUIFeatures.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLGUIFeatures.jar"

set varDir=ECLMergePaths
set varJar=%varDir%

copy /Y "Jobs\%varDir%\dist\%varJar%.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\%varJar%.jar"
copy /Y "ECLDirect\dist\ECLDirect.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLDirect.jar"
copy /Y "ECLGUIFeatures\dist\ECLGUIFeatures.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLGUIFeatures.jar"

set varDir=ECLOutputJob
set varJar=%varDir%

copy /Y "Jobs\%varDir%\dist\%varJar%.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\%varJar%.jar"
copy /Y "ECLDirect\dist\ECLDirect.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLDirect.jar"
copy /Y "ECLGUIFeatures\dist\ECLGUIFeatures.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLGUIFeatures.jar"

set varDir=ECLProjectJob
set varJar=%varDir%

copy /Y "Jobs\%varDir%\dist\%varJar%.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\%varJar%.jar"
copy /Y "ECLDirect\dist\ECLDirect.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLDirect.jar"
copy /Y "ECLGUIFeatures\dist\ECLGUIFeatures.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLGUIFeatures.jar"

set varDir=ECLRollupJob
set varJar=%varDir%

copy /Y "Jobs\%varDir%\dist\%varJar%.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\%varJar%.jar"
copy /Y "ECLDirect\dist\ECLDirect.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLDirect.jar"
copy /Y "ECLGUIFeatures\dist\ECLGUIFeatures.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLGUIFeatures.jar"

set varDir=ECLSortJob
set varJar=%varDir%

copy /Y "Jobs\%varDir%\dist\%varJar%.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\%varJar%.jar"
copy /Y "ECLDirect\dist\ECLDirect.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLDirect.jar"
copy /Y "ECLGUIFeatures\dist\ECLGUIFeatures.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLGUIFeatures.jar"

set varDir=ECLSprayFileJob
set varJar=%varDir%

copy /Y "Jobs\%varDir%\dist\%varJar%.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\%varJar%.jar"
copy /Y "ECLDirect\dist\ECLDirect.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLDirect.jar"
copy /Y "ECLGUIFeatures\dist\ECLGUIFeatures.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLGUIFeatures.jar"

set varDir=ECLTableJob
set varJar=%varDir%

copy /Y "Jobs\%varDir%\dist\%varJar%.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\%varJar%.jar"
copy /Y "ECLDirect\dist\ECLDirect.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLDirect.jar"
copy /Y "ECLGUIFeatures\dist\ECLGUIFeatures.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLGUIFeatures.jar"


set varDir=ECLGenericJob
set varJar=%varDir%

copy /Y "Jobs\%varDir%\dist\%varJar%.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\%varJar%.jar"
copy /Y "ECLDirect\dist\ECLDirect.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLDirect.jar"
copy /Y "ECLGUIFeatures\dist\ECLGUIFeatures.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLGUIFeatures.jar"





set varDir=ECLML_KmeansJob
set varJar=%varDir%

copy /Y "Jobs\%varDir%\dist\%varJar%.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\%varJar%.jar"
copy /Y "ECLDirect\dist\ECLDirect.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLDirect.jar"
copy /Y "ECLGUIFeatures\dist\ECLGUIFeatures.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLGUIFeatures.jar"

set varDir=ECLML_AssociateJob
set varJar=%varDir%

copy /Y "Jobs\%varDir%\dist\%varJar%.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\%varJar%.jar"
copy /Y "ECLDirect\dist\ECLDirect.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLDirect.jar"
copy /Y "ECLGUIFeatures\dist\ECLGUIFeatures.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLGUIFeatures.jar"


set varDir=ECLML_ToFieldJob
set varJar=%varDir%

copy /Y "Jobs\%varDir%\dist\%varJar%.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\%varJar%.jar"
copy /Y "ECLDirect\dist\ECLDirect.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLDirect.jar"
copy /Y "ECLGUIFeatures\dist\ECLGUIFeatures.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLGUIFeatures.jar"

set varDir=ECLML_FromFieldJob
set varJar=%varDir%

copy /Y "Jobs\%varDir%\dist\%varJar%.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\%varJar%.jar"
copy /Y "ECLDirect\dist\ECLDirect.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLDirect.jar"
copy /Y "ECLGUIFeatures\dist\ECLGUIFeatures.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLGUIFeatures.jar"

set varDir=ECLML_ClassificationJob
set varJar=%varDir%

copy /Y "Jobs\%varDir%\dist\%varJar%.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\%varJar%.jar"
copy /Y "ECLDirect\dist\ECLDirect.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLDirect.jar"
copy /Y "ECLGUIFeatures\dist\ECLGUIFeatures.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLGUIFeatures.jar"

set varDir=ECLML_BuildNaiveBayesJob
set varJar=%varDir%

copy /Y "Jobs\%varDir%\dist\%varJar%.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\%varJar%.jar"
copy /Y "ECLDirect\dist\ECLDirect.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLDirect.jar"
copy /Y "ECLGUIFeatures\dist\ECLGUIFeatures.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLGUIFeatures.jar"


set varDir=ECLExecuteJob
set varJar=%varDir%

copy /Y "Jobs\%varDir%\dist\%varJar%.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\%varJar%.jar"
copy /Y "ECLDirect\dist\ECLDirect.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLDirect.jar"
copy /Y "ECLGUIFeatures\dist\ECLGUIFeatures.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLGUIFeatures.jar"

set varDir=ECLGroupJob
set varJar=%varDir%

copy /Y "Jobs\%varDir%\dist\%varJar%.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\%varJar%.jar"
copy /Y "ECLDirect\dist\ECLDirect.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLDirect.jar"
copy /Y "ECLGUIFeatures\dist\ECLGUIFeatures.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLGUIFeatures.jar"

set varDir=ECLIndexJob
set varJar=%varDir%

copy /Y "Jobs\%varDir%\dist\%varJar%.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\%varJar%.jar"
copy /Y "ECLDirect\dist\ECLDirect.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLDirect.jar"
copy /Y "ECLGUIFeatures\dist\ECLGUIFeatures.jar" "C:\Program Files\data-integration\plugins\jobentries\%varDir%\lib\ECLGUIFeatures.jar"



@echo on

