%~d0
cd %~dp0
REM Folder changed

> temp.vbs ECHO '
>> temp.vbs ECHO '
>> temp.vbs ECHO '
>> temp.vbs ECHO ' UnZip Eclipse SWT files to Maven Repository
>> temp.vbs ECHO '
>> temp.vbs ECHO ' NOTE: This script assumes that your Maven Repository
>> temp.vbs ECHO ' is located under %HOME% - if not please modify
>> temp.vbs ECHO ' to account for the difference.
>> temp.vbs ECHO. 
>> temp.vbs ECHO ' The location of the zip file.
>> temp.vbs ECHO Set WshShell = CreateObject("Wscript.Shell")
>> temp.vbs ECHO. 
>> temp.vbs ECHO Dim sCurPath
>> temp.vbs ECHO sCurPath = CreateObject("Scripting.FileSystemObject").GetAbsolutePathName(".")
>> temp.vbs ECHO. 
>> temp.vbs ECHO strZipFile = sCurPath ^& "\maven-eclipse-swt.zip" 
>> temp.vbs ECHO WScript.Echo (strZipFile)
>> temp.vbs ECHO. 
>> temp.vbs ECHO ' The folder the contents should be extracted to.
>> temp.vbs ECHO homeDir = WshShell.ExpandEnvironmentStrings("%HOME%")
>> temp.vbs ECHO outFolder = homeDir ^& "\.m2\repository\org\eclipse"
>> temp.vbs ECHO. 
>> temp.vbs ECHO WScript.Echo ( "Extracting file" ^& strFileZIP ^& " to " ^& outFolder)
>> temp.vbs ECHO. 
>> temp.vbs ECHO Set objShell = CreateObject( "Shell.Application" )
>> temp.vbs ECHO Set objSource = objShell.NameSpace(strZipFile).Items()
>> temp.vbs ECHO Set objTarget = objShell.NameSpace(outFolder)
>> temp.vbs ECHO intOptions = 256
>> temp.vbs ECHO. 
>> temp.vbs ECHO objTarget.CopyHere objSource, intOptions
>> temp.vbs ECHO. 
>> temp.vbs ECHO WScript.Echo ( "Extracted" )

cscript //B temp.vbs

del temp.vbs
pause;

