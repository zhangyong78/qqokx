Option Explicit

Dim shell
Dim fso
Dim scriptDir
Dim command

Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

scriptDir = fso.GetParentFolderName(WScript.ScriptFullName)
If fso.FileExists(scriptDir & "\.venv\Scripts\pythonw.exe") Then
    command = "cmd /c cd /d """ & scriptDir & """ && start """" """ & scriptDir & "\.venv\Scripts\pythonw.exe"" run_roll_terminal_qt.pyw"
Else
    command = "cmd /c cd /d """ & scriptDir & """ && start """" pythonw run_roll_terminal_qt.pyw"
End If

shell.Run command, 0, False
