Option Explicit

Dim shell
Dim fso
Dim scriptDir
Dim command

Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

scriptDir = fso.GetParentFolderName(WScript.ScriptFullName)
command = "cmd /c cd /d """ & scriptDir & """ && start """" pythonw run_roll_terminal_qt.pyw"

shell.Run command, 0, False
