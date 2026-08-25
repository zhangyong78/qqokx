Option Explicit

Dim shell
Dim fso
Dim scriptDir
Dim command

Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

scriptDir = fso.GetParentFolderName(WScript.ScriptFullName)
If fso.FileExists(scriptDir & "\start_roll_terminal_qt.bat") Then
    command = "cmd /c call """ & scriptDir & "\start_roll_terminal_qt.bat"""
Else
    command = "cmd /c cd /d """ & scriptDir & """ && pythonw run_roll_terminal_qt.pyw"
End If

shell.Run command, 0, False
