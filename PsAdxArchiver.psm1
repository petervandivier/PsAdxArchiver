
Push-Location $PsScriptRoot

Get-ChildItem Classes -Filter *.ps1 | ForEach-Object {
    . $_.FullName
}

Get-ChildItem Functions -Filter *.ps1 | ForEach-Object {
    . $_.FullName
    Export-ModuleMember $_.BaseName
}

Pop-Location
