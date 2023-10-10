$path = ($PSScriptRoot + "/logs")
$offset = Read-Host "Enter maximum age of retained logs (in hours): "
$threshold = (Get-Date).AddHours(-$offset)
Get-ChildItem -Path $path -File -Recurse |
    Where-Object {!$_.PSIsContainer -and $_.LastWriteTime -lt $threshold} | 
    Remove-Item -Recurse
Get-ChildItem -Path $PSScriptRoot -Directory -Recurse | 
    Where-Object (Get-ChildItem -Path $_.FullName -File -Recurse).Count -eq 0 |
    Remove-Item -Recurse