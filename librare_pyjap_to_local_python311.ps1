# $location = Get-Location
# $location = ($location -split "\\")[0..2] -join "\"
# $location = (Get-ChildItem *Reference* -Path $location -Recurse -Directory)[0].FullName
$location = $PSScriptRoot
Set-Location $location
Write-Output ("Copying from: $location.")
$location = ($location -split "\\")[0..2] -join "\"
$location += "\AppData\Local\Programs\Python\Python311\Lib\site-packages"
Write-Output ("Copying to: $location.")
copy-item -Path "pyjap" -Destination $location -Recurse -Force