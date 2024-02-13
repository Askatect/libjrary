# $location = Get-Location
# $location = ($location -split "\\")[0..2] -join "\"
# $location = (Get-ChildItem *Reference* -Path $location -Recurse -Directory)[0].FullName
Write-Output ("Run Start: $((Get-Date).ToString())")
$location = $PSScriptRoot + '\Python'
Set-Location $location
Write-Output ("Copying from: $location.")
$location = ($location -split "\\")[0..2] -join "\"
$location += "\AppData\Local\Programs\Python\Python311\Lib\site-packages"
Write-Output ("Copying to: $location.")
$item = Read-Host "Enter the item to be copied"
Remove-Item -Path ($location + "\" + $item) -Recurse -ErrorAction SilentlyContinue
Copy-Item -Path $item -Destination $location -Recurse -Force
Write-Output ("Run End: $((Get-Date).ToString())")