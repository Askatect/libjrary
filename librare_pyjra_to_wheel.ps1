# $pyjra = Get-ChildItem -Path 'wheels' | Sort-Object LastWriteTime -Descending | Select-Object -First 1
# pip install $pyjra.FullName
pip list --format=freeze | Where-Object {$_ -notlike "pyjra*"} > requirements.txt
python setup.py sdist bdist_wheel --dist-dir=.\wheels
Remove-Item 'dist' -Force -Recurse
Remove-Item 'build' -Force -Recurse
Remove-Item 'pyjra.egg-info' -Force -Recurse
# Move-Item 'dist' -Destination 'wheels' -Force -ErrorAction Continue
# Move-Item 'build' -Destination 'wheels' -Force -ErrorAction Continue
# Move-Item 'pyjra.egg-info' -Destination 'wheels' -Force -ErrorAction Continue