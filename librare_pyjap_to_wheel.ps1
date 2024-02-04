# $pyjap = Get-ChildItem -Path 'wheels' | Sort-Object LastWriteTime -Descending | Select-Object -First 1
# pip install $pyjap.FullName
pip list --format=freeze | Where-Object {$_ -notlike "pyjap*"} > python\requirements.txt
python setup.py sdist bdist_wheel --dist-dir=.\python\wheels
Remove-Item 'dist' -Force -Recurse
Remove-Item 'build' -Force -Recurse
Remove-Item 'pyjap.egg-info' -Force -Recurse
# Move-Item 'dist' -Destination 'wheels' -Force -ErrorAction Continue
# Move-Item 'build' -Destination 'wheels' -Force -ErrorAction Continue
# Move-Item 'pyjap.egg-info' -Destination 'wheels' -Force -ErrorAction Continue