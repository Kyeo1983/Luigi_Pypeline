echo "Uncomment this to recreate docs: sphinx-apidoc -f -o /home/kyeo/pypeline/docs /home/kyeo/pypeline"
rm -R ~/pypeline/docs/build/doctrees
rm -R ~/pypeline/docs/build/html
PYTHONPATH="../" make html
rm -R ~/public_html/_static
rm -R ~/public_html/_sources
rm ~/public_html/*
mv ~/pypeline/docs/build/html/* ~/public_html
