Setup Server
==================================

```bash
sudo apt-get update
sudo apt-get -y install git
git clone https://github.com/Kyeo1983/pypeline.git

echo "Installing bzip2, required for miniconda"
sudo apt-get -y install bzip2

echo "Installing Miniconda"
wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh
chmod 755 miniconda.sh
./miniconda.sh
echo "export LUIGI_CONFIG_PATH=\$HOME/pypeline/luigi_central_scheduler/luigi.cfg" >> ~/.bash_profile
echo "export PATH=\$PATH:\$HOME/miniconda3/bin" >> ~/.bash_profile


echo "Update Conda"
conda update -n base -c defaults conda


echo "Creating Conda environment"
conda create -n pipeline
source activate pipeline

echo "Installing packages"
pip install --upgrade pip
conda install -y numpy
conda install -y pandas
conda install -y luigi
conda install -y requests
```
