Setup Server
==================================

```bash
sudo apt-get update
sudo apt-get install mailutils
sudo apt-get install heirloom-mailx
sudo apt-get install gcc

sudo apt-get -y install git
git clone https://github.com/Kyeo1983/pypeline.git
echo "export PYPELINE=\$HOME/pypeline" >> ~/.bash_profile

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
conda install -y jinja2
conda install -y requests
conda install -y sqlalchemy

echo "Installing (optional) packages"
pip install -y fake_useragent
pip install -y http_request_randomizer
conda install -y tqdm
conda install -y xlrd

echo "To support multi-developers access, need to grant to groups."
echo "Granting luigi logs write access"
chmod -R g+rws $PYPELINE/var/logs/luigi
echo "Granting .git repo write access"
chmod -R g+rws $PYPELINE/.git
echo "Granting jobs folder write access"
chmod -R g+rws $PYPELINE/jobs
```


Getting Started
==================================
```bash
cd init
source activate pipeline
python setup.py
```

```bash
cp ./luigi_central_scheduler/luigi_log.cfg.tmpl ./luigi_central_scheduler/luigi_log.cfg
cp ./luigi_central_scheduler/luigi.cfg.tmpl ./luigi_central_scheduler/luigi.cfg
sed -i  "s@{PYPELINE}@"${PYPELINE}"@g" ./luigi_central_scheduler/luigi_log.cfg
sed -i  "s@{PYPELINE}@"${PYPELINE}"@g" ./luigi_central_scheduler/luigi.cfg
```


For New Users
==================================
First update your _~/.bash_profile_
```bash
export LUIGI_CONFIG_PATH={WHERE_LUIGI_FILES_ARE}/pypeline/luigi_central_scheduler/luigi.cfg
export PATH=\$PATH:\{WHERE_MINICONDA_IS}/miniconda3/bin
```

Create your working pipeline files in _{WHERE_LUIGI_FILES_ARE}/jobs_.
Working files are always a pair of .sh and .py scripts.
Copy _sample_stage.sh_ and modify path variables. _.sh_ files typically operate the same way.
Then copy _sample_stage.py_ file and modify the path variables in the top portion of code (before _start_ task).
Then update your own code logics within and leave code within and above _start_ task and below _end_ task untouched.
