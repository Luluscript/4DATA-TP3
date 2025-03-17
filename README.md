# 4DATA-TP3

Setup working environment :

# Création d'un environnement virtuel
python3 -m venv venv
# Activation (Windows)
venv\Scripts\activate
# Activation (Mac/Linux)
source venv/bin/activate

Install libs (first time):

pip install numpy pandas matplotlib seaborn jupyter notebook sqlalchemy pysqlite3

Freeze all packages:

pip freeze > requirements.txt

Recover from scratch:

pip install -r requirements.txt
