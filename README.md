📊 Hiring Full Test – Data Engineer
Ce projet a été réalisé dans le cadre d’un test technique pour un poste de Data Engineer chez FULL.
Il consiste à implémenter un pipeline ETL pour charger des données de transactions dans une base SQLite, avec vérifications et corrections automatiques.

🗂 Contenu du projet
Fichier	Description
fullETL.py	Script principal du pipeline ETL : Extract → Transform → Load
test.py	Fichier de tests unitaires pour valider le fonctionnement du pipeline
retail_15_01_2022.csv	Données de transactions à importer (input)
retail.db	Base SQLite contenant les transactions
fullqueries.sql	Requêtes SQL répondant aux questions analytiques demandées
deployment.pdf	Proposition d’architecture pour le déploiement automatique sur AWS via Terraform
🚀 Exécution du pipeline
bash
Copier
Modifier
python fullETL.py
📌 Assurez-vous d'avoir pandas installé (pip install pandas)

✅ Lancer les tests
bash
Copier
Modifier
python -m unittest test.py
💡 À propos de Docker / Makefile
Je n’ai pas ajouté de fichier Docker ou Makefile car l'exécution est simple et directe avec les commandes ci-dessus.
Une solution complète de déploiement est proposée dans deployment.pdf.

⚙️ Détails du pipeline ETL
🔹 Extract
Lecture du fichier CSV contenant les transactions

🔹 Transform
Vérification des colonnes et des valeurs obligatoires

Nettoyage : suppression des doublons et gestion des quantités négatives

Correction automatique de la TVA à 20% si incorrecte

Extraction de la date depuis le nom du fichier

🔹 Load
Insertion dans la base SQLite tout en évitant les doublons

