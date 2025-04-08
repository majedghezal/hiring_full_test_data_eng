# Hiring_full_test_data_eng 

Ce projet a été réalisé dans le cadre d’un test technique pour un poste de Data Engineer chez FULL.

 # Contenu du projet :
`fullETL.py` : Script pythoon principal qui implémente le pipeline Extract (du fichier csv) → Transform   → Load (dans la base de données retail.db).

`test.py` : Fichier de tests unitaires pour valider le bon fonctionnement du pipeline.

`retail_15_01_2022.csv` : Le fichier de transactions à charger (input).

`retail.db` : Base de données SQLite contenant les transactions historiques.

`fullqueries.sql` : Requêtes SQL répondant aux questions analytiques demandées.

 `deployment.pdf` : Une proposition de l'architecture et son explication pour automatiser et déployer le pipeline en utilisant les différents services AWS & managés par Terraform.

# Execution du projet : 

`python fullETL.py` (installer pandas puisque c'est une librairie externe)

# Pour lancer les tests :

`python -m unittest test.py`

`Je n'ai pas ajouté de fichier Docker ou Makefile, car l'exécution du projet est simple et directe via python fullETL.py et python -m unittest test.py.`

# ETL Pipeline :

`Extract` : Lecture du fichier CSV contenant les transactions.

`Transform` :

Vérification des colonnes et des valeurs obligatoires.

Correction automatique de la TVA si elle est erronée (TVA = 20%).

Extraction de la date depuis le nom du fichier CSV.

`Load` : Insertion dans la base SQLite tout en évitant les doublons.
