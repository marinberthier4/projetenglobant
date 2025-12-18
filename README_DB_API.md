But rapide — base MySQL (XAMPP) + API

1) Démarrer XAMPP
   - Ouvrez le XAMPP Control Panel et démarrez `MySQL` (et `Apache` si nécessaire).

2) Créer la base et l'utilisateur (via phpMyAdmin ou SQL)
   - Exemple SQL (exécutez dans phpMyAdmin -> SQL ou via mysql client):

   CREATE DATABASE maladies_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
   CREATE USER 'agreg'@'localhost' IDENTIFIED BY 'votre_mot_de_passe';
   GRANT ALL PRIVILEGES ON maladies_db.* TO 'agreg'@'localhost';
   FLUSH PRIVILEGES;

   - Remplacez 'votre_mot_de_passe' par un mot de passe sûr.

3) Configurer les variables d'environnement (Windows PowerShell)

```powershell
$env:DB_HOST = '127.0.0.1'
$env:DB_PORT = '3306'
$env:DB_USER = 'agreg'
$env:DB_PASS = 'votre_mot_de_passe'
$env:DB_NAME = 'maladies_db'
```

4) Installer dépendances dans le venv

```powershell
& ".\.venv\Scripts\python.exe" -m pip install -r requirements.txt
```

5) Importer le CSV en base

```powershell
& ".\.venv\Scripts\python.exe" load_db_mysql.py
```

6) Lancer l'API

```powershell
& ".\.venv\Scripts\python.exe" -m uvicorn api:app --reload --port 8000
```

7) Tester l'API
   - Observations : http://127.0.0.1:8000/observations?maladie=cancer&indicateur=prevalence&annee=2018
   - Statistiques : http://127.0.0.1:8000/stats?maladie=cancer&indicateur=prevalence

Remarques & aide
- Si vous utilisez phpMyAdmin, vous pouvez créer la DB et l'utilisateur via l'interface graphique.
- Si l'import échoue pour cause d'accès, vérifiez identifiants et que MySQL est lancé.
- Après migration vers Postgres, on pourra facilement réutiliser `load_db_mysql.py` en changeant la chaîne de connexion.
